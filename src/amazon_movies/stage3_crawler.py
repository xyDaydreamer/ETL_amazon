from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import uuid
from urllib.parse import urlparse
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

import psycopg
import scrapy
from scrapy import Selector
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import IgnoreRequest

from amazon_movies.stage3_pipeline import USER_AGENT, _finish, _json, _run


COLLECTOR_VERSION = "amazon-product-page-collector-v2"
SAMPLE_PLAN_VERSION = "amazon-product-page-sample-plan-v1"
SAMPLE_SELECTION_METHOD = "seeded-md5-unattempted-v1"
MAX_CANDIDATE_SAMPLE = 200
RESTRICTED_MARKERS = (
    b"validatecaptcha", b"enter the characters you see below",
    b"sorry, we just need to make sure you're not a robot", b"robot check",
)
NOT_FOUND_MARKERS = (
    b"looking for something?", b"sorry! we couldn't find that page", b"page not found",
)
PRODUCT_MARKERS = (
    rb'id=["\']productTitle["\']', rb'id=["\']dp["\']',
    rb'id=["\']detailBullets', rb'id=["\']productDetails',
)
ASIN_PATTERNS = (
    rb'<link[^>]+rel=["\']canonical["\'][^>]+href=["\'][^"\']*/(?:dp|gp/product)/([A-Z0-9]{10})',
    rb'<input[^>]+(?:name|id)=["\']ASIN["\'][^>]+value=["\']([A-Z0-9]{10})',
    rb'data-asin=["\']([A-Z0-9]{10})["\']',
    rb'["\']asin["\']\s*:\s*["\']([A-Z0-9]{10})["\']',
)


@dataclass(frozen=True)
class ResponseAssessment:
    outcome: str
    error_code: str | None
    target_asin: str
    observed_asins: tuple[str, ...]
    product_markers: tuple[str, ...]
    restricted_marker: str | None
    final_url_asin: str | None
    retryable: bool
    product_title: str | None = None


def _url_asin(url: str) -> str | None:
    match = re.search(r"/(?:dp|gp/product)/([A-Z0-9]{10})(?:[/?]|$)", url, re.I)
    return match.group(1).upper() if match else None


def assess_product_response(*, target_asin: str, status: int, final_url: str, body: bytes) -> ResponseAssessment:
    """Require product structure and matching ASIN; HTTP 200/title alone never succeeds."""
    target = target_asin.upper()
    lowered = body.lower()
    restricted = next((m.decode("ascii", "replace") for m in RESTRICTED_MARKERS if m in lowered), None)
    observed = {m.group(1).decode("ascii").upper() for p in ASIN_PATTERNS for m in re.finditer(p, body, re.I)}
    markers = [p.decode("ascii") for p in PRODUCT_MARKERS if re.search(p, body, re.I)]
    selector = Selector(text=body.decode("utf-8", "replace"))
    for value in selector.css('input[name="ASIN"]::attr(value), input[id="ASIN"]::attr(value), [data-asin]::attr(data-asin)').getall():
        if re.fullmatch(r"[A-Z0-9]{10}", value.strip(), re.I):
            observed.add(value.strip().upper())
    for value in selector.css('link[rel="canonical"]::attr(href)').getall():
        asin = _url_asin(value)
        if asin:
            observed.add(asin)
    jsonld_products = []
    for raw_json in selector.css('script[type="application/ld+json"]::text').getall():
        try:
            document = json.loads(raw_json)
        except (ValueError, TypeError):
            continue
        nodes = document if isinstance(document, list) else [document]
        for node in nodes:
            if isinstance(node, dict):
                nodes_to_check = node.get("@graph", [node])
                jsonld_products.extend(item for item in nodes_to_check if isinstance(item, dict)
                                       and "Product" in str(item.get("@type", "")))
    for product in jsonld_products:
        for key in ("sku", "productID", "asin"):
            value = str(product.get(key, "")).strip()
            if re.fullmatch(r"[A-Z0-9]{10}", value, re.I):
                observed.add(value.upper())
    if jsonld_products:
        markers.append("jsonld:Product")
    title = selector.css('#productTitle::text, #title span::text, meta[property="og:title"]::attr(content)').get()
    if not title:
        title = next((str(p.get("name")) for p in jsonld_products if p.get("name")), None)
    if title:
        title = " ".join(html.unescape(title).split())
    final_asin = _url_asin(final_url)
    if status == 404 or any(marker in lowered for marker in NOT_FOUND_MARKERS):
        outcome, code, retryable = "not_found", "http_or_soft_404", False
    elif status in (401, 403, 429) or restricted:
        outcome, code, retryable = "access_restricted", (
            "verification_page" if restricted else f"http_{status}"
        ), status == 429 and not restricted
    elif status == 408 or status >= 500:
        outcome, code, retryable = "network_failed", f"http_{status}", True
    elif status != 200:
        outcome, code, retryable = "parse_failed", f"unexpected_http_{status}", False
    elif final_asin and final_asin != target:
        outcome, code, retryable = "parse_failed", "redirected_asin_mismatch", False
    elif target not in observed:
        outcome, code, retryable = "parse_failed", "target_asin_not_proven", False
    elif not markers:
        outcome, code, retryable = "parse_failed", "product_template_not_proven", False
    elif not title:
        outcome, code, retryable = "parse_failed", "product_title_missing", False
    else:
        outcome, code, retryable = "success", None, False
    return ResponseAssessment(
        outcome, code, target, tuple(sorted(observed)), tuple(markers), restricted, final_asin, retryable, title
    )


def validate_candidate_limit(limit: int) -> int:
    """Validate a candidate cap; it is deliberately independent of request controls."""
    if not 1 <= int(limit) <= MAX_CANDIDATE_SAMPLE:
        raise ValueError(f"candidate limit must be between 1 and {MAX_CANDIDATE_SAMPLE}")
    return int(limit)


def prepare_sample_plan(database_url: str, *, batch_id: str, candidate_limit: int,
                        random_seed: int, access_basis: str) -> dict[str, Any]:
    """Persist a deterministic, network-free sample plan from the complete product list.

    Planning never performs HTTP I/O.  Existing HTTP attempts are excluded by default so
    expanding a sample does not keep selecting the same response/ASIN.
    """
    limit = validate_candidate_limit(candidate_limit)
    if not access_basis.strip():
        raise ValueError("access_basis is required")
    with psycopg.connect(database_url) as conn:
        existing = conn.execute(
            """SELECT selection_method,random_seed,candidate_limit,access_basis,status
               FROM meta.product_harvest_batch WHERE batch_id=%s FOR UPDATE""", (batch_id,),
        ).fetchone()
        if existing:
            expected = (SAMPLE_SELECTION_METHOD, random_seed, limit, access_basis)
            if tuple(existing[:4]) != expected:
                raise ValueError(f"batch {batch_id} already exists with different selection metadata")
            rows = conn.execute(
                """SELECT asin,selection_rank FROM staging.product_harvest_work_item
                   WHERE batch_id=%s ORDER BY selection_rank NULLS LAST,asin""", (batch_id,),
            ).fetchall()
            return {
                "batch_id": batch_id, "status": existing[4], "candidate_limit": limit,
                "selected_count": len(rows), "selection_method": SAMPLE_SELECTION_METHOD,
                "random_seed": random_seed, "exclude_attempted": True,
                "access_basis": access_basis, "network_requests": 0,
                "idempotent_replay": True, "asins": [str(row[0]) for row in rows],
            }

        run_id = _run(conn, "product_page_sample_plan", batch_id, SAMPLE_PLAN_VERSION)
        product_count = int(conn.execute("SELECT count(*) FROM staging.product_harvest").fetchone()[0])
        rows = conn.execute(
            """SELECT h.asin FROM staging.product_harvest h
                 WHERE NOT EXISTS (SELECT 1 FROM staging.product_harvest_attempt a WHERE a.asin=h.asin)
                   AND NOT EXISTS (
                       SELECT 1 FROM staging.product_harvest_work_item wi
                       JOIN meta.product_harvest_batch b ON b.batch_id=wi.batch_id
                       WHERE wi.asin=h.asin AND b.status IN ('prepared','running')
                   )
                 ORDER BY md5(h.asin || ':' || %s::text),h.asin LIMIT %s""",
            (random_seed, limit),
        ).fetchall()
        selected = [str(row[0]) for row in rows]
        if not selected:
            raise RuntimeError("no eligible product ASINs remain for the requested sample")
        conn.execute(
            """INSERT INTO meta.product_harvest_batch(
                   batch_id,run_id,status,max_products,max_attempts_per_product,
                   request_delay_seconds,concurrency,prepared_count,selection_method,
                   random_seed,candidate_limit,access_basis)
               VALUES (%s,%s,'prepared',%s,2,5,1,%s,%s,%s,%s,%s)""",
            (batch_id, run_id, len(selected), len(selected), SAMPLE_SELECTION_METHOD,
             random_seed, limit, access_basis),
        )
        with conn.cursor() as cursor:
            cursor.executemany(
                """INSERT INTO staging.product_harvest_work_item(batch_id,asin,selection_rank)
                   VALUES (%s,%s,%s)""",
                [(batch_id, asin, rank) for rank, asin in enumerate(selected, start=1)],
            )
        _finish(conn, run_id, {
            "product_list_count": product_count, "selected_count": len(selected),
            "candidate_limit": limit, "selection_method": SAMPLE_SELECTION_METHOD,
            "random_seed": random_seed, "exclude_attempted": True,
            "network_requests": 0, "access_basis": access_basis,
        })
    return {
        "run_id": run_id, "batch_id": batch_id, "status": "prepared",
        "product_list_count": product_count, "candidate_limit": limit,
        "selected_count": len(selected), "selection_method": SAMPLE_SELECTION_METHOD,
        "random_seed": random_seed, "exclude_attempted": True,
        "access_basis": access_basis, "network_requests": 0,
        "idempotent_replay": False, "asins": selected,
    }


def _prepare_and_claim(database_url: str, *, asins: Iterable[str], batch_id: str, run_id: int,
                       max_attempts: int, delay: float, concurrency: int) -> tuple[list[str], bool]:
    requested = sorted({a.upper() for a in asins})
    if not requested or any(not re.fullmatch(r"[A-Z0-9]{10}", a) for a in requested):
        raise ValueError("all ASINs must be valid 10-character identifiers")
    validate_candidate_limit(len(requested))
    with psycopg.connect(database_url) as conn:
        existing = conn.execute(
            "SELECT status FROM meta.product_harvest_batch WHERE batch_id=%s FOR UPDATE", (batch_id,)
        ).fetchone()
        if existing and existing[0] == "succeeded":
            return [], True
        if existing and existing[0] == "running":
            active = int(conn.execute(
                """SELECT count(*) FROM staging.product_harvest_work_item
                   WHERE batch_id=%s AND state='claimed'
                     AND claimed_at > clock_timestamp() - interval '1 hour'""", (batch_id,),
            ).fetchone()[0])
            if active:
                raise RuntimeError(f"batch {batch_id} already has {active} active claims")
        conn.execute(
            """INSERT INTO meta.product_harvest_batch(
                   batch_id,run_id,status,max_products,max_attempts_per_product,
                   request_delay_seconds,concurrency,prepared_count)
               VALUES (%s,%s,'prepared',%s,%s,%s,%s,%s)
               ON CONFLICT (batch_id) DO UPDATE SET run_id=EXCLUDED.run_id,status='prepared',
                   max_products=EXCLUDED.max_products,
                   max_attempts_per_product=EXCLUDED.max_attempts_per_product,
                   request_delay_seconds=EXCLUDED.request_delay_seconds,
                   concurrency=EXCLUDED.concurrency,updated_at=clock_timestamp()""",
            (batch_id, run_id, len(requested), max_attempts, delay, concurrency, len(requested)),
        )
        with conn.cursor() as cursor:
            cursor.executemany(
                """INSERT INTO staging.product_harvest_work_item(batch_id,asin)
                   VALUES (%s,%s) ON CONFLICT (batch_id,asin) DO NOTHING""",
                [(batch_id, asin) for asin in requested],
            )
        conn.execute(
            """UPDATE staging.product_harvest_work_item SET state='pending',claim_token=NULL,
                   claimed_at=NULL,updated_at=clock_timestamp()
               WHERE batch_id=%s AND (state IN ('failed','deferred') OR
                   (state='claimed' AND claimed_at <= clock_timestamp() - interval '1 hour'))""", (batch_id,),
        )
        token = uuid.uuid4()
        claimed = conn.execute(
            """WITH picked AS (
                   SELECT asin FROM staging.product_harvest_work_item
                   WHERE batch_id=%s AND state='pending' ORDER BY asin FOR UPDATE SKIP LOCKED)
               UPDATE staging.product_harvest_work_item w
               SET state='claimed',claim_token=%s,claimed_at=clock_timestamp(),updated_at=clock_timestamp()
               FROM picked WHERE w.batch_id=%s AND w.asin=picked.asin RETURNING w.asin""",
            (batch_id, token, batch_id),
        ).fetchall()
        conn.execute(
            """UPDATE meta.product_harvest_batch SET status='running',claimed_count=%s,
                   updated_at=clock_timestamp() WHERE batch_id=%s""", (len(claimed), batch_id),
        )
    return [str(row[0]) for row in claimed], False


class BoundedAmazonProductSpider(scrapy.Spider):
    name = "bounded_amazon_product"
    allowed_domains = ["amazon.com", "www.amazon.com"]

    def __init__(self, *, database_url: str, batch_id: str, run_id: int, asins: str,
                 snapshot_dir: str, max_attempts: int = 2,
                 url_template: str = "https://www.amazon.com/dp/{asin}", **kwargs: Any):
        super().__init__(**kwargs)
        self.database_url = database_url
        self.batch_id = batch_id
        self.run_id = int(run_id)
        self.asins = [v for v in asins.split(",") if v]
        self.snapshot_dir = Path(snapshot_dir)
        self.max_attempts = int(max_attempts)
        self.url_template = url_template
        self.allowed_domains = [urlparse(url_template).hostname or "amazon.com"]

    async def start(self):
        for asin in self.asins:
            yield self._request(asin, 1)

    def _request(self, asin: str, ordinal: int, retry_reason: str | None = None) -> scrapy.Request:
        return scrapy.Request(
            self.url_template.format(asin=asin), callback=self.parse_product,
            errback=self.request_failed, cb_kwargs={"asin": asin, "ordinal": ordinal},
            meta={"handle_httpstatus_all": True, "retry_reason": retry_reason}, dont_filter=True,
        )

    @staticmethod
    def _attempt_number(conn: psycopg.Connection[Any], asin: str) -> int:
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (asin,))
        return int(conn.execute(
            "SELECT coalesce(max(attempt_number),0)+1 FROM staging.product_harvest_attempt WHERE asin=%s", (asin,)
        ).fetchone()[0])

    def _save_response(self, response: scrapy.http.Response, assessment: ResponseAssessment) -> int:
        body = bytes(response.body)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(body).hexdigest()
        suffix = ".html" if b"<html" in body[:2048].lower() else ".bin"
        snapshot = self.snapshot_dir / f"amazon-{assessment.target_asin}-{digest[:16]}{suffix}"
        if not snapshot.exists():
            snapshot.write_bytes(body)
        redirect_urls = list(response.request.meta.get("redirect_urls", []))
        redirect_status = list(response.request.meta.get("redirect_reasons", []))
        with psycopg.connect(self.database_url) as conn:
            row = conn.execute(
                """INSERT INTO raw.source_record(
                       source_kind,source_uri,field_locator,content_sha256,snapshot_path,fetched_at,
                       http_status,access_status,raw_payload)
                   VALUES ('http_response',%s,%s::jsonb,%s,%s,clock_timestamp(),%s,%s,%s::jsonb)
                   ON CONFLICT DO NOTHING RETURNING source_record_id""",
                (response.url, _json({"request_url": response.request.url, "body_bytes": len(body),
                 "redirect_urls": redirect_urls, "redirect_statuses": redirect_status}), digest,
                 str(snapshot), response.status, assessment.outcome,
                 _json({"headers": {k.decode("latin1"): [v.decode("latin1") for v in values]
                       for k, values in response.headers.items()}, "body_sha256": digest,
                       "assessment": asdict(assessment)})),
            ).fetchone()
            if row:
                return int(row[0])
            existing = conn.execute(
                """SELECT source_record_id FROM raw.source_record WHERE source_uri=%s
                   AND content_sha256=%s AND record_number IS NULL ORDER BY source_record_id LIMIT 1""",
                (response.url, digest),
            ).fetchone()
            if not existing:
                raise RuntimeError("response evidence insert was not recoverable")
            return int(existing[0])

    def _record(self, *, asin: str, ordinal: int, requested_url: str, final_url: str | None,
                outcome: str, http_status: int | None, source_record_id: int | None,
                error_code: str | None, retry_reason: str | None, cache_hit: bool,
                redirect_chain: list[dict[str, Any]], evidence: dict[str, Any], final: bool) -> None:
        with psycopg.connect(self.database_url) as conn:
            attempt = self._attempt_number(conn, asin)
            conn.execute(
                """INSERT INTO staging.product_harvest_attempt(
                       asin,attempt_number,requested_url,final_url,started_at,finished_at,outcome,
                       http_status,source_record_id,error_code,run_id,request_ordinal,retry_reason,
                       cache_hit,redirect_chain,validation_evidence)
                   VALUES (%s,%s,%s,%s,clock_timestamp(),clock_timestamp(),%s,%s,%s,%s,%s,%s,%s,
                           %s,%s::jsonb,%s::jsonb)""",
                (asin, attempt, requested_url, final_url, outcome, http_status, source_record_id,
                 error_code, self.run_id, ordinal, retry_reason, cache_hit,
                 _json(redirect_chain), _json(evidence)),
            )
            conn.execute(
                """UPDATE staging.product_harvest_work_item SET request_attempts=request_attempts+1,
                       state=CASE WHEN %s THEN 'completed' ELSE state END,
                       completed_at=CASE WHEN %s THEN clock_timestamp() ELSE completed_at END,
                       final_outcome=CASE WHEN %s THEN %s ELSE final_outcome END,
                       last_error_code=%s,updated_at=clock_timestamp()
                   WHERE batch_id=%s AND asin=%s""",
                (final, final, final, outcome, error_code, self.batch_id, asin),
            )
            conn.execute(
                """UPDATE staging.product_harvest SET
                       status=CASE WHEN %s='success' THEN 'success'
                           WHEN valid_source_record_id IS NOT NULL THEN 'success' ELSE %s END,
                       final_url=CASE WHEN %s='success' OR valid_source_record_id IS NULL THEN %s ELSE final_url END,
                       http_status=CASE WHEN %s='success' OR valid_source_record_id IS NULL THEN %s ELSE http_status END,
                       attempt_count=attempt_count+1,
                       fetched_at=CASE WHEN %s IS NOT NULL THEN clock_timestamp() ELSE fetched_at END,
                       source_record_id=CASE WHEN %s='success' THEN %s
                           WHEN valid_source_record_id IS NULL THEN %s ELSE source_record_id END,
                       valid_source_record_id=CASE WHEN %s='success' THEN %s ELSE valid_source_record_id END,
                       latest_source_record_id=%s,last_attempt_status=%s,last_attempt_at=clock_timestamp(),
                       last_error_code=%s,updated_at=clock_timestamp()
                   WHERE asin=%s""",
                (outcome, outcome, outcome, final_url, outcome, http_status, source_record_id,
                 outcome, source_record_id, source_record_id, outcome, source_record_id,
                 source_record_id, outcome, error_code, asin),
            )

    def parse_product(self, response: scrapy.http.Response, asin: str, ordinal: int):
        assessment = assess_product_response(
            target_asin=asin, status=response.status, final_url=response.url, body=bytes(response.body)
        )
        source_id = self._save_response(response, assessment)
        retry = assessment.retryable and ordinal < self.max_attempts
        urls = list(response.request.meta.get("redirect_urls", []))
        reasons = list(response.request.meta.get("redirect_reasons", []))
        chain = [{"url": u, "status": reasons[i] if i < len(reasons) else None} for i, u in enumerate(urls)]
        self._record(
            asin=asin, ordinal=ordinal, requested_url=response.request.url, final_url=response.url,
            outcome=assessment.outcome, http_status=response.status, source_record_id=source_id,
            error_code=assessment.error_code, retry_reason=response.request.meta.get("retry_reason"),
            cache_hit="cached" in response.flags, redirect_chain=chain,
            evidence=asdict(assessment), final=not retry,
        )
        if retry:
            yield self._request(asin, ordinal + 1, assessment.error_code)

    def request_failed(self, failure: Any):
        asin = failure.request.cb_kwargs["asin"]
        ordinal = int(failure.request.cb_kwargs["ordinal"])
        if failure.check(IgnoreRequest):
            outcome, code, retryable = "robots_denied", "robots_txt_denied", False
        elif "timeout" in failure.value.__class__.__name__.lower():
            outcome, code, retryable = "timeout", failure.value.__class__.__name__, True
        else:
            outcome, code, retryable = "network_failed", failure.value.__class__.__name__, True
        retry = retryable and ordinal < self.max_attempts
        self._record(
            asin=asin, ordinal=ordinal, requested_url=failure.request.url, final_url=None,
            outcome=outcome, http_status=None, source_record_id=None, error_code=code,
            retry_reason=failure.request.meta.get("retry_reason"), cache_hit=False,
            redirect_chain=[], evidence={"failure_type": code}, final=not retry,
        )
        if retry:
            yield self._request(asin, ordinal + 1, code)


def crawl(database_url: str, asins: list[str], batch_id: str, snapshot_dir: Path, *,
          max_attempts: int = 2, delay: float = 5.0, concurrency: int = 1,
          url_template: str = "https://www.amazon.com/dp/{asin}") -> dict[str, Any]:
    if not 1 <= max_attempts <= 10 or delay < 1 or not 1 <= concurrency <= 4:
        raise ValueError("unsafe collector limits")
    host = urlparse(url_template).hostname
    if host not in {"www.amazon.com", "127.0.0.1", "localhost"} or "{asin}" not in url_template:
        raise ValueError("collector URL must be Amazon or loopback and contain {asin}")
    with psycopg.connect(database_url) as conn:
        run_id = _run(conn, "stage3_amazon_page_validation", batch_id, COLLECTOR_VERSION)
    claimed, replay = _prepare_and_claim(
        database_url, asins=asins, batch_id=batch_id, run_id=run_id,
        max_attempts=max_attempts, delay=delay, concurrency=concurrency,
    )
    if replay:
        replay_status = collection_status(database_url, batch_id) | {"idempotent_replay": True}
        with psycopg.connect(database_url) as conn:
            _finish(conn, run_id, {"idempotent_replay": True, "batch_id": batch_id})
        return replay_status
    process = CrawlerProcess({
        "ROBOTSTXT_OBEY": True, "DOWNLOAD_DELAY": delay, "RANDOMIZE_DOWNLOAD_DELAY": False,
        "CONCURRENT_REQUESTS": concurrency, "CONCURRENT_REQUESTS_PER_DOMAIN": concurrency,
        "RETRY_ENABLED": False, "HTTPCACHE_ENABLED": True,
        "HTTPCACHE_DIR": str(snapshot_dir / "httpcache-v2"), "HTTPCACHE_EXPIRATION_SECS": 86400,
        "USER_AGENT": USER_AGENT, "LOG_LEVEL": "INFO", "DOWNLOAD_TIMEOUT": 30,
        "COOKIES_ENABLED": False,
    })
    process.crawl(
        BoundedAmazonProductSpider, database_url=database_url, batch_id=batch_id, run_id=run_id,
        asins=",".join(claimed), snapshot_dir=str(snapshot_dir), max_attempts=max_attempts,
        url_template=url_template,
    )
    process.start()
    with psycopg.connect(database_url) as conn:
        incomplete = int(conn.execute(
            "SELECT count(*) FROM staging.product_harvest_work_item WHERE batch_id=%s AND state<>'completed'",
            (batch_id,),
        ).fetchone()[0])
        outcomes = dict(conn.execute(
            """SELECT final_outcome,count(*) FROM staging.product_harvest_work_item
               WHERE batch_id=%s GROUP BY final_outcome ORDER BY final_outcome""", (batch_id,),
        ).fetchall())
        request_rows = int(conn.execute(
            "SELECT count(*) FROM staging.product_harvest_attempt WHERE run_id=%s", (run_id,)
        ).fetchone()[0])
        actual_http = int(conn.execute(
            "SELECT count(*) FROM staging.product_harvest_attempt WHERE run_id=%s AND NOT cache_hit AND source_record_id IS NOT NULL", (run_id,)
        ).fetchone()[0])
        if incomplete:
            conn.execute("UPDATE meta.product_harvest_batch SET status='failed' WHERE batch_id=%s", (batch_id,))
            conn.execute(
                """UPDATE meta.etl_run SET status='failed',finished_at=clock_timestamp(),stats=%s::jsonb
                   WHERE run_id=%s""", (_json({"incomplete": incomplete}), run_id),
            )
            raise RuntimeError(f"collector left {incomplete} work items incomplete")
        conn.execute(
            """UPDATE meta.product_harvest_batch SET status='succeeded',completed_count=%s,
               updated_at=clock_timestamp() WHERE batch_id=%s""", (len(claimed), batch_id),
        )
        _finish(conn, run_id, {"logical_tasks": len(claimed), "request_attempts": request_rows,
                              "actual_http_requests": actual_http, "outcomes": outcomes})
    return {"run_id": run_id, "batch_id": batch_id, "logical_tasks": len(claimed),
            "request_attempts": request_rows, "actual_http_requests": actual_http,
            "outcomes": outcomes, "idempotent_replay": False}


def collection_status(database_url: str, batch_id: str | None = None) -> dict[str, Any]:
    with psycopg.connect(database_url) as conn:
        distribution = dict(conn.execute(
            "SELECT status,count(*) FROM staging.product_harvest GROUP BY status ORDER BY status"
        ).fetchall())
        totals = conn.execute(
            """SELECT count(*),count(DISTINCT asin),
                      count(*) FILTER (WHERE outcome='success'),
                      count(*) FILTER (WHERE cache_hit IS TRUE),
                      count(*) FILTER (WHERE cache_hit IS FALSE AND source_record_id IS NOT NULL AND requested_url ~ '^https?://'),
                      count(*) FILTER (WHERE cache_hit IS FALSE AND source_record_id IS NOT NULL AND requested_url ~ '^https?://'
                                               AND outcome='success'),
                      count(*) FILTER (WHERE cache_hit IS FALSE AND source_record_id IS NOT NULL AND requested_url ~ '^https?://'
                                               AND outcome IN ('access_restricted','robots_denied')),
                      count(*) FILTER (WHERE requested_url !~ '^https?://')
               FROM staging.product_harvest_attempt"""
        ).fetchone()
        batch = None
        if batch_id:
            row = conn.execute(
                """SELECT status,prepared_count,claimed_count,completed_count,max_attempts_per_product,
                          request_delay_seconds,concurrency,selection_method,random_seed,
                          candidate_limit,access_basis FROM meta.product_harvest_batch WHERE batch_id=%s""",
                (batch_id,),
            ).fetchone()
            batch = ({
                "status": row[0], "prepared_count": row[1], "claimed_count": row[2],
                "completed_count": row[3], "max_attempts_per_product": row[4],
                "request_delay_seconds": str(row[5]), "concurrency": row[6],
                "selection_method": row[7], "random_seed": row[8],
                "candidate_limit": row[9], "access_basis": row[10],
            } if row else None)
    real_requests = int(totals[4])
    real_success = int(totals[5])
    restricted = int(totals[6])
    return {"product_status": distribution, "attempt_rows": int(totals[0]),
            "attempted_asins": int(totals[1]), "valid_page_attempts": int(totals[2]),
            "cache_hits": int(totals[3]), "actual_target_http_requests": real_requests,
            "real_http_successes": real_success, "real_http_restricted": restricted,
            "local_simulation_attempts": int(totals[7]),
            "real_success_rate": (real_success / real_requests if real_requests else None),
            "real_restricted_rate": (restricted / real_requests if real_requests else None),
            "real_other_failure_rate": (
                (real_requests - real_success - restricted) / real_requests if real_requests else None
            ), "batch": batch}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--asins", help="comma-separated explicit ASINs (maximum 100)")
    selection.add_argument("--sample-limit", type=int, help="deterministic candidate cap (maximum 100)")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--snapshot-dir", default="data/snapshots/stage3")
    parser.add_argument("--random-seed", type=int, default=20261107)
    parser.add_argument("--access-basis", default="existing-captcha-no-written-authorization")
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--network-authorized", action="store_true",
                        help="explicit gate; never use to bypass access restrictions")
    parser.add_argument("--max-attempts", type=int, default=2)
    parser.add_argument("--delay", type=float, default=5.0)
    parser.add_argument("--concurrency", type=int, default=1)
    args = parser.parse_args()
    if args.sample_limit is not None:
        plan = prepare_sample_plan(
            args.database_url, batch_id=args.batch_id, candidate_limit=args.sample_limit,
            random_seed=args.random_seed, access_basis=args.access_basis,
        )
        if args.plan_only:
            result = {k: v for k, v in plan.items() if k != "asins"}
        else:
            if not args.network_authorized:
                raise SystemExit("network collection is blocked: use --plan-only, or provide documented authorization")
            result = crawl(
                args.database_url, plan["asins"], args.batch_id, Path(args.snapshot_dir),
                max_attempts=args.max_attempts, delay=args.delay, concurrency=args.concurrency,
            )
    else:
        asins = [value for value in args.asins.split(",") if value]
        validate_candidate_limit(len(set(asins)))
        if args.plan_only:
            raise SystemExit("--plan-only requires --sample-limit so selection metadata can be recorded")
        if not args.network_authorized:
            raise SystemExit("network collection is blocked without --network-authorized")
        result = crawl(
            args.database_url, asins, args.batch_id, Path(args.snapshot_dir),
            max_attempts=args.max_attempts, delay=args.delay, concurrency=args.concurrency,
        )
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
