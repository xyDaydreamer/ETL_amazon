"""Run with the existing Compose image and a read-only tests bind mount."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psycopg


def main() -> None:
    original = os.environ["AIRFLOW_CONN_AMAZON_MOVIES_DB"]
    database_url = original.rsplit("/", 1)[0] + "/amazon_movies_product_collection_test"
    prefix = uuid.uuid4().hex[:6].upper()
    asins = ["TST" + prefix + str(i) for i in range(3)]
    assert all(len(a) == 10 for a in asins)
    counts = {"robots": 0, "product": 0}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/robots.txt":
                counts["robots"] += 1
                body = b"User-agent: *\nAllow: /\n"
            else:
                counts["product"] += 1
                asin = self.path.rsplit("/", 1)[-1]
                if asin == asins[2]:
                    body = b"<html>validatecaptcha</html>"
                elif asin == asins[1]:
                    body = (f'<script type="application/ld+json">{{"@type":"Product",'
                            f'"sku":"{asin}","name":"Second Film"}}</script>').encode()
                else:
                    body = (f'<input value="{asin}" name="ASIN">'
                            '<span class="name" id="productTitle">First Film</span>').encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html" if self.path != "/robots.txt" else "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO staging.product_harvest(asin,original_url,status) "
                    "VALUES (%s,%s,'not_attempted')",
                    [(a, f"http://127.0.0.1:{server.server_port}/dp/{a}") for a in asins],
                )

        def run(batch: str, items: list[str], snapshot_dir: Path) -> dict:
            code = (
                "import json,sys; from pathlib import Path; "
                "from amazon_movies.stage3_crawler import crawl; "
                "print(json.dumps(crawl(sys.argv[1],sys.argv[2].split(','),sys.argv[3],"
                "Path(sys.argv[4]),max_attempts=1,delay=1,concurrency=2,url_template=sys.argv[5])))"
            )
            output = subprocess.run(
                [sys.executable, "-c", code, database_url, ",".join(items), batch,
                 str(snapshot_dir), f"http://127.0.0.1:{server.server_port}/dp/{{asin}}"],
                capture_output=True, text=True, check=True,
            )
            if batch.startswith("mock-first-"):
                assert "'httpcache/store': 4" in output.stderr, output.stderr[-3000:]
            else:
                assert "'httpcache/hit': 2" in output.stderr, output.stderr[-3000:]
            return json.loads(output.stdout.strip().splitlines()[-1])

        with tempfile.TemporaryDirectory(prefix="product-page-mock-") as temp:
            root = Path(temp)
            batch1 = "mock-first-" + prefix
            batch2 = "mock-cache-" + prefix
            first = run(batch1, asins, root)
            first_count = counts["product"]
            second = run(batch2, [asins[0]], root)
            assert first_count == 3 and counts["product"] == 3, counts
            assert first["actual_http_requests"] == 3 and first["outcomes"] == {
                "access_restricted": 1, "success": 2,
            }, first
            assert second["actual_http_requests"] == 0 and second["request_attempts"] == 1, second
            with psycopg.connect(database_url) as conn:
                rows = conn.execute(
                    "SELECT outcome,cache_hit,source_record_id FROM staging.product_harvest_attempt "
                    "WHERE asin=ANY(%s) ORDER BY asin,attempt_number", (asins,),
                ).fetchall()
                assert len(rows) == 4 and sum(bool(r[1]) for r in rows) == 1, rows
                assert all(r[2] is not None for r in rows), rows
            print(json.dumps({"first": first, "second": second, "http_product_requests": counts["product"],
                              "http_robots_requests": counts["robots"]}, ensure_ascii=False))
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
