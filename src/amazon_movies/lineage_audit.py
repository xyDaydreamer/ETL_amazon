from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, Iterable


AUDIT_VERSION = "final-field-lineage-v1"
REPAIR_VERSION = "final-field-lineage-repair-v1"
ADOPTED_STATUSES = ("verified", "manual", "predicted")


@dataclass(frozen=True)
class FieldResult:
    entity_type: str
    field_name: str
    applicable_count: int
    nonempty_count: int
    complete_lineage_count: int
    missing_value_count: int
    missing_lineage_count: int
    verified_count: int = 0
    manual_count: int = 0
    inferred_count: int = 0
    conflict_count: int = 0
    denominator_note: str = ""


def _url(value: str | None) -> str:
    if not value:
        raise ValueError("database URL is required")
    return value


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _active_versions(conn: Any) -> tuple[str, str]:
    row = conn.execute(
        """SELECT
             (SELECT parser_version FROM meta.parser_release
               WHERE is_current AND coverage_kind='full' AND verified_source_eof),
             (SELECT rule_version FROM meta.logical_review_release
               WHERE is_current AND status='full_verified')"""
    ).fetchone()
    if not row or not row[0] or not row[1]:
        raise RuntimeError("full parser and logical-review releases must both be active")
    return str(row[0]), str(row[1])


def _etl_run(conn: Any, batch_id: str, parser: str, dedupe: str) -> int:
    return int(conn.execute(
        """INSERT INTO meta.etl_run(
               pipeline_name,batch_id,input_version,rule_version,status,scope_kind,is_full_source
           ) VALUES ('final_field_lineage_repair',%s,%s,%s,'running','full_source',false)
           ON CONFLICT(pipeline_name,batch_id,input_version,rule_version) DO UPDATE
           SET status='running',finished_at=NULL
           RETURNING run_id""",
        (batch_id, f"{parser}|{dedupe}", REPAIR_VERSION),
    ).fetchone()[0])


def repair_existing_lineage(database_url: str, *, batch_id: str) -> dict[str, Any]:
    """Link only already-saved evidence to current values; never infer from a target value alone."""
    import psycopg

    with psycopg.connect(_url(database_url)) as conn:
        parser, dedupe = _active_versions(conn)
        run_id = _etl_run(conn, batch_id, parser, dedupe)

        # Preserve all UCSD classification observations, including rejected/conflicting candidates.
        classification_assertions = conn.execute(
            """WITH inserted AS (
                 INSERT INTO integration.field_assertion(
                   entity_type,entity_key,field_name,raw_value,normalized_value,value_semantics,
                   source_record_id,field_locator,extraction_rule_version,confidence,quality_status,run_id)
                 SELECT 'product',pmr.asin,'classification',pmr.classification_basis,
                        to_jsonb(pmr.proposed_classification),'ucsd_product_classification_rule',
                        pmr.source_record_id,
                        jsonb_build_object('record_number',pmr.source_record_number,
                          'inputs',jsonb_build_array('title','category','description')),
                        %s,NULL,'inferred',%s
                 FROM staging.product_metadata_record pmr
                 WHERE NOT EXISTS (
                   SELECT 1 FROM integration.field_assertion fa
                   WHERE fa.entity_type='product' AND fa.entity_key=pmr.asin
                     AND fa.field_name='classification' AND fa.source_record_id=pmr.source_record_id
                     AND fa.extraction_rule_version=%s
                 ) RETURNING assertion_id)
               SELECT count(*) FROM inserted""",
            (REPAIR_VERSION, run_id, REPAIR_VERSION),
        ).fetchone()[0]

        classification_selections = conn.execute(
            """WITH candidates AS (
                 SELECT DISTINCT ON (pe.asin) pe.asin,fa.assertion_id,fa.source_record_id
                 FROM target.product_edition pe
                 JOIN integration.field_assertion fa
                   ON fa.entity_type='product' AND fa.entity_key=pe.asin
                  AND fa.field_name='classification' AND fa.extraction_rule_version=%s
                  AND fa.normalized_value=to_jsonb(pe.classification)
                 WHERE pe.classification<>'pending'
                 ORDER BY pe.asin,fa.source_record_id,fa.assertion_id
               ), inserted AS (
                 INSERT INTO integration.field_selection(
                   entity_type,entity_key,field_name,selection_version,selected_assertion_id,
                   selected_assertion_ids,decision_status,decision_reason,run_id)
                 SELECT 'product',asin,'classification',%s,assertion_id,ARRAY[assertion_id],
                        'selected','existing target classification equals a saved UCSD rule output; linked without reverse inference',%s
                 FROM candidates
                 ON CONFLICT(entity_type,entity_key,field_name,selection_version) DO NOTHING
                 RETURNING field_selection_id)
               SELECT count(*) FROM inserted""",
            (REPAIR_VERSION, REPAIR_VERSION, run_id),
        ).fetchone()[0]

        # Release precision is a deterministic derivative of an already-selected release-year assertion.
        precision_assertions = conn.execute(
            """WITH adopted AS (
                 SELECT DISTINCT m.movie_id,m.stable_key,m.release_precision,cfs.selected_assertion_id,
                        fa.source_record_id
                 FROM target.movie m JOIN target.product_movie pm USING(movie_id)
                 JOIN integration.current_field_selection cfs
                   ON cfs.entity_type='movie' AND cfs.entity_key=m.stable_key
                  AND cfs.field_name='release_year' AND cfs.decision_status='selected'
                 JOIN integration.field_assertion fa ON fa.assertion_id=cfs.selected_assertion_id
                 WHERE pm.mapping_status IN ('verified','manual','predicted')
                   AND m.release_precision IS NOT NULL
               ), inserted AS (
                 INSERT INTO integration.field_assertion(
                   entity_type,entity_key,field_name,raw_value,normalized_value,value_semantics,
                   source_record_id,field_locator,extraction_rule_version,confidence,quality_status,run_id)
                 SELECT 'movie',stable_key,'release_precision',NULL,to_jsonb(release_precision),
                        'precision_derived_from_selected_release_year',source_record_id,
                        jsonb_build_object('operation','derive_precision_from_release_year',
                                           'input_assertion_id',selected_assertion_id),
                        %s,1.0,'inferred',%s
                 FROM adopted a WHERE NOT EXISTS (
                   SELECT 1 FROM integration.field_assertion fa
                   WHERE fa.entity_type='movie' AND fa.entity_key=a.stable_key
                     AND fa.field_name='release_precision' AND fa.extraction_rule_version=%s)
                 RETURNING assertion_id)
               SELECT count(*) FROM inserted""",
            (REPAIR_VERSION, run_id, REPAIR_VERSION),
        ).fetchone()[0]
        conn.execute(
            """INSERT INTO integration.assertion_dependency(
                 output_assertion_id,input_assertion_id,operation_type,rule_id,run_id)
               SELECT outa.assertion_id,cfs.selected_assertion_id,'derive_precision_from_release_year',%s,%s
               FROM integration.field_assertion outa
               JOIN integration.current_field_selection cfs
                 ON cfs.entity_type='movie' AND cfs.entity_key=outa.entity_key
                AND cfs.field_name='release_year'
               WHERE outa.field_name='release_precision' AND outa.extraction_rule_version=%s
               ON CONFLICT DO NOTHING""",
            (REPAIR_VERSION, run_id, REPAIR_VERSION),
        )
        precision_selections = conn.execute(
            """WITH inserted AS (
                 INSERT INTO integration.field_selection(
                   entity_type,entity_key,field_name,selection_version,selected_assertion_id,
                   selected_assertion_ids,decision_status,decision_reason,run_id)
                 SELECT 'movie',m.stable_key,'release_precision',%s,fa.assertion_id,
                        ARRAY[fa.assertion_id],'selected',
                        'precision derived from the selected release-year assertion; dependency retained',%s
                 FROM target.movie m JOIN target.product_movie pm USING(movie_id)
                 JOIN integration.field_assertion fa
                   ON fa.entity_type='movie' AND fa.entity_key=m.stable_key
                  AND fa.field_name='release_precision' AND fa.extraction_rule_version=%s
                 WHERE pm.mapping_status IN ('verified','manual','predicted')
                 ON CONFLICT(entity_type,entity_key,field_name,selection_version) DO NOTHING
                 RETURNING field_selection_id)
               SELECT count(*) FROM inserted""",
            (REPAIR_VERSION, run_id, REPAIR_VERSION),
        ).fetchone()[0]

        # Two legacy suggestion-name relationships have explicit merge decisions. Keep rows, mark superseded.
        superseded = conn.execute(
            """WITH matches AS (
                 SELECT oldmp.ctid AS old_ctid,oldp.person_id AS old_person_id,
                        newp.person_id AS new_person_id,md.rule_version,md.evidence,
                        oldmp.movie_id,oldmp.role_type,oldmp.source_record_id
                 FROM target.movie_person oldmp JOIN target.person oldp USING(person_id)
                 JOIN integration.merge_decision md
                   ON md.entity_type='person' AND md.left_entity_key=oldp.stable_key
                  AND md.decision='merge'
                 JOIN target.person newp ON newp.stable_key=md.right_entity_key
                 WHERE oldp.status='retired' AND oldmp.relation_status='selected'
                   AND EXISTS (SELECT 1 FROM target.movie_person newmp
                     WHERE newmp.movie_id=oldmp.movie_id AND newmp.person_id=newp.person_id)
               ), updated AS (
                 UPDATE target.movie_person mp SET relation_status='superseded',
                   superseded_by_person_id=m.new_person_id,selection_rule_version=m.rule_version,
                   relation_selection_reason='explicit saved person merge decision; historical suggestion credit retained'
                 FROM matches m WHERE mp.ctid=m.old_ctid
                 RETURNING mp.movie_id,mp.person_id,mp.role_type,mp.source_record_id,
                           mp.superseded_by_person_id,mp.selection_rule_version)
               SELECT count(*) FROM updated"""
        ).fetchone()[0]

        # Relational links for ProductMovie are recovered only from source IDs already inside saved decisions.
        relationship_links = conn.execute(
            """WITH relation AS (
                 SELECT pm.product_id,pm.movie_id,pm.relationship_type,md.rule_version,md.evidence,
                        pe.asin,m.stable_key,
                        concat_ws(':',pm.product_id,pm.movie_id,pm.relationship_type) relationship_key
                 FROM target.product_movie pm JOIN target.product_edition pe USING(product_id)
                 JOIN target.movie m USING(movie_id)
                 JOIN integration.merge_decision md ON md.merge_decision_id=pm.decision_id
                 WHERE pm.mapping_status IN ('verified','manual','predicted')
               ), ids AS (
                 SELECT r.*,v.evidence_role,v.source_id
                 FROM relation r CROSS JOIN LATERAL (VALUES
                   ('title_source',(r.evidence->>'title_source_record_id')::bigint),
                   ('product_source',(r.evidence->>'product_source_record_id')::bigint),
                   ('work_source',(r.evidence#>>'{work,source_record_id}')::bigint),
                   ('member_source',(r.evidence#>>'{member,source_record_id}')::bigint)
                 ) v(evidence_role,source_id) WHERE v.source_id IS NOT NULL
                 UNION ALL
                 SELECT r.*,'product_source',(p.item->>'source_record_id')::bigint
                 FROM relation r CROSS JOIN LATERAL jsonb_array_elements(
                   CASE WHEN jsonb_typeof(r.evidence->'products')='array' THEN r.evidence->'products' ELSE '[]'::jsonb END
                 ) p(item) WHERE p.item ? 'source_record_id'
                 UNION ALL
                 SELECT r.*,'fact_source',(f.item->>'source_record_id')::bigint
                 FROM relation r CROSS JOIN LATERAL jsonb_array_elements(
                   CASE WHEN jsonb_typeof(r.evidence->'facts')='array' THEN r.evidence->'facts' ELSE '[]'::jsonb END
                 ) f(item) WHERE f.item ? 'source_record_id'
               ), inserted AS (
                 INSERT INTO integration.relationship_source_link(
                   relationship_type,relationship_key,source_record_id,evidence_role,field_names,
                   rule_version,selection_reason)
                 SELECT DISTINCT 'product_movie',relationship_key,ids.source_id,evidence_role,
                        ARRAY['product_id','movie_id','relationship_type','mapping_status'],rule_version,
                        'source ID already saved in the adopted merge decision evidence'
                 FROM ids JOIN raw.source_record sr ON sr.source_record_id=ids.source_id
                 ON CONFLICT DO NOTHING RETURNING relationship_key)
               SELECT count(*) FROM inserted"""
        ).fetchone()[0]

        for entity_type, field_name, action, count in (
            ("product_edition", "classification", "link_saved_ucsd_classification", classification_selections),
            ("movie", "release_precision", "add_derived_selection_dependency", precision_selections),
            ("movie_person", "relationship", "supersede_retired_suggestion_relation", superseded),
            ("product_movie", "relationship", "materialize_saved_source_ids", relationship_links),
        ):
            conn.execute(
                """INSERT INTO integration.lineage_repair_event(
                     repair_version,entity_type,entity_key,field_name,repair_action,
                     prior_state,new_state,reason,run_id)
                   VALUES (%s,%s,'__batch__',%s,%s,%s::jsonb,%s::jsonb,%s,%s)
                   ON CONFLICT(repair_version,entity_type,entity_key,field_name,repair_action) DO NOTHING""",
                (REPAIR_VERSION, entity_type, field_name, action, _json({}), _json({"affected": int(count)}),
                 "linked only previously saved evidence and decisions; no value inferred from target alone", run_id),
            )

        stats = {
            "classification_assertions_inserted": int(classification_assertions),
            "classification_selections_inserted": int(classification_selections),
            "release_precision_assertions_inserted": int(precision_assertions),
            "release_precision_selections_inserted": int(precision_selections),
            "movie_person_relations_superseded": int(superseded),
            "product_movie_source_links_inserted": int(relationship_links),
        }
        conn.execute(
            "UPDATE meta.etl_run SET status='succeeded',finished_at=clock_timestamp(),stats=%s::jsonb WHERE run_id=%s",
            (_json(stats), run_id),
        )
        return {"run_id": run_id, "repair_version": REPAIR_VERSION, **stats}


def _put_result(conn: Any, audit_run_id: int, result: FieldResult) -> None:
    values = asdict(result)
    conn.execute(
        """INSERT INTO integration.lineage_audit_result(
             audit_run_id,entity_type,field_name,applicable_count,nonempty_count,
             complete_lineage_count,missing_value_count,missing_lineage_count,
             verified_count,manual_count,inferred_count,conflict_count,denominator_note)
           VALUES (%(audit_run_id)s,%(entity_type)s,%(field_name)s,%(applicable_count)s,
             %(nonempty_count)s,%(complete_lineage_count)s,%(missing_value_count)s,
             %(missing_lineage_count)s,%(verified_count)s,%(manual_count)s,%(inferred_count)s,
             %(conflict_count)s,%(denominator_note)s)
           ON CONFLICT(audit_run_id,entity_type,field_name) DO UPDATE SET
             applicable_count=EXCLUDED.applicable_count,nonempty_count=EXCLUDED.nonempty_count,
             complete_lineage_count=EXCLUDED.complete_lineage_count,
             missing_value_count=EXCLUDED.missing_value_count,
             missing_lineage_count=EXCLUDED.missing_lineage_count,
             verified_count=EXCLUDED.verified_count,manual_count=EXCLUDED.manual_count,
             inferred_count=EXCLUDED.inferred_count,conflict_count=EXCLUDED.conflict_count,
             denominator_note=EXCLUDED.denominator_note""",
        {"audit_run_id": audit_run_id, **values},
    )


def _field_results(rows: Iterable[Any], entity_type: str, denominator: str) -> list[FieldResult]:
    return [FieldResult(
        entity_type=entity_type, field_name=str(row[0]), applicable_count=int(row[1]),
        nonempty_count=int(row[2]), complete_lineage_count=int(row[3]),
        missing_value_count=int(row[1] - row[2]), missing_lineage_count=int(row[2] - row[3]),
        verified_count=int(row[4] or 0), manual_count=int(row[5] or 0),
        inferred_count=int(row[6] or 0), conflict_count=int(row[7] or 0),
        denominator_note=denominator,
    ) for row in rows]


def _start_audit(conn: Any, batch_id: str, scope_key: str, full: bool) -> tuple[int, str, str]:
    parser, dedupe = _active_versions(conn)
    row = conn.execute(
        """INSERT INTO meta.lineage_audit_run(
             audit_version,batch_id,scope_key,status,active_parser_version,
             active_dedupe_rule_version,full_scope)
           VALUES (%s,%s,%s,'running',%s,%s,%s)
           ON CONFLICT(audit_version,batch_id) DO UPDATE SET
             status='running',finished_at=NULL,stats='{}'::jsonb
           RETURNING audit_run_id""",
        (AUDIT_VERSION, batch_id, scope_key, parser, dedupe, full),
    ).fetchone()
    return int(row[0]), parser, dedupe


def _mark_component(conn: Any, audit_run_id: int, component: str, status: str, examined: int, gaps: int) -> None:
    conn.execute(
        """INSERT INTO meta.lineage_audit_progress(
             audit_run_id,component,status,examined_count,gap_count)
           VALUES (%s,%s,%s,%s,%s)
           ON CONFLICT(audit_run_id,component) DO UPDATE SET status=EXCLUDED.status,
             examined_count=EXCLUDED.examined_count,gap_count=EXCLUDED.gap_count,
             updated_at=clock_timestamp()""",
        (audit_run_id, component, status, examined, gaps),
    )


def run_lineage_audit(
    database_url: str, *, batch_id: str, scope_key: str, full: bool = False,
    limit: int = 10_000,
) -> dict[str, Any]:
    import psycopg

    if not full and limit < 1:
        raise ValueError("bounded audit limit must be positive")
    cap = None if full else limit
    with psycopg.connect(_url(database_url)) as conn:
        # Coverage is read-only apart from audit metadata. Autocommit makes each
        # component's progress visible while the long full-scope scans run.
        conn.autocommit = True
        audit_run_id, parser, dedupe = _start_audit(conn, batch_id, scope_key, full)
        conn.execute("DELETE FROM integration.lineage_audit_result WHERE audit_run_id=%s", (audit_run_id,))
        conn.execute("DELETE FROM integration.lineage_audit_gap WHERE audit_run_id=%s", (audit_run_id,))

        # Product scope. ASIN uses the parser observation chain; other values use current selections.
        _mark_component(conn, audit_run_id, "product_edition", "running", 0, 0)
        rows = conn.execute(
            """WITH products AS (SELECT * FROM target.product_edition ORDER BY product_id LIMIT %s),
             expanded AS (
               SELECT p.product_id,p.asin,v.field_name,v.final_value,
                 CASE WHEN v.field_name='asin' THEN a.complete
                      WHEN v.field_name='classification' AND p.classification='pending' THEN false
                      ELSE cfs.selected_assertion_id IS NOT NULL AND fa.source_record_id IS NOT NULL
                           AND sr.content_sha256 IS NOT NULL AND sr.field_locator<>'{}'::jsonb END complete,
                 fa.quality_status,
                 CASE WHEN v.field_name='classification' AND p.classification='pending' THEN false
                      ELSE v.final_value IS NOT NULL END nonempty
               FROM products p CROSS JOIN LATERAL (VALUES
                 ('asin',to_jsonb(p.asin)),('classification',to_jsonb(p.classification)),
                 ('media_format',to_jsonb(p.media_format)),('language_codes',to_jsonb(p.language_codes)),
                 ('subtitle_codes',to_jsonb(p.subtitle_codes)),('region_code',to_jsonb(p.region_code)),
                 ('cut_label',to_jsonb(p.cut_label)),('publisher',to_jsonb(p.publisher)),
                 ('product_release_date',to_jsonb(p.product_release_date))) v(field_name,final_value)
               LEFT JOIN LATERAL (
                 SELECT true AS complete FROM staging.product_asin_observation o
                 JOIN staging.review_record rr USING(source_review_id)
                 JOIN raw.source_record sr USING(source_record_id)
                 JOIN staging.review_parse_locator l USING(source_review_id,parser_version)
                 WHERE o.asin=p.asin AND o.parser_version=%s AND o.asin_status='usable'
                   AND sr.content_sha256 IS NOT NULL AND l.content_sha256=rr.content_sha256
                 LIMIT 1
               ) a ON true
               LEFT JOIN integration.current_field_selection cfs
                 ON cfs.entity_type='product' AND cfs.entity_key=p.asin AND cfs.field_name=v.field_name
               LEFT JOIN integration.field_assertion fa ON fa.assertion_id=cfs.selected_assertion_id
               LEFT JOIN raw.source_record sr USING(source_record_id)
             ) SELECT field_name,count(*),count(*) FILTER(WHERE nonempty),
                 count(*) FILTER(WHERE nonempty AND complete),
                 count(*) FILTER(WHERE nonempty AND complete AND coalesce(quality_status,'observed')='observed'),
                 count(*) FILTER(WHERE nonempty AND complete AND quality_status='manual'),
                 count(*) FILTER(WHERE nonempty AND complete AND quality_status='inferred'),
                 count(*) FILTER(WHERE quality_status='conflicted')
               FROM expanded GROUP BY field_name ORDER BY field_name""",
            (cap, parser),
        ).fetchall()
        product_results = _field_results(rows, "product_edition", "all ProductEdition rows in the audit scope; pending classification is missing")
        for item in product_results:
            _put_result(conn, audit_run_id, item)
        _mark_component(conn, audit_run_id, "product_edition", "completed", max((r.applicable_count for r in product_results), default=0), sum(r.missing_lineage_count for r in product_results))

        # Only movies reached by adopted mappings are final; candidate inventory is deliberately excluded.
        _mark_component(conn, audit_run_id, "movie", "running", 0, 0)
        rows = conn.execute(
            """WITH movies AS (
               SELECT DISTINCT m.* FROM target.movie m JOIN target.product_movie pm USING(movie_id)
               WHERE pm.mapping_status IN ('verified','manual','predicted') ORDER BY movie_id LIMIT %s
             ), expanded AS (
               SELECT m.movie_id,m.stable_key,v.field_name,v.final_value,fa.quality_status,
                 cfs.selected_assertion_id IS NOT NULL AND fa.normalized_value IS NOT DISTINCT FROM v.final_value
                 AND sr.content_sha256 IS NOT NULL AND sr.field_locator<>'{}'::jsonb AS complete
               FROM movies m CROSS JOIN LATERAL (VALUES
                 ('canonical_title',to_jsonb(m.canonical_title)),('original_title',to_jsonb(m.original_title)),
                 ('release_date',to_jsonb(m.release_date)),('release_year',to_jsonb(m.release_year)),
                 ('release_precision',to_jsonb(m.release_precision)),('release_region',to_jsonb(m.release_region)),
                 ('release_type',to_jsonb(m.release_type))) v(field_name,final_value)
               LEFT JOIN integration.current_field_selection cfs ON cfs.entity_type='movie'
                 AND cfs.entity_key=m.stable_key AND cfs.field_name=v.field_name
               LEFT JOIN integration.field_assertion fa ON fa.assertion_id=cfs.selected_assertion_id
               LEFT JOIN raw.source_record sr USING(source_record_id)
             ) SELECT field_name,count(*),count(final_value),
                 count(*) FILTER(WHERE final_value IS NOT NULL AND complete),
                 count(*) FILTER(WHERE complete AND quality_status='observed'),
                 count(*) FILTER(WHERE complete AND quality_status='manual'),
                 count(*) FILTER(WHERE complete AND quality_status='inferred'),
                 count(*) FILTER(WHERE quality_status='conflicted')
               FROM expanded GROUP BY field_name ORDER BY field_name""",
            (cap,),
        ).fetchall()
        movie_results = _field_results(rows, "movie", "distinct movies reached by adopted ProductMovie rows; candidate-only inventory excluded")
        for item in movie_results:
            _put_result(conn, audit_run_id, item)
        _mark_component(conn, audit_run_id, "movie", "completed", max((r.applicable_count for r in movie_results), default=0), sum(r.missing_lineage_count for r in movie_results))

        _mark_component(conn, audit_run_id, "person", "running", 0, 0)
        person_row = conn.execute(
            """WITH people AS (
               SELECT DISTINCT p.* FROM target.person p JOIN target.current_movie_person mp USING(person_id)
               JOIN target.product_movie pm USING(movie_id)
               WHERE pm.mapping_status IN ('verified','manual','predicted') ORDER BY person_id LIMIT %s
             ) SELECT count(*) applicable,count(canonical_name) nonempty,
               count(*) FILTER(WHERE cfs.selected_assertion_id IS NOT NULL
                 AND fa.normalized_value=to_jsonb(p.canonical_name) AND sr.content_sha256 IS NOT NULL
                 AND sr.field_locator<>'{}'::jsonb) complete,
               count(*) FILTER(WHERE fa.quality_status='observed'),
               count(*) FILTER(WHERE fa.quality_status='manual'),
               count(*) FILTER(WHERE fa.quality_status='inferred'),
               count(*) FILTER(WHERE fa.quality_status='conflicted')
             FROM people p LEFT JOIN integration.current_field_selection cfs
               ON cfs.entity_type='person' AND cfs.entity_key=p.stable_key AND cfs.field_name='canonical_name'
             LEFT JOIN integration.field_assertion fa ON fa.assertion_id=cfs.selected_assertion_id
             LEFT JOIN raw.source_record sr USING(source_record_id)""",
            (cap,),
        ).fetchone()
        person_result = FieldResult("person", "canonical_name", int(person_row[0]), int(person_row[1]), int(person_row[2]),
                                    int(person_row[0]-person_row[1]), int(person_row[1]-person_row[2]),
                                    int(person_row[3]), int(person_row[4]), int(person_row[5]), int(person_row[6]),
                                    "distinct non-retired persons in current credits of adopted movies")
        _put_result(conn, audit_run_id, person_result)
        _mark_component(conn, audit_run_id, "person", "completed", person_result.applicable_count, person_result.missing_lineage_count)

        _mark_component(conn, audit_run_id, "person_alias", "running", 0, 0)
        alias_rows = conn.execute(
            """WITH people AS (
               SELECT DISTINCT p.person_id FROM target.person p JOIN target.current_movie_person mp USING(person_id)
               WHERE EXISTS (SELECT 1 FROM target.product_movie pm WHERE pm.movie_id=mp.movie_id
                 AND pm.mapping_status IN ('verified','manual','predicted'))
               ORDER BY person_id LIMIT %s
             ), aliases AS (
               SELECT pa.*,sr.content_sha256,sr.field_locator
               FROM target.person_alias pa JOIN people USING(person_id)
               LEFT JOIN raw.source_record sr USING(source_record_id)
             )
             SELECT 'alias_raw',count(*),count(alias_raw),
               count(*) FILTER(WHERE alias_raw IS NOT NULL AND content_sha256 IS NOT NULL
                 AND field_locator<>'{}'::jsonb),
               count(*) FILTER(WHERE alias_raw IS NOT NULL AND content_sha256 IS NOT NULL),0,0,0
             FROM aliases
             UNION ALL
             SELECT 'alias_normalized',count(*),count(alias_normalized),
               count(*) FILTER(WHERE alias_normalized IS NOT NULL AND alias_raw IS NOT NULL
                 AND content_sha256 IS NOT NULL),0,0,
               count(*) FILTER(WHERE alias_normalized IS NOT NULL AND alias_raw IS NOT NULL
                 AND content_sha256 IS NOT NULL),0
             FROM aliases""",
            (cap,),
        ).fetchall()
        alias_results = _field_results(
            alias_rows, "person_alias",
            "aliases belonging to current persons in adopted-movie credits; normalization is technical, not identity proof",
        )
        for item in alias_results:
            _put_result(conn, audit_run_id, item)
        _mark_component(conn, audit_run_id, "person_alias", "completed",
                        max((r.applicable_count for r in alias_results), default=0),
                        sum(r.missing_lineage_count for r in alias_results))

        _mark_component(conn, audit_run_id, "relationships", "running", 0, 0)
        relation_rows = conn.execute(
            """WITH pm_scope AS (
               SELECT pm.*,concat_ws(':',pm.product_id,pm.movie_id,pm.relationship_type) relationship_key
               FROM target.product_movie pm WHERE pm.mapping_status IN ('verified','manual','predicted')
               ORDER BY product_id,movie_id LIMIT %s
             ), mp_scope AS (
               SELECT mp.* FROM target.current_movie_person mp
               WHERE EXISTS (SELECT 1 FROM target.product_movie pm WHERE pm.movie_id=mp.movie_id
                 AND pm.mapping_status IN ('verified','manual','predicted'))
               ORDER BY movie_id,person_id LIMIT %s
             )
             SELECT 'product_movie',count(*),count(*),
               count(*) FILTER(WHERE md.merge_decision_id IS NOT NULL AND l.source_record_id IS NOT NULL
                 AND sr.content_sha256 IS NOT NULL AND sr.field_locator<>'{}'::jsonb),
               count(*) FILTER(WHERE md.decision_origin='rule'),count(*) FILTER(WHERE md.decision_origin='human_label'),0,
               count(*) FILTER(WHERE md.decision<>'merge')
             FROM pm_scope p LEFT JOIN integration.merge_decision md ON md.merge_decision_id=p.decision_id
             LEFT JOIN LATERAL (SELECT min(source_record_id) source_record_id
               FROM integration.relationship_source_link x WHERE x.relationship_type='product_movie'
                 AND x.relationship_key=p.relationship_key) l ON true
             LEFT JOIN raw.source_record sr USING(source_record_id)
             UNION ALL
             SELECT 'movie_person',count(*),count(*),
               count(*) FILTER(WHERE e.relationship_source_record_id IS NOT NULL AND rs.content_sha256 IS NOT NULL
                 AND rs.field_locator<>'{}'::jsonb AND e.name_source_record_id IS NOT NULL
                 AND ns.content_sha256 IS NOT NULL),
               count(*),0,0,count(*) FILTER(WHERE e.selection_status='conflicted')
             FROM mp_scope mp LEFT JOIN LATERAL (
               SELECT e.* FROM integration.movie_person_source_evidence e
               WHERE e.movie_id=mp.movie_id AND e.person_id=mp.person_id
                 AND e.relationship_source_record_id=mp.source_record_id AND e.selection_status='selected'
               ORDER BY e.created_at,e.snapshot_id LIMIT 1
             ) e ON true
             LEFT JOIN raw.source_record rs ON rs.source_record_id=mp.source_record_id
             LEFT JOIN raw.source_record ns ON ns.source_record_id=e.name_source_record_id""",
            (cap, cap),
        ).fetchall()
        relation_results = _field_results(relation_rows, "relationship", "current adopted relationship rows; each member, role and source are checked")
        relation_results = [FieldResult(r.field_name, "relationship", r.applicable_count, r.nonempty_count,
                                        r.complete_lineage_count, r.missing_value_count,r.missing_lineage_count,
                                        r.verified_count,r.manual_count,r.inferred_count,r.conflict_count,r.denominator_note)
                            for r in relation_results]
        for item in relation_results:
            _put_result(conn, audit_run_id, item)
        _mark_component(conn, audit_run_id, "relationships", "completed", sum(r.applicable_count for r in relation_results), sum(r.missing_lineage_count for r in relation_results))

        # One scan of logical reviews calculates all final-field coverage counters.
        _mark_component(conn, audit_run_id, "logical_review", "running", 0, 0)
        logical = conn.execute(
            """WITH scope AS (
               SELECT lr.* FROM target.current_logical_review lr ORDER BY logical_review_id LIMIT %s
             ), chain AS (
               SELECT s.*,r.asin source_asin,r.reviewer_id source_reviewer,r.review_time_utc source_time,
                 r.score source_score,r.summary_raw source_summary,r.review_text_raw source_text,
                 r.helpful_votes source_helpful,r.total_votes source_total,
                 rr.source_record_id,rr.content_sha256 identity_hash,sr.content_sha256 source_hash,
                 l.content_sha256 locator_hash,pe.product_id source_product_id,
                 (sr.source_record_id IS NOT NULL AND sr.content_sha256=rr.content_sha256
                  AND l.content_sha256=rr.content_sha256 AND l.parser_version=s.parser_version
                 ) base_complete
               FROM scope s LEFT JOIN staging.review_record rr
                 ON rr.source_review_id=s.representative_source_review_id
               LEFT JOIN raw.source_record sr USING(source_record_id)
               LEFT JOIN staging.review_parse_locator l
                 ON l.source_review_id=s.representative_source_review_id AND l.parser_version=s.parser_version
               LEFT JOIN staging.review_parse_result r
                 ON r.source_review_id=s.representative_source_review_id AND r.parser_version=s.parser_version
               LEFT JOIN target.product_edition pe ON pe.asin=r.asin
             ) SELECT count(*) applicable,
               count(product_id),count(*) FILTER(WHERE product_id IS NOT NULL AND base_complete AND product_id=source_product_id),
               count(reviewer_id),count(*) FILTER(WHERE reviewer_id IS NOT NULL AND base_complete AND reviewer_id IS NOT DISTINCT FROM source_reviewer),
               count(review_time_utc),count(*) FILTER(WHERE review_time_utc IS NOT NULL AND base_complete AND review_time_utc IS NOT DISTINCT FROM source_time),
               count(score),count(*) FILTER(WHERE score IS NOT NULL AND base_complete AND score IS NOT DISTINCT FROM source_score),
               count(summary),count(*) FILTER(WHERE summary IS NOT NULL AND base_complete AND summary IS NOT DISTINCT FROM source_summary),
               count(review_text),count(*) FILTER(WHERE review_text IS NOT NULL AND base_complete AND review_text IS NOT DISTINCT FROM source_text),
               count(*) FILTER(WHERE helpfulness_resolution IS NOT NULL),
               count(*) FILTER(WHERE helpfulness_resolution IS NOT NULL AND base_complete AND (
                 (helpfulness_resolution='unanimous_source_value' AND helpful_votes IS NOT DISTINCT FROM source_helpful
                  AND total_votes IS NOT DISTINCT FROM source_total)
                 OR helpfulness_resolution='conflict_preserved_at_source')),
               count(*) FILTER(WHERE helpfulness_resolution='conflict_preserved_at_source') conflicts
             FROM chain""",
            (cap,),
        ).fetchone()
        applicable = int(logical[0])
        logical_fields = (
            ("product_id", logical[1], logical[2]),
            ("reviewer_id", logical[3], logical[4]),
            ("review_time_utc", logical[5], logical[6]),
            ("score", logical[7], logical[8]),
            ("summary", logical[9], logical[10]),
            ("review_text", logical[11], logical[12]),
            ("helpfulness", logical[13], logical[14]),
        )
        logical_results: list[FieldResult] = []
        for name, nonempty, complete in logical_fields:
            item = FieldResult("logical_review", name, applicable, int(nonempty), int(complete),
                               applicable-int(nonempty), int(nonempty)-int(complete), int(complete),
                               conflict_count=int(logical[15] if name == "helpfulness" else 0),
                               denominator_note="active full-verified logical-review release; representative parse/source chain reused")
            logical_results.append(item)
            _put_result(conn, audit_run_id, item)
        for name in ("logical_review_id", "stable_key"):
            item = FieldResult(
                "logical_review", name, applicable, applicable, applicable, 0, 0,
                inferred_count=applicable,
                denominator_note="active logical-review rows; generated by the documented identity/hash rule, not an external fact",
            )
            logical_results.append(item)
            _put_result(conn, audit_run_id, item)
        _mark_component(conn, audit_run_id, "logical_review", "completed", applicable, sum(r.missing_lineage_count for r in logical_results))

        _mark_component(conn, audit_run_id, "review_mapping", "running", 0, 0)
        mapping = conn.execute(
            """WITH scope AS (SELECT * FROM target.current_review_mapping ORDER BY source_review_id LIMIT %s)
             SELECT count(*) applicable,count(*) FILTER(WHERE scope.logical_review_id IS NOT NULL AND scope.source_review_id IS NOT NULL),
               count(*) FILTER(WHERE scope.logical_review_id IS NOT NULL AND rr.source_review_id IS NOT NULL
                 AND sr.content_sha256=rr.content_sha256 AND l.content_sha256=rr.content_sha256
                 AND scope.mapping_reason<>'' AND scope.rule_version<>'' AND scope.source_role IS NOT NULL
                 AND scope.evidence<>'{}'::jsonb),
               count(*) FILTER(WHERE scope.source_role='exact_duplicate') conflicts
             FROM scope LEFT JOIN staging.review_record rr USING(source_review_id)
             LEFT JOIN raw.source_record sr USING(source_record_id)
             LEFT JOIN staging.review_parse_locator l
               ON l.source_review_id=scope.source_review_id AND l.parser_version=%s""",
            (cap, parser),
        ).fetchone()
        mapping_result = FieldResult("review_mapping", "relationship", int(mapping[0]), int(mapping[1]), int(mapping[2]),
                                     int(mapping[0]-mapping[1]), int(mapping[1]-mapping[2]), int(mapping[2]),
                                     conflict_count=0,
                                     denominator_note="active mapping rows; source locator/hash, rule, reason and mapping evidence required")
        _put_result(conn, audit_run_id, mapping_result)
        _mark_component(conn, audit_run_id, "review_mapping", "completed", mapping_result.applicable_count, mapping_result.missing_lineage_count)

        results = product_results + movie_results + [person_result] + alias_results + relation_results + logical_results + [mapping_result]
        totals = {
            "field_count": len(results),
            "applicable_sum": sum(r.applicable_count for r in results),
            "nonempty_sum": sum(r.nonempty_count for r in results),
            "complete_lineage_sum": sum(r.complete_lineage_count for r in results),
            "missing_value_sum": sum(r.missing_value_count for r in results),
            "missing_lineage_sum": sum(r.missing_lineage_count for r in results),
            "conflict_sum": sum(r.conflict_count for r in results),
        }
        conn.execute(
            "UPDATE meta.lineage_audit_run SET status='completed',finished_at=clock_timestamp(),stats=%s::jsonb WHERE audit_run_id=%s",
            (_json(totals), audit_run_id),
        )
        return {"audit_run_id": audit_run_id, "audit_version": AUDIT_VERSION, "full_scope": full,
                "active_parser_version": parser, "active_dedupe_rule_version": dedupe,
                **totals, "results": [asdict(item) for item in results]}


def mark_audit_failed(database_url: str, *, batch_id: str, error: str) -> None:
    import psycopg

    with psycopg.connect(_url(database_url)) as conn:
        conn.execute(
            """UPDATE meta.lineage_audit_run SET status='failed',finished_at=clock_timestamp(),
                 stats=stats || jsonb_build_object('error',%s)
               WHERE audit_version=%s AND batch_id=%s""",
            (error, AUDIT_VERSION, batch_id),
        )
        conn.execute(
            """UPDATE meta.lineage_audit_progress SET status='failed',
                 details=details || jsonb_build_object('error',%s),updated_at=clock_timestamp()
               WHERE audit_run_id=(SELECT audit_run_id FROM meta.lineage_audit_run
                 WHERE audit_version=%s AND batch_id=%s)
                 AND status='running'""",
            (error, AUDIT_VERSION, batch_id),
        )


def audit_status(database_url: str, *, batch_id: str | None = None) -> dict[str, Any]:
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(_url(database_url), row_factory=dict_row) as conn:
        if batch_id:
            run = conn.execute(
                "SELECT * FROM meta.lineage_audit_run WHERE audit_version=%s AND batch_id=%s",
                (AUDIT_VERSION, batch_id),
            ).fetchone()
        else:
            run = conn.execute(
                "SELECT * FROM meta.lineage_audit_run WHERE audit_version=%s ORDER BY audit_run_id DESC LIMIT 1",
                (AUDIT_VERSION,),
            ).fetchone()
        if not run:
            return {"run": None, "progress": [], "results": []}
        progress = conn.execute(
            "SELECT * FROM meta.lineage_audit_progress WHERE audit_run_id=%s ORDER BY component",
            (run["audit_run_id"],),
        ).fetchall()
        results = conn.execute(
            "SELECT * FROM integration.lineage_audit_result WHERE audit_run_id=%s ORDER BY entity_type,field_name",
            (run["audit_run_id"],),
        ).fetchall()
        return {"run": dict(run), "progress": [dict(v) for v in progress], "results": [dict(v) for v in results]}


def run_impact_demo(database_url: str, *, batch_id: str) -> dict[str, Any]:
    """Recompute one real selected movie title in an isolated shadow entity and retain both candidates."""
    import psycopg

    with psycopg.connect(_url(database_url)) as conn:
        parser, dedupe = _active_versions(conn)
        run_id = _etl_run(conn, batch_id, parser, dedupe)
        row = conn.execute(
            """SELECT m.stable_key,cfs.selected_assertion_id,fa.source_record_id,fa.raw_value,fa.normalized_value
               FROM target.movie m JOIN target.product_movie pm USING(movie_id)
               JOIN integration.current_field_selection cfs ON cfs.entity_type='movie'
                 AND cfs.entity_key=m.stable_key AND cfs.field_name='canonical_title'
               JOIN integration.field_assertion fa ON fa.assertion_id=cfs.selected_assertion_id
               WHERE pm.mapping_status IN ('verified','manual','predicted') AND fa.source_record_id IS NOT NULL
               ORDER BY m.movie_id LIMIT 1"""
        ).fetchone()
        if not row:
            raise RuntimeError("no real adopted movie title assertion available")
        entity_key = f"impact-demo:{row[0]}"
        assertion = conn.execute(
            """INSERT INTO integration.field_assertion(
                 entity_type,entity_key,field_name,raw_value,normalized_value,value_semantics,
                 source_record_id,field_locator,extraction_rule_version,confidence,quality_status,run_id)
               SELECT 'audit_movie_shadow',%s,'canonical_title',%s::jsonb,%s::jsonb,
                 'isolated recomputation from real selected input',%s,
                 jsonb_build_object('input_assertion_id',%s,'operation','unicode_and_whitespace_normalization'),
                 %s,1.0,'inferred',%s
               WHERE NOT EXISTS (SELECT 1 FROM integration.field_assertion
                 WHERE entity_type='audit_movie_shadow' AND entity_key=%s
                   AND field_name='canonical_title' AND extraction_rule_version=%s)
               RETURNING assertion_id""",
            (entity_key, _json(row[3]), _json(row[4]), row[2], row[1], AUDIT_VERSION, run_id,
             entity_key, AUDIT_VERSION),
        ).fetchone()
        if assertion:
            output_id = int(assertion[0])
        else:
            output_id = int(conn.execute(
                """SELECT assertion_id FROM integration.field_assertion
                   WHERE entity_type='audit_movie_shadow' AND entity_key=%s
                     AND field_name='canonical_title' AND extraction_rule_version=%s""",
                (entity_key, AUDIT_VERSION),
            ).fetchone()[0])
        conn.execute(
            """INSERT INTO integration.assertion_dependency(
                 output_assertion_id,input_assertion_id,operation_type,rule_id,run_id)
               VALUES (%s,%s,'unicode_and_whitespace_normalization',%s,%s) ON CONFLICT DO NOTHING""",
            (output_id, row[1], AUDIT_VERSION, run_id),
        )
        conn.execute(
            """INSERT INTO integration.field_selection(
                 entity_type,entity_key,field_name,selection_version,selected_assertion_id,
                 selected_assertion_ids,decision_status,decision_reason,run_id)
               VALUES ('audit_movie_shadow',%s,'canonical_title',%s,%s,ARRAY[%s,%s],
                 'selected','isolated impact test selected recomputation; original input retained as candidate',%s)
               ON CONFLICT(entity_type,entity_key,field_name,selection_version) DO UPDATE SET
                 selected_assertion_id=EXCLUDED.selected_assertion_id,
                 selected_assertion_ids=EXCLUDED.selected_assertion_ids,
                 decision_reason=EXCLUDED.decision_reason,run_id=EXCLUDED.run_id,
                 decided_at=clock_timestamp()""",
            (entity_key, AUDIT_VERSION, output_id, output_id, row[1], run_id),
        )
        affected = conn.execute(
            """SELECT count(*) FROM integration.source_record_impact
               WHERE source_record_id=%s AND entity_key IN (%s,%s)""",
            (row[2], row[0], entity_key),
        ).fetchone()[0]
        stats = {"source_record_id": int(row[2]), "real_movie_key": row[0], "shadow_entity_key": entity_key,
                 "input_assertion_id": int(row[1]), "output_assertion_id": output_id,
                 "affected_rows_found": int(affected), "target_value_changed": False}
        conn.execute(
            "UPDATE meta.etl_run SET status='succeeded',finished_at=clock_timestamp(),stats=%s::jsonb WHERE run_id=%s",
            (_json(stats), run_id),
        )
        return {"run_id": run_id, **stats}
