\set ON_ERROR_STOP on
\pset pager off

\if :{?batch_id}
\else
\set batch_id 'final-lineage-bounded-10000-v2'
\endif

\echo 'Audit run and active-version identity'
SELECT * FROM meta.lineage_audit_run
WHERE audit_version='final-field-lineage-v1' AND batch_id=:'batch_id';

\echo 'Field inventory and coverage result'
SELECT c.entity_type,c.field_name,c.field_semantics,c.applicable_scope,
       c.final_value_location,c.lineage_mode,c.generation_rule,c.required_source,
       r.applicable_count,r.nonempty_count,r.complete_lineage_count,
       r.missing_value_count,r.missing_lineage_count,
       r.verified_count,r.manual_count,r.inferred_count,r.conflict_count,
       r.denominator_note
FROM integration.lineage_field_catalog c
LEFT JOIN meta.lineage_audit_run ar
  ON ar.audit_version='final-field-lineage-v1' AND ar.batch_id=:'batch_id'
LEFT JOIN integration.lineage_audit_result r
  ON r.audit_run_id=ar.audit_run_id AND r.entity_type=c.entity_type AND r.field_name=c.field_name
ORDER BY c.entity_type,c.field_name;

\echo 'Gap detail (empty means every nonempty value in scope has a complete chain)'
SELECT g.* FROM integration.lineage_audit_gap g
JOIN meta.lineage_audit_run ar USING(audit_run_id)
WHERE ar.audit_version='final-field-lineage-v1' AND ar.batch_id=:'batch_id'
ORDER BY g.entity_type,g.field_name,g.entity_key;

\echo 'Repair history and idempotent current counts'
SELECT repair_version,entity_type,field_name,repair_action,new_state,reason,repaired_at
FROM integration.lineage_repair_event
WHERE repair_version='final-field-lineage-repair-v1'
ORDER BY repair_event_id;

SELECT
 (SELECT count(*) FROM integration.field_assertion
   WHERE extraction_rule_version='final-field-lineage-repair-v1'
     AND entity_type='product' AND field_name='classification') classification_assertions,
 (SELECT count(*) FROM integration.field_selection
   WHERE selection_version='final-field-lineage-repair-v1'
     AND entity_type='product' AND field_name='classification') classification_selections,
 (SELECT count(*) FROM integration.field_selection
   WHERE selection_version='final-field-lineage-repair-v1'
     AND entity_type='movie' AND field_name='release_precision') precision_selections,
 (SELECT count(*) FROM target.movie_person WHERE relation_status='superseded') superseded_historical_relations,
 (SELECT count(*) FROM target.current_movie_person) current_movie_person_relations,
 (SELECT count(*) FROM integration.relationship_source_link
   WHERE relationship_type='product_movie') product_movie_source_links;

\echo 'Impact/recompute demonstration: original and derived assertions remain queryable'
SELECT fa.assertion_id,fa.entity_type,fa.entity_key,fa.field_name,
       fa.source_record_id,fa.normalized_value,fa.extraction_rule_version,
       cfs.selected_assertion_id,cfs.selected_assertion_ids,cfs.decision_reason,
       ad.input_assertion_id,ad.operation_type,ad.rule_id
FROM integration.field_assertion fa
LEFT JOIN integration.current_field_selection cfs
  ON cfs.entity_type=fa.entity_type AND cfs.entity_key=fa.entity_key AND cfs.field_name=fa.field_name
LEFT JOIN integration.assertion_dependency ad ON ad.output_assertion_id=fa.assertion_id
WHERE fa.entity_type='audit_movie_shadow'
ORDER BY fa.assertion_id;

\echo 'Candidate inventory versus adopted works'
SELECT (SELECT count(*) FROM target.movie) database_movie_inventory,
       (SELECT count(DISTINCT movie_id) FROM target.product_movie
         WHERE mapping_status IN ('verified','manual','predicted')) adopted_movies,
       (SELECT count(*) FROM target.product_movie
         WHERE mapping_status IN ('verified','manual','predicted')) adopted_product_movie_rows;
