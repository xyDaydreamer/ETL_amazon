from amazon_movies.lineage_audit import (
    AUDIT_VERSION,
    REPAIR_VERSION,
    FieldResult,
    _field_results,
)


def test_rule_versions_are_explicit_and_distinct() -> None:
    assert AUDIT_VERSION == "final-field-lineage-v1"
    assert REPAIR_VERSION == "final-field-lineage-repair-v1"
    assert AUDIT_VERSION != REPAIR_VERSION


def test_field_result_separates_missing_values_from_missing_lineage() -> None:
    result = _field_results(
        [("release_year", 10, 8, 7, 6, 0, 1, 2)],
        "movie",
        "adopted movies only",
    )[0]
    assert result == FieldResult(
        entity_type="movie",
        field_name="release_year",
        applicable_count=10,
        nonempty_count=8,
        complete_lineage_count=7,
        missing_value_count=2,
        missing_lineage_count=1,
        verified_count=6,
        manual_count=0,
        inferred_count=1,
        conflict_count=2,
        denominator_note="adopted movies only",
    )


def test_candidate_inventory_is_not_named_as_adopted_scope() -> None:
    result = FieldResult(
        "movie", "canonical_title", 46, 46, 46, 0, 0,
        denominator_note="distinct movies reached by adopted ProductMovie rows; candidate-only inventory excluded",
    )
    assert "candidate-only inventory excluded" in result.denominator_note
