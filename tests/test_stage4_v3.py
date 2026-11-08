from amazon_movies.stage4_v3 import (
    RULE_VERSION,
    classify_product_type,
    decide,
    evaluate_candidate,
    extract_all_candidates,
    number_evidence,
    person_evidence,
    resolve_label_binding,
    title_evidence,
    type_evidence,
    year_evidence,
)


def _candidate(candidate_id: str, title: str, principals: list[str], *, rank: int = 1, imdb_type: str = "feature") -> dict:
    return {
        "id": candidate_id,
        "title": title,
        "year": 2008,
        "type": imdb_type,
        "rank": rank,
        "principals": principals,
        "raw_variants": [],
    }


def test_rule_version() -> None:
    assert RULE_VERSION == "stage4-first50-movie-match-v3.1-evidence-extraction"


def test_extracts_all_tt_candidates_before_display_cut_and_deduplicates_id() -> None:
    payload = {"d": [
        {"id": f"tt{i:07d}", "l": f"Movie {i}", "q": "feature", "rank": i, "s": "Actor One"}
        for i in range(1, 9)
    ] + [
        {"id": "tt0000001", "l": "Movie 1", "q": "feature", "rank": 99, "s": "Actor Two"},
        {"id": "nm0000001", "l": "Not a title"},
    ]}
    candidates = extract_all_candidates(payload, "Movie")
    assert len(candidates) == 8
    first = next(value for value in candidates if value["id"] == "tt0000001")
    assert first["principals"] == ["Actor One", "Actor Two"]
    assert len(first["raw_variants"]) == 2


def test_unicode_title_and_subtitle_route_preserve_non_latin() -> None:
    unicode_evidence = title_evidence("英雄：第一章 [Blu-ray]", "英雄：第一章")
    assert "英雄" in unicode_evidence["sequence_matcher_input_product"]
    assert unicode_evidence["T_exact"] is True
    subtitle_evidence = title_evidence(
        "An Extraordinary Journey: Alpha",
        "An Extraordinary Journey: Completely Different Subtitle",
    )
    assert subtitle_evidence["main_subtitle_match"] is True
    assert subtitle_evidence["T_strong"] is True


def test_short_generic_main_title_cannot_use_subtitle_only_route() -> None:
    evidence = title_evidence("Frozen: Alpha", "Frozen: Completely Different Work")
    assert evidence["main_title_route_blocked"] is True
    assert evidence["main_subtitle_match"] is False


def test_leading_article_variant_is_strong_but_not_exact_or_alias() -> None:
    evidence = title_evidence(
        "The World's Biggest and Baddest Bugs",
        "World's Biggest and Baddest Bugs",
    )
    assert evidence["leading_article_variant"] is True
    assert evidence["T_strong"] is True
    assert evidence["T_exact"] is False
    assert evidence["alias_exact"] is False


def test_name_boundary_ann_lee_does_not_match_joann_lee() -> None:
    assert person_evidence("Ann Lee", None, ["Joann Lee appears here."], 1) is None


def test_role_names_are_not_actor_credits_and_parenthesized_performers_are() -> None:
    description = [
        "Baron Manfred von Richthofen (MATTHIAS SCHWEIGHOFER) fights Captain Roy Brown (JOSEPH FIENNES)."
    ]
    manfred = person_evidence("Manfred von Richthofen", None, description, 1)
    roy = person_evidence("Roy Brown", None, description, 1)
    matthias = person_evidence("Matthias Schweighöfer", "Matthias Schweighofer", description, 1)
    joseph = person_evidence("Joseph Fiennes", None, description, 1)
    assert manfred["effective_class"] == "character_or_subject"
    assert roy["effective_class"] == "character_or_subject"
    assert matthias["effective_class"] == "explicit_credit"
    assert joseph["effective_class"] == "explicit_credit"


def test_other_work_reference_is_not_credit() -> None:
    evidence = person_evidence(
        "Alex Smith", None,
        ["Known for another film, Alex Smith is mentioned only as a comparison."], 1,
    )
    assert evidence["effective_class"] == "other_work_reference"


def test_generic_actor_host_and_performance_contexts_are_explicit() -> None:
    actor = person_evidence(
        "Paul Newman", None,
        ["Experience legendary actor Paul Newman in this drama."], 1,
    )
    host = person_evidence(
        "Ruud Kleinpaste", None,
        ["The program follows host Ruud Kleinpaste around the globe."], 1,
    )
    performance = person_evidence(
        "John Malkovich", None,
        ["John Malkovich (In the Line of Fire, 1993) stars as Professor Lurie."], 1,
    )
    assert actor["effective_class"] == "explicit_credit"
    assert host["effective_class"] == "explicit_credit"
    assert performance["effective_class"] == "explicit_credit"


def test_top_billed_and_leading_metadata_cast_lists_are_explicit() -> None:
    top_billed = person_evidence(
        "Dennis Quaid", None,
        ["Ben Foster and Dennis Quaid--two watchable actors--are top-billed in the film."], 1,
    )
    metadata_cast = person_evidence(
        "Justin Timberlake", "Justin Timberlake",
        ["Jeff Bridges, Justin Timberlake <br><b>Item Type: </b>BLU-RAY DVD Movie"], 1,
    )
    assert top_billed["effective_class"] == "explicit_credit"
    assert metadata_cast["effective_class"] == "explicit_credit"


def test_brand_and_explicit_description_count_once_as_explicit() -> None:
    evaluated = evaluate_candidate(
        _candidate("tt1", "Example Film", ["Robin Wright"]),
        product_title="Example Film",
        product_type={"class": "movie", "basis": "test"},
        brand="Robin Wright",
        descriptions=["This film stars Robin Wright."],
        source_record_id=1,
    )
    assert evaluated["person_counts"]["explicit_credit"] == 1
    assert evaluated["person_counts"]["brand_support"] == 0
    assert evaluated["score_breakdown"]["explicit_credit"] == 20


def test_missing_year_has_no_penalty_or_conflict() -> None:
    evidence = year_evidence(2008, [])
    assert evidence["status"] == "missing"
    assert evidence["score"] == 0
    assert evidence["hard_conflict"] is False


def test_type_unknown_is_not_conflict() -> None:
    assert type_evidence("unknown", "feature")["compatibility"] == "unknown"


def test_knockout_word_alone_does_not_make_sports_highlight() -> None:
    result = classify_product_type("Knockout", ["A dramatic film about one boxer."])
    assert result["class"] == "movie"


def test_3d_and_4k_are_not_installment_numbers() -> None:
    evidence = number_evidence("Example 3D [4K UHD]", "Example")
    assert evidence["hard_conflict"] is False


def test_arabic_and_roman_installment_numbers_correspond() -> None:
    evidence = number_evidence("Death Race II", "Death Race 2")
    assert evidence["hard_conflict"] is False


def test_multiple_eligible_candidates_stay_ambiguous() -> None:
    product_type = {"class": "movie", "basis": "test"}
    descriptions = ["This film stars Alex Smith and Jamie Doe."]
    evaluations = [
        evaluate_candidate(
            _candidate("tt1", "Same Film", ["Alex Smith"], rank=1),
            product_title="Same Film", product_type=product_type, brand=None,
            descriptions=descriptions, source_record_id=1,
        ),
        evaluate_candidate(
            _candidate("tt2", "Same Film", ["Jamie Doe"], rank=999),
            product_title="Same Film", product_type=product_type, brand=None,
            descriptions=descriptions, source_record_id=1,
        ),
    ]
    assert decide(evaluations)["status"] == "needs_review_ambiguous"


def test_hard_conflict_cannot_outrank_conflict_free_candidate() -> None:
    product_type = {"class": "unknown", "basis": "test"}
    conflicting = evaluate_candidate(
        _candidate("tt1", "Castle Rock", [], rank=1, imdb_type="TV series"),
        product_title="Castle Rock", product_type=product_type, brand=None,
        descriptions=[], source_record_id=1,
    )
    conflict_free = evaluate_candidate(
        _candidate("tt2", "Castle Rock", [], rank=999, imdb_type="feature"),
        product_title="Castle Rock", product_type=product_type, brand=None,
        descriptions=[], source_record_id=1,
    )
    decision = decide([conflicting, conflict_free])
    assert decision["best"]["id"] == "tt2"


def test_mention_only_cannot_satisfy_auto_confirmation() -> None:
    evaluated = evaluate_candidate(
        _candidate("tt1", "Same Film", ["Alex Smith"]),
        product_title="Same Film", product_type={"class": "unknown", "basis": "test"},
        brand=None, descriptions=["Alex Smith appears in this text."], source_record_id=1,
    )
    assert evaluated["person_counts"]["mention_only"] == 1
    assert evaluated["auto_confirmation_eligible"] is False


def test_pool_label_does_not_inherit_predicted_candidate() -> None:
    scope, candidate_id, label = resolve_label_binding(
        {
            "human_label": "no_match_in_pool",
            "imdb_candidate_id": "tt1234567",
            "human_label_scope": "",
            "human_label_candidate_id": "",
        },
        2,
    )
    assert (scope, candidate_id, label) == ("pool", None, "no_match_in_pool")


def test_candidate_mismatch_and_pool_absence_are_distinct() -> None:
    assert resolve_label_binding(
        {
            "human_label": "incorrect_match",
            "human_label_candidate_id": "tt7654321",
            "imdb_candidate_id": "tt1234567",
        },
        2,
    )[:2] == ("candidate", "tt7654321")
    try:
        resolve_label_binding(
            {"human_label": "incorrect_match", "human_label_scope": "pool"}, 3
        )
    except ValueError as error:
        assert "must bind to one candidate" in str(error)
    else:
        raise AssertionError("candidate mismatch must not be stored as a pool label")
