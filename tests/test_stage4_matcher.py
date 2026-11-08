from amazon_movies.stage4_matcher import _person_in_text, _years, query_title
from amazon_movies.stage4_reevaluator import (
    candidate_type_evidence,
    classify_product_intent,
    decide_candidate,
    evaluate_candidate,
)


def test_query_title_removes_carrier_but_not_movie_identity() -> None:
    assert query_title("Vacancy [Blu-ray] Special Edition") == "Vacancy"
    assert query_title("An American Christmas Carol VHS") == "An American Christmas Carol"


def test_year_extraction_is_evidence_not_date_conversion() -> None:
    assert _years("Expanded Version 2009") == [2009]
    assert _years("No year") == []


def test_person_match_uses_token_boundaries() -> None:
    assert _person_in_text("Luke Wilson", "A thriller starring Luke Wilson and Kate Beckinsale")
    assert not _person_in_text("Luke Wilson", "Luke Wilsonian tribute")


def test_cached_candidate_evidence_precedes_imdb_popularity_rank() -> None:
    candidates = [
        {"id": "tt-series", "title": "The Chosen One", "year": 2023, "type": "TV series",
         "principals": ["Bobby Luhnow"], "similarity": 1.0, "rank": 1},
        {"id": "tt-video", "title": "The Chosen One", "year": 2010, "type": "video",
         "principals": ["Steve Buscemi"], "similarity": 1.0, "rank": 1000},
    ]
    evaluated = [
        evaluate_candidate(
            candidate, query_title="The Chosen One", product_class="single_movie",
            brand="Rob Schneider", descriptions=["Rob Schneider and Steve Buscemi star."],
            source_record_id=99,
        )
        for candidate in candidates
    ]
    decision = decide_candidate(evaluated)
    assert decision["status"] == "rule_verified"
    assert decision["best"]["id"] == "tt-video"


def test_multiple_sufficient_candidates_remain_ambiguous() -> None:
    candidates = [
        {"id": "tt-one", "title": "Same Film", "year": 2001, "type": "feature",
         "principals": ["Alex Smith"], "similarity": 1.0, "rank": 10},
        {"id": "tt-two", "title": "Same Film", "year": 2002, "type": "feature",
         "principals": ["Alex Smith"], "similarity": 1.0, "rank": 20},
    ]
    evaluated = [
        evaluate_candidate(
            candidate, query_title="Same Film", product_class="single_movie",
            brand=None, descriptions=["Alex Smith stars."], source_record_id=7,
        )
        for candidate in candidates
    ]
    assert decide_candidate(evaluated)["status"] == "needs_review_ambiguous"


def test_type_and_installment_conflicts_produce_no_suitable_candidate() -> None:
    product_class, _ = classify_product_intent(
        "UFC: Ultimate Knockouts 8", ["UFC sports highlights and knockouts"]
    )
    evaluated = evaluate_candidate(
        {"id": "tt-three", "title": "UFC: Ultimate Knockouts 3", "year": 2004,
         "type": "video", "principals": [], "similarity": 0.95833, "rank": 1},
        query_title="UFC: Ultimate Knockouts 8", product_class=product_class,
        brand="Anderson Silva", descriptions=["UFC sports highlights and knockouts"],
        source_record_id=12,
    )
    assert candidate_type_evidence(product_class, "video")["compatibility"] == "conflict"
    assert evaluated["title_evidence"]["number_conflict"] is True
    assert decide_candidate([evaluated])["status"] == "no_suitable_candidate"


def test_same_person_in_brand_and_description_is_one_person_with_two_locations() -> None:
    evaluated = evaluate_candidate(
        {"id": "tt-x", "title": "Example", "year": 2009, "type": "feature",
         "principals": ["Robin Wright"], "similarity": 1.0, "rank": 1},
        query_title="Example", product_class="single_movie", brand="Robin Wright",
        descriptions=["A drama starring Robin Wright."], source_record_id=55,
    )
    assert evaluated["unique_person_match_count"] == 1
    locations = evaluated["person_evidence"][0]["locations"]
    assert {location["field_path"] for location in locations} == {
        "raw_payload.brand", "raw_payload.description[0]"
    }


def test_editorial_comparison_does_not_change_product_work_type() -> None:
    product_class, _ = classify_product_intent(
        "Vacancy",
        [
            "A thriller starring Luke Wilson and Kate Beckinsale.",
            "A later editorial review compares its atmosphere to a Seventies TV movie.",
        ],
    )
    assert product_class == "single_movie"
