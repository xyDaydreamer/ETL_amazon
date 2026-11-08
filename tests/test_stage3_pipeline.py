from amazon_movies.stage3_pipeline import classify_product, normalize_title


def test_title_normalization_removes_carrier_not_identity() -> None:
    assert normalize_title("An American Christmas Carol [VHS]") == "an american christmas carol"
    assert normalize_title("An American Christmas Carol VHS") == "an american christmas carol"


def test_collection_series_non_movie_and_pending_are_distinct() -> None:
    assert classify_product({"title": "Four Film Collection", "category": ["Movies"]})[0] == "collection"
    assert classify_product({"title": "Complete Series", "category": ["Movies & TV"]})[0] == "series"
    assert classify_product({"title": "Praise Aerobics VHS", "category": ["Exercise & Fitness"]})[0] == "non_movie"
    assert classify_product({"title": "Unknown Program VHS", "category": ["Movies"]})[0] == "pending"


def test_movie_requires_more_than_movies_category() -> None:
    record = {
        "title": "An American Christmas Carol VHS",
        "category": ["Movies & TV", "Movies"],
        "description": ["A narrative description. " * 10],
    }
    classification, basis = classify_product(record)
    assert classification == "movie"
    assert basis["rule"] == "movie_category_plus_narrative_description"
