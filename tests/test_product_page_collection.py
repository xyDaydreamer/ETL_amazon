import unittest

from amazon_movies.stage3_crawler import MAX_CANDIDATE_SAMPLE, assess_product_response, validate_candidate_limit


ASIN = "B000000001"


def assess(body: bytes, status: int = 200, url: str | None = None):
    return assess_product_response(
        target_asin=ASIN, status=status,
        final_url=url or f"https://www.amazon.com/dp/{ASIN}", body=body,
    )


class ProductPageAssessmentTests(unittest.TestCase):
    def test_candidate_limit_is_independent_and_capped_at_200(self):
        self.assertEqual(validate_candidate_limit(200), MAX_CANDIDATE_SAMPLE)
        for invalid in (0, 201, -1):
            with self.assertRaises(ValueError):
                validate_candidate_limit(invalid)

    def test_http_200_title_only_is_not_success(self):
        result = assess(b"<html><title>Amazon</title></html>")
        self.assertEqual((result.outcome, result.error_code), ("parse_failed", "target_asin_not_proven"))


    def test_valid_page_requires_matching_asin_and_product_structure(self):
        body = f'<html><input name="ASIN" value="{ASIN}"><span id="productTitle">Film</span></html>'.encode()
        result = assess(body)
        self.assertEqual(result.outcome, "success")
        self.assertEqual(result.observed_asins, (ASIN,))


    def test_verification_page_is_restricted_even_with_200(self):
        result = assess(b"<html><title>Robot Check</title>Enter the characters you see below</html>")
        self.assertEqual((result.outcome, result.error_code), ("access_restricted", "verification_page"))


    def test_http_access_and_retry_classification(self):
        forbidden = assess(b"denied", status=403)
        throttled = assess(b"slow down", status=429)
        self.assertEqual(forbidden.outcome, "access_restricted")
        self.assertFalse(forbidden.retryable)
        self.assertEqual(throttled.outcome, "access_restricted")
        self.assertTrue(throttled.retryable)


    def test_not_found_and_soft_not_found(self):
        self.assertEqual(assess(b"missing", status=404).outcome, "not_found")
        self.assertEqual(assess(b"Sorry! We couldn't find that page").outcome, "not_found")


    def test_redirected_asin_mismatch_is_not_success(self):
        other = "B000000002"
        body = f'<input name="ASIN" value="{other}"><span id="productTitle">Other</span>'.encode()
        result = assess(body, url=f"https://www.amazon.com/dp/{other}")
        self.assertEqual((result.outcome, result.error_code), ("parse_failed", "redirected_asin_mismatch"))


    def test_matching_asin_without_product_template_is_parse_failure(self):
        result = assess(f'<input name="ASIN" value="{ASIN}">'.encode())
        self.assertEqual((result.outcome, result.error_code), ("parse_failed", "product_template_not_proven"))

    def test_product_marker_without_title_is_not_success(self):
        body = f'<input name="ASIN" value="{ASIN}"><div id="dp"></div>'.encode()
        self.assertEqual(assess(body).error_code, "product_title_missing")

    def test_reordered_attributes_and_jsonld_product(self):
        html_page = f'<input value="{ASIN}" name="ASIN"><span class="x" id="productTitle">A Film</span>'.encode()
        self.assertEqual(assess(html_page).outcome, "success")
        jsonld = (f'<script type="application/ld+json">{{"@type":"Product","sku":"{ASIN}",'
                  '"name":"JSON Film"}</script>').encode()
        self.assertEqual(assess(jsonld).product_title, "JSON Film")

    def test_jsonld_without_matching_asin_is_rejected(self):
        body = b'<script type="application/ld+json">{"@type":"Product","sku":"B000000002","name":"Other"}</script>'
        self.assertEqual(assess(body).error_code, "target_asin_not_proven")


if __name__ == "__main__":
    unittest.main()
