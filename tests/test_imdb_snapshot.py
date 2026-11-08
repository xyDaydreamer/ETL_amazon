from __future__ import annotations

import gzip
import tempfile
import unittest
from datetime import date
from pathlib import Path

from amazon_movies.imdb_snapshot import REQUIRED_HEADERS, inspect_snapshot, title_evidence


ROWS = {
    "title.basics.tsv.gz": ["tt0000001", "movie", "Pilot Film", "Pilot Film", "0", "2001", r"\N", "90", "Drama"],
    "title.akas.tsv.gz": ["tt0000001", "1", "Le Film Pilote", "FR", "fr", "imdbDisplay", r"\N", "0"],
    "title.crew.tsv.gz": ["tt0000001", "nm0000001", r"\N"],
    "title.principals.tsv.gz": ["tt0000001", "1", "nm0000002", "actor", r"\N", '["Alex"]'],
    "name.basics.tsv.gz": ["nm0000001", "Director One", "1970", r"\N", "director", "tt0000001"],
}


class ImdbSnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.root = Path(self._temporary.name)
        for file_name, header in REQUIRED_HEADERS.items():
            rows = [ROWS[file_name]]
            if file_name == "name.basics.tsv.gz":
                rows.append(["nm0000002", "Actor Two", "1980", r"\N", "actor", "tt0000001"])
            with gzip.open(self.root / file_name, "wt", encoding="utf-8", newline="") as stream:
                stream.write("\t".join(header) + "\n")
                for row in rows:
                    stream.write("\t".join(row) + "\n")

    def tearDown(self) -> None:
        self._temporary.cleanup()

    def test_bounded_inspection_does_not_claim_full_integrity(self) -> None:
        result = inspect_snapshot(self.root, date(2026, 11, 5))
        self.assertTrue(result["complete_file_set"])
        self.assertTrue(result["all_headers_verified"])
        self.assertFalse(result["all_gzip_streams_verified"])
        self.assertTrue(all(item["file_sha256"] is None for item in result["files"]))

    def test_full_verification_hashes_and_reads_each_stream(self) -> None:
        result = inspect_snapshot(self.root, date(2026, 11, 5), full_verify=True, progress_every=0)
        self.assertTrue(result["all_gzip_streams_verified"])
        self.assertTrue(all(len(item["file_sha256"]) == 64 for item in result["files"]))
        counts = {item["file_name"]: item["uncompressed_record_count"] for item in result["files"]}
        self.assertEqual(counts["name.basics.tsv.gz"], 2)

    def test_title_evidence_links_tconst_to_nconsts_with_source_positions(self) -> None:
        result = title_evidence(self.root, "tt0000001")
        self.assertEqual(result["title"]["primaryTitle"], "Pilot Film")
        self.assertEqual(set(result["names"]), {"nm0000001", "nm0000002"})
        self.assertFalse(result["missing_name_ids"])
        self.assertEqual(result["principals"][0]["source_line"], 2)
        self.assertEqual(len(result["principals"][0]["raw_line_sha256"]), 64)
        self.assertEqual(result["names"]["nm0000002"]["source_url"], "https://www.imdb.com/name/nm0000002/")

    def test_large_tsv_field_is_not_limited_by_csv_default(self) -> None:
        huge_title = "X" * 140_000
        with gzip.open(self.root / "title.akas.tsv.gz", "wt", encoding="utf-8", newline="") as stream:
            stream.write("\t".join(REQUIRED_HEADERS["title.akas.tsv.gz"]) + "\n")
            stream.write("\t".join(["tt0000001", "1", huge_title, "US", "en", "imdbDisplay", r"\N", "0"]) + "\n")
        result = title_evidence(self.root, "tt0000001")
        self.assertEqual(len(result["aliases"][0]["title"]), 140_000)


if __name__ == "__main__":
    unittest.main()
