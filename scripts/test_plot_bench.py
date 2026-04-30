import unittest

from scripts.plot_bench import immediate_winner_variant_report


def stats(ops):
    return {
        "mean_ops_per_sec": float(ops),
        "stddev_ops_per_sec": 0.0,
        "sem_ops_per_sec": 0.0,
        "samples": 1,
    }


class ImmediateWinnerVariantReportTest(unittest.TestCase):
    def test_four_part_immediate_neighbors_are_present(self):
        entries = {
            "segqueue": stats(1),
            "ubq_balanced,8,127,crossbeam": stats(100),
            "ubq_balanced,4,127,crossbeam": stats(90),
            "ubq_balanced,16,127,crossbeam": stats(90),
            "ubq_balanced,8,63,crossbeam": stats(90),
            "ubq_balanced,8,255,crossbeam": stats(90),
            "ubq_balanced,8,127,yield": stats(90),
        }

        report = immediate_winner_variant_report(entries, "1p1c")

        self.assertEqual([], report["missing_required_labels"])
        self.assertNotIn(
            "ubq_balanced,8,127,crossbeam,cas",
            report["required_labels"],
        )

    def test_pool_zero_is_an_immediate_neighbor(self):
        entries = {
            "ubq_balanced,1,127,crossbeam": stats(100),
            "ubq_balanced,0,127,crossbeam": stats(90),
            "ubq_balanced,2,127,crossbeam": stats(90),
            "ubq_balanced,1,63,crossbeam": stats(90),
            "ubq_balanced,1,255,crossbeam": stats(90),
            "ubq_balanced,1,127,yield": stats(90),
        }

        report = immediate_winner_variant_report(entries, "1p1c")

        self.assertIn(
            "ubq_balanced,0,127,crossbeam",
            report["present_required_labels"],
        )
        self.assertEqual([], report["missing_required_labels"])

    def test_explicit_cas_alias_counts_as_present(self):
        entries = {
            "ubq_balanced,8,127,crossbeam,cas": stats(100),
            "ubq_balanced,4,127,crossbeam": stats(90),
            "ubq_balanced,16,127,crossbeam": stats(90),
            "ubq_balanced,8,63,crossbeam": stats(90),
            "ubq_balanced,8,255,crossbeam": stats(90),
            "ubq_balanced,8,127,yield": stats(90),
        }

        report = immediate_winner_variant_report(entries, "1p1c")

        self.assertEqual([], report["missing_required_labels"])

    def test_scenario_excludes_block_neighbor_below_producer_count(self):
        entries = {
            "ubq_balanced,8,127,crossbeam": stats(100),
            "ubq_balanced,4,127,crossbeam": stats(90),
            "ubq_balanced,16,127,crossbeam": stats(90),
            "ubq_balanced,8,255,crossbeam": stats(90),
            "ubq_balanced,8,127,yield": stats(90),
        }

        report = immediate_winner_variant_report(entries, "64p1c")

        self.assertNotIn(
            "ubq_balanced,8,63,crossbeam",
            report["required_labels"],
        )
        self.assertEqual([], report["missing_required_labels"])


if __name__ == "__main__":
    unittest.main()
