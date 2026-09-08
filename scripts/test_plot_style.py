import unittest

from scripts.plot_style import (
    ACCESSIBLE_DIVERGING_CMAP,
    OKABE_ITO,
    canonical_family,
    family_color,
    family_color_ramp,
    family_hatch,
    family_linestyle,
    family_marker,
)


class PlotStyleTests(unittest.TestCase):
    def test_dense_batch_families_have_unique_base_colors(self):
        families = ("UBQ", "LUBQ", "SegQueue", "moodycamel::CQ", "Mutex+VecDeque")
        self.assertEqual(len(families), len({family_color(family) for family in families}))

    def test_lubq_and_segqueue_have_redundant_accessible_identities(self):
        self.assertEqual(OKABE_ITO["vermillion"], family_color("LUBQ"))
        self.assertEqual(OKABE_ITO["bluish_green"], family_color("segqueue"))
        self.assertNotEqual(family_linestyle("LUBQ"), family_linestyle("SegQueue"))
        self.assertNotEqual(family_marker("LUBQ"), family_marker("SegQueue"))
        self.assertNotEqual(family_hatch("LUBQ"), family_hatch("SegQueue"))

    def test_scalar_and_batched_aliases_keep_one_family_identity(self):
        for scalar, batched in (
            ("UBQ", "UBQ batched"),
            ("LUBQ", "LUBQ batched"),
            ("crossbeam SegQueue", "crossbeam SegQueue batched"),
        ):
            self.assertEqual(canonical_family(scalar), canonical_family(batched))
            self.assertEqual(family_color(scalar), family_color(batched))
            self.assertEqual(family_linestyle(scalar), family_linestyle(batched))

    def test_family_ramps_remain_visible_and_dark_in_order(self):
        ramp = family_color_ramp("LUBQ")
        light = ramp(0.0)
        dark = ramp(1.0)
        luminance = lambda color: (
            0.2126 * color[0] + 0.7152 * color[1] + 0.0722 * color[2]
        )

        self.assertEqual(1.0, light[3])
        self.assertGreater(luminance(light), luminance(dark))
        self.assertLess(max(light[:3]), 1.0)

    def test_diverging_heatmaps_avoid_red_green_encoding(self):
        self.assertEqual("PuOr", ACCESSIBLE_DIVERGING_CMAP)


if __name__ == "__main__":
    unittest.main()
