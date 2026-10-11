import importlib.util
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


path = Path(__file__).parents[1] / "plugins.v3/brushflow/rules.py"
spec = importlib.util.spec_from_file_location("brushflow_rules", path)
rules = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rules)


def has_timezone_data():
    try:
        ZoneInfo("Asia/Shanghai")
        ZoneInfo("America/New_York")
        return True
    except ZoneInfoNotFoundError:
        return False


class PeerDemandTests(unittest.TestCase):
    def test_single_value_is_minimum_and_range_includes_both_ends(self):
        self.assertTrue(rules.evaluate_peer_demand(8, 10, "5")[0])
        self.assertFalse(rules.evaluate_peer_demand(4, 10, "5")[0])
        for peers in (5, 8):
            self.assertTrue(rules.evaluate_peer_demand(peers, 10, "5-8")[0])
        self.assertFalse(rules.evaluate_peer_demand(9, 10, "5-8")[0])

    def test_demand_ratio_and_zero_seeders(self):
        self.assertTrue(rules.evaluate_peer_demand(1, 10)[0])
        self.assertFalse(rules.evaluate_peer_demand(1, 11)[0])
        self.assertTrue(rules.evaluate_peer_demand(1, 0)[0])
        self.assertFalse(rules.evaluate_peer_demand(0, 0, "0")[0])

    def test_unknown_counts_only_block_enabled_conditions(self):
        for unknown in (None, "", "unknown", -1, float("nan"), float("inf"), True, 0.5):
            with self.subTest(unknown=unknown):
                self.assertFalse(rules.evaluate_peer_demand(unknown, 10)[0])
                self.assertFalse(rules.evaluate_peer_demand(1, unknown)[0])
        self.assertEqual(rules.evaluate_peer_demand(None, None, None, None), (True, None))
        self.assertEqual(rules.evaluate_peer_demand(None, None, None, 0), (True, None))
        self.assertEqual(rules.evaluate_peer_demand(1, None, "1", None), (True, None))
        self.assertFalse(rules.evaluate_peer_demand(None, 10, None, 0.1)[0])

    def test_invalid_configuration_fails_closed(self):
        for limits in ("5-1", "1-2-3", "-1", "1.5", "many"):
            self.assertFalse(rules.evaluate_peer_demand(5, 10, limits)[0])
        for ratio in (-1, float("nan"), float("inf")):
            self.assertFalse(rules.evaluate_peer_demand(5, 10, None, ratio)[0])
        self.assertTrue(rules.evaluate_peer_demand("5", "10", " 5 - 8 ")[0])


class DemandRankTests(unittest.TestCase):
    def test_demand_and_competition_both_affect_order(self):
        self.assertGreater(rules.demand_rank(20, 5, 30), rules.demand_rank(1, 5, 5))
        self.assertGreater(rules.demand_rank(20, 5, 30), rules.demand_rank(20, 50, 30))
        self.assertGreater(rules.demand_rank(20, 20, 30), rules.demand_rank(1, 1, 30))

    def test_old_high_ratio_does_not_dominate_fresh_demand(self):
        self.assertGreater(rules.demand_rank(5, 5, 30), rules.demand_rank(100, 1, 7 * 1440))
        self.assertGreater(rules.demand_rank(5, 5, 30), rules.demand_rank(5, 5, 390))

    def test_unknown_or_future_values_do_not_gain_priority(self):
        valid = rules.demand_rank(1, 10, 10)
        for values in ((None, 1, 1), (1, None, 1), (1000, 1, None),
                       (1000, 1, -1), (float("inf"), 1, 1)):
            with self.subTest(values=values):
                self.assertGreater(valid, rules.demand_rank(*values))


class PublicationAgeTests(unittest.TestCase):
    now = datetime(2026, 10, 12, 12, tzinfo=timezone.utc)

    def test_explicit_timezones_represent_the_same_age(self):
        for published in ("2026-10-12T11:00:00Z", "2026-10-12T19:00:00+08:00",
                          "2026-10-12T07:00:00-04:00"):
            with self.subTest(published=published):
                self.assertEqual(rules.publication_age_minutes(published, 8, "UTC", self.now), 60)

    def test_naive_publication_preserves_manual_correction(self):
        self.assertEqual(rules.publication_age_minutes("2026-10-12 03:00:00", 8, "UTC", self.now), 60)

    @unittest.skipUnless(has_timezone_data(), "系统未提供 IANA 时区数据")
    def test_host_zone_only_applies_to_naive_publication(self):
        self.assertEqual(rules.publication_age_minutes("2026-10-12 19:00:00", 0, "Asia/Shanghai", self.now), 60)
        self.assertEqual(rules.publication_age_minutes("2026-10-12T11:00:00Z", 8, "Asia/Shanghai", self.now), 60)

    def test_invalid_date_is_unknown_instead_of_fresh(self):
        for published in (None, "", "bad", "2026-02-30", 12345):
            self.assertIsNone(rules.publication_age_minutes(published, 0, "UTC", self.now))
        self.assertEqual(rules.publication_age_minutes("2026-10-12T13:00:00Z", 0, "UTC", self.now), -60)


class PromotionDeadlineTests(unittest.TestCase):
    def test_naive_expiry_preserves_hour_offset(self):
        deadline = rules.promotion_deadline("2026-10-11 20:00:00", 8, "UTC")
        self.assertEqual(deadline, datetime(2026, 10, 12, 3, 50, tzinfo=timezone.utc))
        self.assertEqual(
            rules.promotion_deadline(datetime(2026, 10, 11, 20), -0.5, "UTC"),
            datetime(2026, 10, 11, 19, 20, tzinfo=timezone.utc),
        )

    def test_aware_expiry_converts_instant_without_manual_offset(self):
        expected = datetime(2026, 10, 11, 11, 50, tzinfo=timezone.utc)
        for expiry in (
            "2026-10-11T12:00:00Z",
            "2026-10-11T20:00:00+08:00",
            datetime(2026, 10, 11, 20, tzinfo=timezone(timedelta(hours=8))),
        ):
            with self.subTest(expiry=expiry):
                self.assertEqual(rules.promotion_deadline(expiry, 8, "UTC"), expected)

    @unittest.skipUnless(has_timezone_data(), "系统未提供 IANA 时区数据")
    def test_named_timezone_and_dst_keep_actual_ten_minutes(self):
        local = rules.promotion_deadline("2026-10-11T12:00:00Z", 0, "Asia/Shanghai")
        self.assertIsNotNone(local)
        self.assertEqual(local.isoformat(), "2026-10-11T19:50:00+08:00")
        deadline = rules.promotion_deadline("2026-03-08T03:05:00-04:00", 0, "America/New_York")
        self.assertEqual(deadline.isoformat(), "2026-03-08T01:55:00-05:00")

    def test_invalid_expiry_or_settings_do_not_create_deadline(self):
        for expiry in (None, "", "invalid", "0000-00-00 00:00:00", 12345):
            self.assertIsNone(rules.promotion_deadline(expiry, 0, "UTC"))
        self.assertIsNone(rules.promotion_deadline("2026-10-11 20:00:00", float("nan"), "UTC"))
        self.assertIsNone(rules.promotion_deadline("2026-10-11 20:00:00", 0, "Invalid/Zone"))
        self.assertIsNone(rules.promotion_deadline("2026-10-11 20:00:00", 0, "UTC", -1))


class DownloadStartedTests(unittest.TestCase):
    def test_paused_or_queued_without_evidence_has_not_started(self):
        for state in ("queuedDL", "download_pending", "pausedDL", "stopped", "stalledDL", "checkingDL", ""):
            with self.subTest(state=state):
                self.assertFalse(rules.download_has_started({
                    "state": state, "downloaded": 0, "transfer_downloaded": 0,
                    "completed": False, "is_downloading": False,
                    "added_on": 123456, "time_active": 999, "last_activity": 123456,
                }, {}))

    def test_started_torrent_remains_protected_after_pause_or_recheck(self):
        for evidence in ({"downloaded": 1}, {"transfer_downloaded": 1}, {"completed": True}):
            self.assertTrue(rules.download_has_started({"state": "pausedDL", **evidence}, {}))
        self.assertTrue(rules.download_has_started({"state": "checkingDL"}, {"download_started": True}))

    def test_current_downloading_counts_as_started(self):
        self.assertTrue(rules.download_has_started({"is_downloading": True}, {}))
        for state in ("downloading", "forcedDL", "forced_download"):
            self.assertTrue(rules.download_has_started({"state": state}, {}))

    def test_invalid_evidence_does_not_imply_started(self):
        for value in (None, -1, float("nan"), float("inf"), True, "invalid"):
            self.assertFalse(rules.download_has_started({"downloaded": value, "transfer_downloaded": value}, {}))
        self.assertFalse(rules.download_has_started({"completed": "false"}, {"download_started": "false"}))


if __name__ == "__main__":
    unittest.main()
