import importlib.util
import json
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


class DownloadAllocationTests(unittest.TestCase):
    def row(self, torrent_hash, site_id, priority, active=False, position=0):
        return {"hash": torrent_hash, "site_id": site_id, "priority": priority,
                "active": active, "position": position}

    def test_each_site_gets_a_chance_before_priority_consumes_remaining_slots(self):
        rows = [self.row(f"exam-{i}", 1, (0, 0), position=i) for i in range(5)]
        rows += [self.row("nova", 2, (1, -.8), position=10),
                 self.row("normal", 3, (2, 0), position=20)]
        self.assertEqual(rules.allocate_downloads(rows, 4), ["exam-0", "nova", "normal", "exam-1"])
        self.assertEqual(rules.allocate_downloads(rows, 2), ["exam-0", "nova"])

    def test_one_site_can_use_all_available_slots(self):
        rows = [self.row(f"only-{i}", 1, (2, 0), position=i) for i in range(5)]
        self.assertEqual(rules.allocate_downloads(rows, 4), ["only-0", "only-1", "only-2", "only-3"])
        self.assertEqual(rules.allocate_downloads(rows, 8), [row["hash"] for row in rows])

    def test_existing_active_tasks_remain_selected_within_the_same_site(self):
        rows = [self.row("queued-first", 1, (1, -.5), position=0),
                self.row("active-first", 1, (1, -.5), True, 10),
                self.row("active-second", 1, (1, -.5), True, 11),
                self.row("other-site", 2, (2, 0), position=20)]
        self.assertEqual(rules.allocate_downloads(rows, 3), ["active-first", "other-site", "active-second"])
        self.assertFalse(rows[0]["active"])

    def test_empty_or_zero_capacity_and_duplicate_hashes(self):
        row = self.row("one", 1, (0, 0))
        self.assertEqual(rules.allocate_downloads([], 5), [])
        for capacity in (0, -1, None):
            self.assertEqual(rules.allocate_downloads([row], capacity), [])
        self.assertEqual(rules.allocate_downloads([row, row], 2), ["one"])


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


class TransferSampleTests(unittest.TestCase):
    def sample(self, record, at, uploaded, downloaded, downloading=True, **fields):
        rules.update_transfer_sample(record, {
            "uploaded": uploaded, "transfer_downloaded": downloaded,
            "is_downloading": downloading, "completed": False, **fields,
        }, at)

    def test_first_existing_counters_are_only_a_baseline(self):
        record = {}
        self.assertIsNone(self.sample(record, 100, 5_000_000, 8_000_000))
        window = rules.transfer_window(record, 100)
        self.assertIsNone(window["uploaded"])
        self.assertIsNone(window["downloaded"])
        self.assertIsNone(window["avg_upspeed"])
        self.assertEqual(window["seconds"], 0)
        self.assertEqual(record["actual_download_seconds"], 0)
        self.sample(record, 130, 5_000_600, 8_001_200)
        self.assertEqual(rules.transfer_window(record, 130), {
            "uploaded": 600, "downloaded": 1200, "seconds": 30,
            "gap": False, "avg_upspeed": 20, "active_download_seconds": 30,
        })

    def test_queue_and_pause_time_do_not_count_as_downloading(self):
        record = {}
        for at, downloading in ((0, False), (30, False), (60, True),
                                (90, True), (120, False), (150, False),
                                (180, True), (210, True)):
            self.sample(record, at, at, at * 2, downloading)
        self.assertEqual(record["actual_download_seconds"], 60)
        window = rules.transfer_window(record, 210)
        self.assertEqual(window["seconds"], 210)
        self.assertEqual(window["active_download_seconds"], 60)
        self.assertEqual(window["uploaded"], 210)

    def test_completion_ends_download_timer(self):
        record = {}
        self.sample(record, 0, 0, 0)
        self.sample(record, 30, 10, 100)
        self.sample(record, 60, 20, 200, completed=True)
        self.sample(record, 90, 30, 200, completed=True)
        self.assertEqual(record["actual_download_seconds"], 30)

    def test_gap_preserves_real_byte_difference_but_not_active_time(self):
        record = {}
        self.sample(record, 0, 100, 200)
        self.sample(record, 30, 200, 400)
        self.sample(record, 630, 2200, 4400)
        window = rules.transfer_window(record, 630)
        self.assertTrue(window["gap"])
        self.assertEqual(window["seconds"], 630)
        self.assertEqual(window["uploaded"], 2100)
        self.assertEqual(window["downloaded"], 4200)
        self.assertEqual(window["active_download_seconds"], 30)

    def test_counter_reset_marks_only_affected_measurement_unknown(self):
        record = {}
        self.sample(record, 0, 1000, 2000)
        self.sample(record, 30, 1100, 2100)
        self.sample(record, 60, 0, 2200)
        self.sample(record, 90, 50, 2300)
        window = rules.transfer_window(record, 90)
        self.assertIsNone(window["uploaded"])
        self.assertIsNone(window["avg_upspeed"])
        self.assertEqual(window["downloaded"], 300)
        self.assertTrue(window["gap"])
        recovered = rules.transfer_window(record, 90, seconds=30)
        self.assertEqual(recovered["uploaded"], 50)
        self.assertFalse(recovered["gap"])

    def test_unknown_counters_and_speeds_do_not_become_zero_uploads(self):
        for unknown in (None, "", -1, float("nan"), float("inf"), True):
            with self.subTest(unknown=unknown):
                record = {}
                self.sample(record, 0, 100, 200)
                self.sample(record, 30, unknown, unknown, upspeed=1000, dlspeed=2000)
                self.sample(record, 60, 300, 600)
                window = rules.transfer_window(record, 60)
                self.assertIsNone(window["uploaded"])
                self.assertIsNone(window["downloaded"])
                self.assertIsNone(window["avg_upspeed"])
                self.assertTrue(window["gap"])

    def test_observed_zero_is_distinct_from_missing_data(self):
        record = {}
        self.sample(record, 0, 100, 200, False)
        self.sample(record, 30, 100, 200, False)
        window = rules.transfer_window(record, 30)
        self.assertEqual(window["uploaded"], 0)
        self.assertEqual(window["downloaded"], 0)
        self.assertEqual(window["avg_upspeed"], 0)
        self.assertFalse(window["gap"])

    def test_nullable_raw_sample_takes_precedence_over_legacy_zero(self):
        record = {}
        self.sample(record, 0, 0, 0, sample_uploaded=None, sample_downloaded=None)
        self.sample(record, 30, 0, 0, sample_uploaded=None, sample_downloaded=None)
        window = rules.transfer_window(record, 30)
        self.assertIsNone(window["uploaded"])
        self.assertIsNone(window["downloaded"])
        self.assertTrue(window["gap"])

    def test_minute_history_is_bounded_and_window_uses_actual_duration(self):
        record = {}
        for at in range(0, 7201, 30):
            self.sample(record, at, at * 100, at * 200)
        self.assertLessEqual(len(record["transfer_samples"]), 36)
        self.assertGreaterEqual(record["transfer_samples"][0]["at"], 5100)
        window = rules.transfer_window(record, 7200)
        self.assertEqual(window["seconds"], 1800)
        self.assertEqual(window["uploaded"], 180_000)
        self.assertEqual(window["active_download_seconds"], 1800)
        self.assertEqual(record["actual_download_seconds"], 7200)
        self.sample(record, 7230, 723_000, 1_446_000)
        window = rules.transfer_window(record, 7230)
        self.assertEqual(window["seconds"], 1770)
        self.assertEqual(window["uploaded"], 177_000)

    def test_json_restart_retains_measurements_and_deletion_settlement(self):
        record = {}
        self.sample(record, 0, 1000, 2000)
        self.sample(record, 30, 1100, 2200)
        restored = json.loads(json.dumps(record))
        self.sample(restored, 60, 1300, 2600)
        restored["deleted"] = True
        restored = json.loads(json.dumps(restored))
        window = rules.transfer_window(restored, 60)
        self.assertEqual(window["uploaded"], 300)
        self.assertEqual(window["downloaded"], 600)
        self.assertEqual(restored["actual_download_seconds"], 60)
        self.assertTrue(restored["deleted"])

    def test_duplicate_and_backward_timestamps_do_not_replace_baseline(self):
        record = {}
        self.sample(record, 100, 100, 200)
        self.sample(record, 100, 1000, 2000)
        self.sample(record, 90, 1000, 2000)
        self.sample(record, 130, 200, 400)
        window = rules.transfer_window(record, 130)
        self.assertEqual(window["uploaded"], 100)
        self.assertEqual(window["seconds"], 30)

    def test_stale_sample_is_marked_without_filling_unobserved_time(self):
        record = {}
        self.sample(record, 0, 0, 0)
        self.sample(record, 30, 300, 600)
        window = rules.transfer_window(record, 300)
        self.assertEqual(window["seconds"], 30)
        self.assertEqual(window["uploaded"], 300)
        self.assertTrue(window["gap"])
        window = rules.transfer_window(record, 3000)
        self.assertEqual(window["seconds"], 0)
        self.assertIsNone(window["uploaded"])
        self.assertTrue(window["gap"])


if __name__ == "__main__":
    unittest.main()
