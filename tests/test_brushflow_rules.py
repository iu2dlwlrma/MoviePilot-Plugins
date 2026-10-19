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


class AccountTrafficTests(unittest.TestCase):
    base_url = "https://example.invalid/index.php"
    auth = '<a href="logout.php">退出</a><a href="userdetails.php?id=123">用户</a>'

    def stat(self, color, label, value):
        return (f'<span class="info-stat-copy"><span class="info-stat-label color_{color}">{label}</span>'
                f' \n <span class="info-stat-value">{value}</span></span>')

    def page(self, upload='12.34<span class="info-stat-unit">GB</span>',
             download='0.00<span class="info-stat-unit">KB</span>', ratio="无限"):
        return (self.auth + self.stat("uploaded", "上传量", upload)
                + self.stat("downloaded", "下载量", download) + self.stat("ratio", "分享率", ratio))

    def parse(self, html):
        return rules.parse_account_traffic(html, 123, self.base_url)

    def test_nested_units_and_explicit_unlimited_ratio(self):
        self.assertEqual(self.parse(self.page()), {
            "upload": int(12.34 * 1024 ** 3), "download": 0, "ratio": None, "err_msg": None,
        })
        self.assertEqual(self.parse(self.page(ratio="∞"))["ratio"], None)

    def test_finite_and_zero_traffic_values(self):
        self.assertEqual(self.parse(self.page("0 B", "2 MB", "0")), {
            "upload": 0, "download": 2 * 1024 ** 2, "ratio": 0, "err_msg": None,
        })
        self.assertEqual(self.parse(self.page("2 TB", "1 GB", "2048"))["upload"], 2 * 1024 ** 4)
        self.assertEqual(self.parse(self.page("0 B", "0 B", "0"))["upload"], 0)
        self.assertIsNone(self.parse(self.page("1 GB", "0.00 KB", "10")))

    def test_unrelated_account_stats_do_not_replace_required_traffic(self):
        html = self.page("1.5 GiB", "3 GB", "0.500") + self.stat("bonus", "魔力值", "100.00")
        self.assertEqual(self.parse(html)["ratio"], .5)
        self.assertEqual(self.parse(html)["upload"], int(1.5 * 1024 ** 3))
        self.assertIsNone(self.parse(self.page().replace('GB</span></span>',
                          'GB</span></span><span class="info-stat-value">1 GB</span>', 1)))

    def test_authentication_requires_current_user_same_origin_and_no_password(self):
        for auth in ("", '<a href="logout.php">退出</a>',
                     self.auth.replace("123", "124"),
                     self.auth.replace("logout.php", "https://other.invalid/logout.php"),
                     self.auth.replace("userdetails.php", "//other.invalid/userdetails.php"),
                     self.auth + '<input type="PASSWORD">'):
            with self.subTest(auth=auth):
                self.assertIsNone(self.parse(self.page().replace(self.auth, auth)))
        authenticated = self.auth.replace('href="logout.php"', 'href="https://example.invalid/logout.php"')
        self.assertIsNotNone(self.parse(self.page().replace(self.auth, authenticated)))

    def test_rejects_missing_duplicate_and_nonadjacent_stat_fields(self):
        ratio = self.stat("ratio", "分享率", "无限")
        upload = self.stat("uploaded", "上传量", '12.34<span class="info-stat-unit">GB</span>')
        pages = [self.page().replace(ratio, ""), self.page() + upload,
                 self.page().replace("上传量</span>", "上传量</span><i>其他值</i>"),
                 self.page().replace("上传量</span>", "上传量</span>其他值"),
                 self.page().replace("上传量</span>", "上传量</span></span><span>"),
                 self.page().replace("上传量</span>", "昨日上传量</span>"),
                 self.page().replace('color_uploaded', 'color_downloaded')]
        for html in pages:
            with self.subTest(html=html):
                self.assertIsNone(self.parse(html))

    def test_rejects_invalid_numbers_units_and_unlimited_combinations(self):
        for value in ("-1 GB", "nan GB", "inf GB", "1 XB", "GB", "1.2.3 GB", "1e1000 GB"):
            with self.subTest(value=value):
                self.assertIsNone(self.parse(self.page(value)))
        for ratio in ("-1", "nan", "inf", "未知", "1.2.3"):
            with self.subTest(ratio=ratio):
                self.assertIsNone(self.parse(self.page("1 GB", "1 GB", ratio)))
        self.assertIsNone(self.parse(self.page("0 B", "0 B", "无限")))
        self.assertIsNone(self.parse(self.page("1 GB", "1 GB", "无限")))
        self.assertIsNone(self.parse("challenge: 12.34 GB"))


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


class SlowDownloadTests(unittest.TestCase):
    def sample(self, record, at, downloaded, uploaded=0, minimum=100 * 1024, **fields):
        info = {"sample_downloaded": downloaded, "sample_uploaded": uploaded,
                "transfer_downloaded": downloaded, "uploaded": uploaded,
                "is_downloading": True, "completed": False, "state": "downloading",
                "dlspeed": 0, "upspeed": 0, **fields}
        rules.update_transfer_sample(record, info, at, minimum_download_speed=minimum)
        return info

    def reason(self, info, record, now, minimum=100, minutes=30):
        return rules.slow_download_reason(info, record, now, minimum, minutes)

    def test_first_sample_is_only_a_baseline_and_thirty_minutes_is_inclusive(self):
        record = {"actual_download_seconds": 86400, "download_started": True}
        base = 50 * 1024 ** 3
        for at in range(0, 1801, 30):
            info = self.sample(record, at, base + at * 10 * 1024)
            if at < 1800:
                self.assertIsNone(self.reason(info, record, at))
        self.assertIsInstance(self.reason(info, record, 1800), str)
        self.assertTrue(self.reason(info, record, 1800))

    def test_downloading_without_peers_or_bytes_counts_and_180_second_intervals_are_valid(self):
        record = {}
        for at in range(0, 1801, 180):
            info = self.sample(record, at, 0, peers_connected=0)
        self.assertTrue(self.reason(info, record, 1800))

    def test_pause_queue_check_and_completion_reset_the_observation(self):
        for state, completed in (("pauseddl", False), ("download pending", False),
                                 ("checking", False), ("seeding", True)):
            with self.subTest(state=state):
                record = {}
                for at in range(0, 1801, 30):
                    info = self.sample(record, at, at * 1024)
                self.assertTrue(self.reason(info, record, 1800))
                info = self.sample(record, 1830, 1800 * 1024, is_downloading=False,
                                   state=state, completed=completed)
                self.assertIsNone(self.reason(info, record, 1830))
                for at in range(1860, 3661, 30):
                    info = self.sample(record, at, 1800 * 1024 + (at - 1860) * 1024)
                    if at < 3660:
                        self.assertIsNone(self.reason(info, record, at))
                self.assertTrue(self.reason(info, record, 3660))

    def test_non_download_state_overrides_stale_rate_and_download_flag(self):
        for state in ("pauseddl", "download pending", "checkingdl", "unknown"):
            with self.subTest(state=state):
                record = {}
                for at in range(0, 1801, 30):
                    info = self.sample(record, at, at * 1024)
                self.assertTrue(self.reason(info, record, 1800))
                info = self.sample(record, 1830, 1800 * 1024,
                                   state=state, is_downloading=True, dlspeed=1)
                self.assertIsNone(self.reason(info, record, 1830))

    def test_unknown_reset_and_sampling_gap_require_a_fresh_complete_observation(self):
        for failure in ("unknown_download", "unknown_upload", "download_reset", "upload_reset", "gap"):
            with self.subTest(failure=failure):
                record = {}
                for at in range(0, 1771, 30):
                    info = self.sample(record, at, 10000000 + at * 1024, uploaded=1000)
                failed_at = 1980 if failure == "gap" else 1800
                downloaded = None if failure == "unknown_download" else 0 if failure == "download_reset" else 12000000
                uploaded = None if failure == "unknown_upload" else 0 if failure == "upload_reset" else 1000
                info = self.sample(record, failed_at, downloaded, uploaded)
                self.assertIsNone(self.reason(info, record, failed_at))
                start = failed_at + 30
                for at in range(start, start + 1801, 30):
                    info = self.sample(record, at, 20000000 + (at - start) * 1024, uploaded=2000)
                    if at < start + 1800:
                        self.assertIsNone(self.reason(info, record, at))
                self.assertTrue(self.reason(info, record, start + 1800))

    def test_current_speed_recovery_protects_and_restarts_the_observation(self):
        for field in ("dlspeed", "upspeed"):
            with self.subTest(field=field):
                record = {}
                for at in range(0, 1801, 30):
                    info = self.sample(record, at, at * 1024)
                self.assertTrue(self.reason(info, record, 1800))
                protected = {**info, field: 100 * 1024}
                self.assertIsNone(self.reason(protected, record, 1800))
                info = self.sample(record, 1830, 1830 * 1024, **{field: 100 * 1024})
                self.assertIsNone(self.reason(info, record, 1830))
                info = self.sample(record, 1860, 1860 * 1024)
                self.assertIsNone(self.reason(info, record, 1860))

    def test_average_download_or_upload_recovery_resets_even_when_current_speed_is_zero(self):
        for field in ("download", "upload"):
            with self.subTest(field=field):
                record = {}
                for at in range(0, 1771, 30):
                    info = self.sample(record, at, at * 1024)
                recovered_bytes = 1800 * 100 * 1024
                info = self.sample(record, 1800,
                                   recovered_bytes if field == "download" else 1800 * 1024,
                                   uploaded=recovered_bytes if field == "upload" else 0)
                self.assertIsNone(self.reason(info, record, 1800))
                info = self.sample(record, 1830,
                                   recovered_bytes + 30 * 1024 if field == "download" else 1830 * 1024,
                                   uploaded=recovered_bytes if field == "upload" else 0)
                self.assertIsNone(self.reason(info, record, 1830))

    def test_tiny_uploads_do_not_exempt_a_persistently_slow_download(self):
        record = {}
        for at in range(0, 1801, 30):
            info = self.sample(record, at, at * 10 * 1024, uploaded=at, upspeed=1)
        self.assertTrue(self.reason(info, record, 1800))

    def test_old_fast_download_does_not_hide_a_later_thirty_minute_slow_period(self):
        record = {}
        fast_bytes = 1800 * 200 * 1024
        for at in range(0, 1801, 30):
            info = self.sample(record, at, at * 200 * 1024)
        self.assertIsNone(self.reason(info, record, 1800))
        for at in range(1830, 3601, 30):
            info = self.sample(record, at, fast_bytes + (at - 1800) * 10 * 1024)
            if at < 3600:
                self.assertIsNone(self.reason(info, record, at))
        self.assertTrue(self.reason(info, record, 3600))

    def test_ninety_minute_observation_does_not_expand_transfer_history(self):
        record = {}
        for at in range(0, 5401, 30):
            info = self.sample(record, at, at * 1024)
            if at < 5400:
                self.assertIsNone(self.reason(info, record, at, minutes=90))
        self.assertTrue(self.reason(info, record, 5400, minutes=90))
        self.assertLessEqual(len(record.get("transfer_samples", [])), 36)

    def test_json_restart_keeps_valid_observation_but_stale_data_cannot_delete(self):
        record = {}
        for at in range(0, 901, 30):
            info = self.sample(record, at, at * 1024)
        restored = json.loads(json.dumps(record))
        for at in range(930, 1801, 30):
            info = self.sample(restored, at, at * 1024)
        self.assertTrue(self.reason(info, restored, 1800))
        self.assertIsNone(self.reason(info, restored, 1981))
        info = self.sample(restored, 1981, 1981 * 1024)
        self.assertIsNone(self.reason(info, restored, 1981))

    def test_changed_threshold_does_not_reuse_the_previous_slow_period(self):
        record = {}
        for at in range(0, 1801, 30):
            info = self.sample(record, at, at * 1024)
        self.assertTrue(self.reason(info, record, 1800))
        self.assertIsNone(self.reason(info, record, 1800, minimum=200))
        for at in range(1830, 3631, 30):
            info = self.sample(record, at, at * 1024, minimum=200 * 1024)
            if at < 3630:
                self.assertIsNone(self.reason(info, record, at, minimum=200))
        self.assertTrue(self.reason(info, record, 3630, minimum=200))

    def test_disabling_then_enabling_starts_a_new_observation(self):
        for disabled in (None, 0):
            with self.subTest(disabled=disabled):
                record = {}
                for at in range(0, 1801, 30):
                    info = self.sample(record, at, at * 1024)
                self.assertTrue(self.reason(info, record, 1800))
                self.assertIsNone(self.reason(info, record, 1800, minutes=None))
                self.assertIsNone(self.reason(info, record, 1800, minutes=0))
                info = self.sample(record, 1830, 1830 * 1024, minimum=disabled)
                self.assertIsNone(self.reason(info, record, 1830, minimum=disabled))
                self.assertIsNone(self.reason(info, record, 1830))
                for at in range(1860, 3661, 30):
                    info = self.sample(record, at, at * 1024)
                    if at < 3660:
                        self.assertIsNone(self.reason(info, record, at))
                self.assertTrue(self.reason(info, record, 3660))


if __name__ == "__main__":
    unittest.main()
