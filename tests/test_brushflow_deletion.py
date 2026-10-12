import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

from test_brushflow import load_plugin
from test_brushflow_rules import rules


class BrushFlowDeletionTests(unittest.TestCase):
    def setUp(self):
        self.ns = load_plugin()
        self.now = datetime(2026, 10, 11, 12, tzinfo=timezone.utc)
        owner = self

        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return owner.now.astimezone(tz) if tz else owner.now.replace(tzinfo=None)

        self.ns.update({
            "download_has_started": rules.download_has_started,
            "promotion_deadline": rules.promotion_deadline,
            "datetime": Clock,
            "ZoneInfo": lambda name: timezone.utc,
            "settings": SimpleNamespace(TZ="UTC"),
            "time": SimpleNamespace(time=lambda: self.now.timestamp(), strftime=time.strftime, localtime=time.localtime),
        })
        self.task = SimpleNamespace(
            id="1", name="site", downloader="qb", enabled=True, notify=False,
            del_no_free=True, promotion_lead_minutes=10, timezone_offset=0,
            hr_seed_time=None, seed_ratio=None, seed_time=None, seed_size=None,
            download_time=1, seed_avgspeed=None, seed_inactivetime=None,
            proxy_delete=True, delete_except_tags="keep",
        )
        self.plugin = self.ns["BrushFlow"]()
        self.plugin._get_task_config = lambda task_id=None: self.task
        self.plugin._task_configs = {self.task.id: self.task}
        self.plugin._task_context = threading.local()
        self.records = {}
        self.rows = []
        self.kind = "qbittorrent"
        self.service = SimpleNamespace(name="qb", instance=SimpleNamespace(
            is_inactive=lambda: False, get_torrents=lambda: (self.rows, False),
        ))
        self.ns["DownloaderHelper"] = lambda: SimpleNamespace(
            get_service=lambda name: self.service,
            is_downloader=lambda kind, service: kind == self.kind,
        )
        self.plugin._get_task_data = lambda task_id, name, default=None: self.records
        self.plugin._save_task_data = Mock()
        self.plugin._BrushFlow__send_delete_message = Mock()
        self.plugin._BrushFlow__update_undeleted_torrents_missing_in_downloader = Mock()

    def torrent(self, torrent_hash="hash", **values):
        return {
            "hash": torrent_hash, "name": torrent_hash, "state": "queuedDL",
            "total_size": 1000, "size": 1000, "completed": 0, "progress": 0,
            "downloaded": 0, "uploaded": 0, "ratio": 0, "tags": "managed",
            "added_on": self.now.timestamp() - 86400, "last_activity": self.now.timestamp() - 86400,
            **values,
        }

    def info(self, torrent):
        return self.plugin._BrushFlow__get_torrent_info(torrent)

    def evaluate(self, torrent, record):
        return self.plugin._BrushFlow__evaluate_conditions_for_delete(self.info(torrent), record)

    def expiry(self, minutes):
        return (self.now + timedelta(minutes=minutes)).isoformat()

    def test_queued_torrent_ignores_age_and_ordinary_delete_conditions(self):
        self.task.seed_inactivetime = 1
        self.task.seed_ratio = 1
        queued = self.torrent(ratio=100)
        self.assertFalse(self.evaluate(queued, {"freedate": self.expiry(60)})[0])
        self.assertFalse(self.evaluate(queued, {"download_started_at": self.now.timestamp() - 7200})[0])

    def test_promotion_window_deletes_unstarted_queue_including_unknown_size(self):
        queued = self.torrent()
        self.assertFalse(self.evaluate(queued, {"freedate": self.expiry(10.1)})[0])
        self.assertTrue(self.evaluate(queued, {"freedate": self.expiry(10)})[0])
        self.assertTrue(self.evaluate(self.torrent(size=0, total_size=0), {"freedate": self.expiry(5)})[0])
        self.task.promotion_lead_minutes = 30
        self.assertTrue(self.evaluate(queued, {"freedate": self.expiry(20)})[0])

    def test_cumulative_transfer_is_not_completion(self):
        unfinished = self.torrent(downloaded=2000, completed=100, progress=0.1, state="pausedDL")
        self.assertTrue(self.evaluate(unfinished, {"freedate": self.expiry(5)})[0])
        complete = self.torrent(downloaded=0, completed=1000, progress=1, state="stalledUP")
        self.assertFalse(self.evaluate(complete, {"freedate": self.expiry(5)})[0])

    def test_hr_and_excluded_tags_stay_protected(self):
        self.assertFalse(self.evaluate(self.torrent(), {"hit_and_run": True, "freedate": self.expiry(5)})[0])
        self.task.seed_inactivetime = 1
        self.assertFalse(self.evaluate(self.torrent(completed=100, progress=0.1), {
            "hit_and_run": True, "download_started": True, "freedate": self.expiry(5),
            "download_started_at": self.now.timestamp() - 7200,
        })[0])
        tagged = self.torrent(tags="managed, keep")
        filtered = self.plugin._BrushFlow__filter_torrents_by_tag([tagged], self.task.delete_except_tags)
        deleted = self.plugin._BrushFlow__delete_torrent_for_evaluate_conditions(
            filtered, {"hash": {"freedate": self.expiry(5)}},
        )
        self.assertEqual(deleted, [])

    def test_download_timeout_starts_at_first_confirmed_download_and_persists(self):
        record = {"size": 1000}
        self.records["hash"] = record
        self.plugin._BrushFlow__update_torrent_tasks_state([self.torrent()], self.records)
        self.assertNotIn("download_started_at", record)
        downloading = self.torrent(state="downloading", completed=10, downloaded=10, progress=0.01)
        self.plugin._BrushFlow__update_torrent_tasks_state([downloading], self.records)
        started_at = self.now.timestamp()
        self.assertEqual(record["download_started_at"], started_at)
        self.assertTrue(record["download_started"])
        self.assertFalse(self.evaluate(downloading, record)[0])
        self.now += timedelta(hours=1, seconds=1)
        paused = self.torrent(state="pausedDL")
        self.plugin._BrushFlow__update_torrent_tasks_state([paused], self.records)
        self.assertEqual(record["download_started_at"], started_at)
        self.assertFalse(self.evaluate(paused, record)[0])
        record["actual_download_seconds"] = 3601
        self.assertTrue(self.evaluate(downloading, record)[0])

    def test_transmission_missing_optional_properties_preserves_transfer_totals(self):
        class Torrent(SimpleNamespace):
            @property
            def start_date(self):
                raise KeyError("startDate")

            @property
            def size_when_done(self):
                raise KeyError("sizeWhenDone")

        self.kind = "transmission"
        torrent = Torrent(
            hashString="tr", name="tr", status="download_pending", total_size=1000,
            percent_done=0, downloaded_ever=1500, uploaded_ever=4321, ratio=8,
            labels=[], added_date=self.now - timedelta(days=1),
        )
        info = self.info(torrent)
        self.assertEqual(info["transfer_downloaded"], 1500)
        self.assertEqual(info["downloaded"], 0)
        self.assertEqual(info["uploaded"], 4321)
        self.assertFalse(info["completed"])
        self.assertTrue(rules.download_has_started(info, {}))

    def test_no_progress_requires_observed_running_time_and_protects_hr(self):
        for downloading, received, protected, expected in [
            (True, 0, False, True), (False, 0, False, False),
            (True, 1, False, False), (True, 0, True, False),
        ]:
            with self.subTest(downloading=downloading, received=received, protected=protected):
                record = {"download_started": True, "hit_and_run": protected}
                end = self.now.timestamp()
                for seconds in range(0, 1201, 30):
                    rules.update_transfer_sample(record, {"uploaded": 0, "transfer_downloaded": received * seconds,
                        "is_downloading": downloading, "completed": False}, end - 1200 + seconds)
                torrent = self.torrent(state="downloading" if downloading else "queuedDL")
                self.assertEqual(self.evaluate(torrent, record)[0], expected)

    def test_promotion_scheduler_uses_lead_and_does_not_repeat_expired_item(self):
        self.records.update({
            "due": {"freedate": self.expiry(5), "size": 1000, "downloaded": 0},
            "future": {"freedate": self.expiry(20), "size": 1000, "downloaded": 0},
            "complete": {"freedate": self.expiry(15), "completed": True},
        })
        self.assertEqual(self.plugin._next_promotion_expiry(self.task), self.now + timedelta(minutes=10))
        self.records.pop("future")
        self.assertIsNone(self.plugin._next_promotion_expiry(self.task))

    def test_local_dynamic_preconditions_protect_queue_but_clear_promotion(self):
        torrents = [self.torrent("waiting"), self.torrent("due"), self.torrent("started", completed=10, progress=0.01)]
        records = {
            "waiting": {"size": 1000},
            "due": {"size": 1000, "freedate": self.expiry(5)},
            "started": {"size": 1000, "download_started": True, "download_started_at": self.now.timestamp() - 7200},
        }
        deleted = self.plugin._BrushFlow__delete_torrent_for_evaluate_proxy_pre_conditions(torrents, records)
        self.assertCountEqual(deleted, ["due", "started"])

    def test_global_dynamic_plan_preserves_waiting_and_tagged_torrents(self):
        self.task.seed_inactivetime = 1
        self.rows = [self.torrent("waiting"), self.torrent("due"), self.torrent("protected", tags="keep")]
        self.records.update({
            "waiting": {"size": 1000},
            "due": {"size": 1000, "freedate": self.expiry(5)},
            "protected": {"size": 1000, "freedate": self.expiry(5)},
        })
        candidates, total_size, _, _ = self.plugin._collect_global_dynamic_delete_candidates()
        self.assertEqual(total_size, 3000)
        selected, _, _ = self.plugin._select_global_dynamic_deletions(candidates, total_size, 0, 1)
        self.assertEqual([item["torrent_hash"] for item in selected], ["due"])

    def test_pending_hash_resolution_keeps_promotion_metadata_in_global_plan(self):
        self.rows = [self.torrent("resolved", tags="managed, ticket")]
        self.records["pending:ticket"] = {"size": 1000, "freedate": self.expiry(5), "title": "original"}
        candidates, total_size, _, _ = self.plugin._collect_global_dynamic_delete_candidates()
        self.assertNotIn("pending:ticket", self.records)
        self.assertEqual(self.records["resolved"]["title"], "original")
        selected, _, _ = self.plugin._select_global_dynamic_deletions(candidates, total_size, 0, 5000)
        self.assertEqual([item["torrent_hash"] for item in selected], ["resolved"])

    def test_dynamic_fallback_never_expands_empty_or_protected_candidates(self):
        self.task.delete_size_range = "1"
        self.records = {"protected": {"size": 2 * 1024 ** 3, "hit_and_run": False}}
        self.service.instance.get_completed_torrents = Mock(return_value=[self.torrent("unmanaged", progress=1)])
        delete = self.plugin._BrushFlow__delete_torrent_for_proxy
        self.assertEqual(delete([], self.records), [])
        self.service.instance.get_completed_torrents.assert_not_called()
        queued = self.torrent("queued")
        self.records["queued"] = {"size": 1000}
        self.assertEqual(delete([queued], self.records), [])
        completed = self.torrent("managed", progress=1, completed=1000, state="stalledUP")
        self.records["managed"] = {"size": 1000}
        self.assertEqual(delete([completed], self.records), ["managed"])


if __name__ == "__main__":
    unittest.main()
