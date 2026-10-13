import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from contextlib import nullcontext
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
        self.plugin._task_locks = {}
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

    def test_no_progress_waits_for_configured_inactivity_and_protects_hr(self):
        self.task.download_time = None
        self.task.seed_inactivetime = 120
        for inactive_minutes, protected, expected in [(20, False, False), (119, False, False),
                                                       (120, False, True), (120, True, False)]:
            with self.subTest(inactive_minutes=inactive_minutes, protected=protected):
                record = {"download_started": True, "hit_and_run": protected}
                end = self.now.timestamp()
                for seconds in range(0, 1201, 30):
                    rules.update_transfer_sample(record, {"uploaded": 0, "transfer_downloaded": 0,
                        "is_downloading": True, "completed": False}, end - 1200 + seconds)
                torrent = self.torrent(state="downloading", last_activity=end - inactive_minutes * 60)
                deleted, reason = self.evaluate(torrent, record)
                self.assertEqual(deleted, expected)
                if expected:
                    self.assertEqual(reason, "未活动时间达到 120 分钟")

    def test_disabled_download_timeout_preserves_active_long_running_and_unstarted_torrents(self):
        self.task.download_time = None
        self.task.seed_inactivetime = 120
        record = {"download_started": True, "actual_download_seconds": 6 * 3600}
        active = self.torrent(state="downloading", downloaded=100, last_activity=self.now.timestamp() - 60)
        self.assertFalse(self.evaluate(active, record)[0])
        self.assertFalse(self.evaluate(self.torrent(), {})[0])

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


class BrushFlowFileDeletionTests(unittest.TestCase):
    setUp = BrushFlowDeletionTests.setUp
    torrent = BrushFlowDeletionTests.torrent
    info = BrushFlowDeletionTests.info
    evaluate = BrushFlowDeletionTests.evaluate

    def qb_client(self, rows):
        self.rows = rows
        self.service.instance.qbc = SimpleNamespace(
            torrents_info=Mock(return_value=rows),
            app_preferences=Mock(return_value={"temp_path_enabled": False}),
            torrents_files=Mock(return_value=[]),
            torrents_reannounce=Mock(),
        )
        self.service.instance.delete_torrents = Mock(return_value=True)
        return self.service.instance.qbc

    def tr_client(self, rows, incomplete=False, directory="/incomplete"):
        self.kind = "transmission"
        self.service.instance.trc = SimpleNamespace(
            get_torrents=Mock(return_value=[SimpleNamespace(fields=row) for row in rows]),
            get_session=Mock(return_value=SimpleNamespace(
                incomplete_dir_enabled=incomplete, incomplete_dir=directory)),
        )
        return self.service.instance.trc

    @staticmethod
    def tr_row(torrent_hash, root="/data", names=("one.bin",), progress=1):
        return {"hashString": torrent_hash, "downloadDir": root, "percentDone": progress,
                "files": [{"name": name} for name in names] if names is not None else None}

    def test_shared_files_are_blocked_and_independent_files_remain_deletable(self):
        self.tr_client([self.tr_row("shared"), self.tr_row("foreign"),
                        self.tr_row("safe", names=("other.bin",))])
        self.assertEqual(self.plugin._safe_delete_hashes(self.service, ["shared", "safe"]), ["safe"])

    def test_blocked_candidate_becomes_retained_for_other_candidates(self):
        self.tr_client([self.tr_row("a", names=("x", "y")), self.tr_row("b", names=("y",)),
                        self.tr_row("foreign", names=("x",)), self.tr_row("safe", names=("z",))])
        self.assertEqual(self.plugin._safe_delete_hashes(self.service, ["a", "b", "safe"]), ["safe"])

    def test_whole_deletion_batch_can_share_its_own_files(self):
        self.tr_client([self.tr_row("a"), self.tr_row("b")])
        self.assertEqual(self.plugin._safe_delete_hashes(self.service, ["a", "b"]), ["a", "b"])

    def test_transmission_incomplete_directory_and_part_files_are_protected(self):
        self.tr_client([self.tr_row("candidate", "/data/a", progress=.5),
                        self.tr_row("foreign", "/incomplete", ("one.bin.part",))], incomplete=True)
        self.assertEqual(self.plugin._safe_delete_hashes(self.service, ["candidate"]), [])

    def test_missing_file_lists_and_read_failures_do_not_authorize_deletion(self):
        client = self.tr_client([self.tr_row("candidate", names=None)])
        self.assertEqual(self.plugin._safe_delete_hashes(self.service, ["candidate"]), [])
        client.get_torrents.side_effect = RuntimeError("unavailable")
        self.assertEqual(self.plugin._safe_delete_hashes(self.service, ["candidate"]), [])

    def test_unknown_retained_files_only_block_their_known_directory(self):
        self.tr_client([self.tr_row("candidate", "/separate"),
                        self.tr_row("foreign", "/data", names=None)])
        self.assertEqual(self.plugin._safe_delete_hashes(self.service, ["candidate"]), ["candidate"])
        self.tr_client([self.tr_row("candidate"), self.tr_row("foreign", root=None, names=None)])
        self.assertEqual(self.plugin._safe_delete_hashes(self.service, ["candidate"]), [])

    def test_qb_directory_boundaries_and_windows_case_are_compared_safely(self):
        for root, child, sibling in (("/data/one", "/data/one/file", "/data/one-other"),
                                     ("C:\\Data\\One", "c:\\data\\one\\file", "C:\\Data\\One-other")):
            with self.subTest(root=root):
                self.qb_client([self.torrent("shared", content_path=root, progress=1),
                                self.torrent("foreign", content_path=child, progress=1),
                                self.torrent("safe", content_path=sibling, progress=1)])
                self.assertEqual(self.plugin._safe_delete_hashes(self.service, ["shared", "safe"]), ["safe"])

    def test_qb_files_and_temporary_directory_are_checked(self):
        client = self.qb_client([self.torrent("candidate", save_path="/data", progress=.5),
                                 self.torrent("foreign", content_path="/temp/one.bin", progress=1)])
        client.torrents_files.return_value = [{"name": "one.bin"}]
        client.app_preferences.return_value = {"temp_path_enabled": True, "temp_path": "/temp"}
        self.assertEqual(self.plugin._safe_delete_hashes(self.service, ["candidate"]), [])

    def test_qb_incomplete_extension_is_protected(self):
        for root, retained in (("/data", "/data/one.bin.!qB"), ("C:\\Data", "c:\\data\\one.bin.!qB")):
            with self.subTest(root=root):
                client = self.qb_client([self.torrent("candidate", save_path=root, progress=.5),
                                         self.torrent("foreign", content_path=retained, progress=1)])
                client.torrents_files.return_value = [{"name": "one.bin"}]
                self.assertEqual(self.plugin._safe_delete_hashes(self.service, ["candidate"]), [])

    def test_qb_unknown_incomplete_files_protect_both_deletion_directions(self):
        for error in (False, True):
            for candidate_incomplete in (False, True):
                with self.subTest(error=error, candidate_incomplete=candidate_incomplete):
                    incomplete = "candidate" if candidate_incomplete else "foreign"
                    complete = "foreign" if candidate_incomplete else "candidate"
                    client = self.qb_client([
                        self.torrent(incomplete, content_path="/data/one.bin", progress=.5),
                        self.torrent(complete, content_path="/data/one.bin.!qB", progress=1),
                    ])
                    if error:
                        client.torrents_files.side_effect = RuntimeError("unavailable")
                    self.assertEqual(self.plugin._safe_delete_hashes(self.service, ["candidate"]), [])

    def test_qb_unknown_incomplete_files_protect_only_a_confirmed_directory(self):
        self.qb_client([
            self.torrent("foreign", content_path="/data/one.bin", save_path="/data", progress=.5),
            self.torrent("candidate", content_path="/separate/one.bin", progress=1),
        ])
        self.assertEqual(self.plugin._safe_delete_hashes(self.service, ["candidate"]), ["candidate"])

    def test_relative_and_escaping_paths_are_not_trusted(self):
        for root, names in (("relative", ("one.bin",)), ("/data", ("../escape",))):
            with self.subTest(root=root, names=names):
                self.tr_client([self.tr_row("candidate", root, names)])
                self.assertEqual(self.plugin._safe_delete_hashes(self.service, ["candidate"]), [])

    def prepare_check(self, result=True):
        self.task.proxy_delete = False
        self.task.seed_inactivetime = 1
        self.qb_client([self.torrent("shared", content_path="/data/one", progress=1),
                        self.torrent("foreign", content_path="/data/one", progress=1),
                        self.torrent("safe", content_path="/data/two", progress=1)])
        self.records.update({key: {"size": 1000, "download_started": True, "deleted": False}
                             for key in ("shared", "safe")})
        self.service.instance.delete_torrents.return_value = result
        self.plugin._validate_task_reference = lambda task: True
        self.plugin._current_task_data = lambda name, default: self.records if name == "torrents" else {}
        self.plugin._resolve_pending_downloads = Mock()
        self.plugin._BrushFlow__update_seeding_tasks_based_on_tags = Mock()
        self.plugin._BrushFlow__auto_archive_tasks = Mock()
        self.plugin._cleanup_unused_task_tag = Mock()
        self.plugin._recalculate_statistics = Mock()
        self.plugin._global_dynamic_delete_enabled = lambda: False

    def test_regular_delete_counts_and_notifies_only_successful_safe_hashes(self):
        self.prepare_check()
        report = {}
        self.plugin._run_check(self.task, report)
        self.service.instance.delete_torrents.assert_called_once_with(ids=["safe"], delete_file=True)
        self.assertEqual(report["deleted_count"], 1)
        self.assertFalse(self.records["shared"]["deleted"])
        self.assertTrue(self.records["safe"]["deleted"])
        self.plugin._BrushFlow__send_delete_message.assert_called_once()
        self.assertIs(self.plugin._BrushFlow__send_delete_message.call_args.args[0], self.records["safe"])

    def test_failed_delete_rpc_does_not_count_or_notify_candidates(self):
        self.prepare_check(result=False)
        report = {}
        self.plugin._run_check(self.task, report)
        self.assertEqual(report["deleted_count"], 0)
        self.assertEqual(report["result"], "delete_failed")
        self.assertFalse(any(record["deleted"] for record in self.records.values()))
        self.plugin._BrushFlow__send_delete_message.assert_not_called()

    def test_dynamic_summary_failure_does_not_lose_successful_deletion(self):
        self.prepare_check()
        self.task.proxy_delete = True
        self.task.delete_size_range = "0-0.0000001"
        self.plugin._BrushFlow__send_message = Mock(side_effect=RuntimeError("unavailable"))
        report = {}
        self.plugin._run_check(self.task, report)
        self.assertEqual(report["deleted_count"], 1)
        self.assertTrue(self.records["safe"]["deleted"])
        self.plugin._save_task_data.assert_called()

    def prepare_shared_records(self, protected=False, enabled=True):
        self.prepare_check()
        self.qb_client([self.torrent("shared", content_path="/data/one", progress=1),
                        self.torrent("safe", content_path="/data/two", progress=1)])
        other = SimpleNamespace(**{**vars(self.task), "id": "2", "name": "other", "enabled": enabled})
        records = {"shared": {"size": 1000, "download_started": True,
                              "hit_and_run": protected, "deleted": False}}
        self.plugin._task_configs[other.id] = other
        self.plugin._get_task_config = lambda task_id=None: self.plugin._task_configs[
            task_id or getattr(self.plugin._task_context, "task_id", None) or self.task.id]
        self.plugin._get_task_data = lambda task_id, name, default=None: records if task_id == other.id else self.records
        return other, records

    def test_shared_hash_respects_other_task_hr_even_when_disabled(self):
        for enabled in (False, True):
            with self.subTest(enabled=enabled):
                self.setUp()
                other, records = self.prepare_shared_records(protected=True, enabled=enabled)
                report = {}
                self.plugin._run_check(self.task, report)
                self.service.instance.delete_torrents.assert_called_once_with(ids=["safe"], delete_file=True)
                self.assertEqual(report["deleted_count"], 1)
                self.assertFalse(records["shared"]["deleted"])
                self.assertFalse(self.records["shared"]["deleted"])

    def test_shared_hash_all_qualify_saves_all_records_and_counts_physical_torrents(self):
        other, records = self.prepare_shared_records(enabled=False)
        report = {}
        self.plugin._run_check(self.task, report)
        self.service.instance.delete_torrents.assert_called_once_with(ids=["shared", "safe"], delete_file=True)
        self.assertEqual(report["deleted_count"], 2)
        self.assertTrue(records["shared"]["deleted"])
        self.plugin._save_task_data.assert_any_call(other.id, "torrents", records)
        self.assertEqual(self.plugin._BrushFlow__send_delete_message.call_count, 3)

    def test_shared_hash_busy_or_tag_protected_does_not_block_independent_delete(self):
        for busy in (False, True):
            with self.subTest(busy=busy):
                self.setUp()
                other, records = self.prepare_shared_records()
                lock = self.plugin._task_locks.setdefault(other.id, threading.Lock())
                if busy:
                    lock.acquire()
                else:
                    other.delete_except_tags = "managed"
                try:
                    report = {}
                    self.plugin._run_check(self.task, report)
                finally:
                    if busy:
                        lock.release()
                self.service.instance.delete_torrents.assert_called_once_with(ids=["safe"], delete_file=True)
                self.assertFalse(records["shared"]["deleted"])

    def test_shared_hash_uses_other_tasks_dynamic_threshold(self):
        for threshold, allowed in (("1", False), ("0.0000001", True)):
            with self.subTest(threshold=threshold):
                self.setUp()
                other, records = self.prepare_shared_records()
                other.proxy_delete = True
                other.delete_size_range = threshold
                report = {}
                self.plugin._run_check(self.task, report)
                self.assertEqual(records["shared"]["deleted"], allowed)
                self.assertEqual(report["deleted_count"], 2 if allowed else 1)

    def test_global_delete_counts_and_notifies_only_successful_safe_hashes(self):
        for succeeded in (False, True):
            with self.subTest(succeeded=succeeded):
                self.setUp()
                self.prepare_check(result=succeeded)
                self.plugin._global_dynamic_delete_enabled = lambda: True
                self.plugin._global_delete_size_range = "1"
                self.plugin._all_task_locks_scope = lambda: nullcontext()
                entries = [{"downloader_name": "qb", "torrent_hash": h, "task": self.task,
                            "torrent_task": r, "delete_reason": "normal", "size": 1000}
                           for h, r in self.records.items()]
                self.plugin._collect_global_dynamic_delete_candidates = lambda: (
                    entries, 2000, {self.task.id: self.records}, {"qb": self.service})
                self.plugin._select_global_dynamic_deletions = lambda *args: (entries, 0, True)
                self.assertEqual(self.plugin._run_global_dynamic_delete(), int(succeeded))
                self.service.instance.delete_torrents.assert_called_once_with(ids=["safe"], delete_file=True)
                self.assertFalse(self.records["shared"]["deleted"])
                self.assertEqual(self.records["safe"]["deleted"], succeeded)
                self.assertEqual(self.plugin._BrushFlow__send_delete_message.call_count, int(succeeded))

    def low_download_record(self):
        return {
            "size": 1000, "download_started": True, "deleted": False,
            "transfer_sample": {"at": self.now.timestamp()},
            "slow_download_sample": {
                "at": self.now.timestamp(), "seconds": 1800,
                "downloaded": 0, "uploaded": 0, "minimum_speed": 102400,
            },
        }

    def enable_slow_download_rule(self):
        self.task.download_time = None
        self.task.seed_inactivetime = None
        self.task.download_min_speed = 100
        self.task.download_slow_minutes = 30
        self.task.delete_size_range = None

    def test_slow_download_is_an_independent_rule_with_existing_protections(self):
        self.enable_slow_download_rule()
        record = self.low_download_record()
        torrent = self.torrent(state="downloading", progress=.1, completed=100)
        self.assertTrue(self.evaluate(torrent, record)[0])
        for fields, protected in (({"state": "queuedDL"}, False),
                                  ({"state": "pausedDL"}, False),
                                  ({"progress": 1, "completed": 1000}, False),
                                  ({}, True)):
            with self.subTest(fields=fields, protected=protected):
                item = {**torrent, **fields}
                self.assertFalse(self.evaluate(item, {**record, "hit_and_run": protected})[0])
        self.assertEqual(self.plugin._select_task_deletions(
            self.task, [{**torrent, "tags": "keep"}], {"hash": record}), [])

    def test_local_dynamic_slow_download_cleans_below_space_threshold(self):
        self.enable_slow_download_rule()
        torrent = self.torrent(state="downloading", progress=.1, completed=100)
        selected = self.plugin._BrushFlow__delete_torrent_for_evaluate_proxy_pre_conditions(
            [torrent], {"hash": self.low_download_record()})
        self.assertEqual(selected, ["hash"])

    def test_global_dynamic_slow_download_cleans_below_space_threshold(self):
        self.enable_slow_download_rule()
        self.rows = [self.torrent(state="downloading", progress=.1, completed=100)]
        self.records["hash"] = self.low_download_record()
        candidates, size, _, _ = self.plugin._collect_global_dynamic_delete_candidates()
        selected, _, threshold = self.plugin._select_global_dynamic_deletions(candidates, size, 0, 5000)
        self.assertFalse(threshold)
        self.assertEqual([entry["torrent_hash"] for entry in selected], ["hash"])

    def test_slow_download_respects_shared_files_before_removing_data(self):
        self.prepare_check()
        self.enable_slow_download_rule()
        client = self.qb_client([self.torrent("shared", state="downloading", save_path="/data", content_path="/data/one", progress=.1),
                        self.torrent("foreign", content_path="/data/one", progress=1),
                        self.torrent("safe", state="downloading", save_path="/data", content_path="/data/two", progress=.1)])
        client.torrents_files.side_effect = lambda torrent_hash: [{"name": "two" if torrent_hash == "safe" else "one"}]
        self.records.update({key: self.low_download_record() for key in ("shared", "safe")})
        report = {}
        self.plugin._run_check(self.task, report)
        self.assertEqual(report["deleted_count"], 1)
        self.assertFalse(self.records["shared"]["deleted"])
        self.assertTrue(self.records["safe"]["deleted"])
        self.service.instance.delete_torrents.assert_called_once_with(ids=["safe"], delete_file=True)

    def test_slow_download_requires_all_shared_hash_references_to_qualify(self):
        other, records = self.prepare_shared_records()
        self.enable_slow_download_rule()
        other.download_time = other.seed_inactivetime = None
        other.download_min_speed = other.download_slow_minutes = None
        client = self.qb_client([self.torrent("shared", state="downloading", save_path="/data", content_path="/data/one", progress=.1),
                        self.torrent("safe", state="downloading", save_path="/data", content_path="/data/two", progress=.1)])
        client.torrents_files.side_effect = lambda torrent_hash: [{"name": "two" if torrent_hash == "safe" else "one"}]
        self.records.update({key: self.low_download_record() for key in ("shared", "safe")})
        report = {}
        self.plugin._run_check(self.task, report)
        self.assertEqual(report["deleted_count"], 1)
        self.assertFalse(self.records["shared"]["deleted"])
        self.assertFalse(records["shared"]["deleted"])


if __name__ == "__main__":
    unittest.main()
