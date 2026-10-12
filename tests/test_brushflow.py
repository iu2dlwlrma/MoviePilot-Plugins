import ast
import copy
import importlib.util
import threading
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


def load_plugin():
    """隔离宿主入口，执行实际插件类；测试不加载数据库、网络或调度器。"""
    path = Path(__file__).parents[1] / "plugins.v3/brushflow/__init__.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    tree.body = [node for node in tree.body if not isinstance(node, ast.ImportFrom)
                 or node.level == 0 and node.module.split(".")[0]
                 not in {"app", "fastapi", "apscheduler"}]
    tree.body.insert(0, ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0))
    namespace = {
        "_PluginBase": object,
        "Query": lambda default, **kwargs: default,
        "eventmanager": SimpleNamespace(register=lambda *args: lambda func: func),
        "EventType": SimpleNamespace(PluginReload="reload", PluginTriggered="triggered"),
        "logger": Mock(),
        "settings": SimpleNamespace(TZ="UTC"),
    }
    for name in ("models", "rules"):
        spec = importlib.util.spec_from_file_location(f"brushflow_{name}", path.with_name(f"{name}.py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        namespace.update({key: value for key, value in vars(module).items() if not key.startswith("_")})
    exec(compile(ast.fix_missing_locations(tree), str(path), "exec"), namespace)
    return namespace


class BrushFlowTests(unittest.TestCase):
    def setUp(self):
        self.ns = load_plugin()
        self.cls = self.ns["BrushFlow"]
        self.plugin = self.cls()
        p = self.plugin
        p._enabled = True
        p._stopping = False
        p._schedule_started = datetime.now().astimezone()
        p._pending_brush = {}
        p._task_context = threading.local()
        p._task_locks = {}
        p._runtime_lock = threading.Lock()
        p._brush_lock = threading.Lock()
        p._runtime = {}
        p._task_configs = {}
        p._global_defaults = {key: self.ns["DEFAULT_TASK_SETTINGS"][key] for key in self.ns["SHARED_TASK_FIELDS"]}
        p._latest_site_user_data_by_domain = lambda: {}
        p._build_site_ratio_status = lambda *args, **kwargs: {"available": False}
        p._global_maxdlcount = 5
        p._global_disksize = p._global_maxupspeed = p._global_maxdlspeed = None
        self.data = {}
        p.get_data = lambda key: copy.deepcopy(self.data.get(key))
        p.save_data = lambda key, value: self.data.__setitem__(key, copy.deepcopy(value))
        self.rows = {"tb": [], "qb": []}
        self.services = {
            name: SimpleNamespace(name=name, instance=SimpleNamespace(
                get_torrents=lambda name=name: (self.rows[name], False), is_inactive=lambda: False))
            for name in self.rows
        }
        self.ns["DownloaderHelper"] = lambda: SimpleNamespace(
            get_service=lambda name: self.services.get(name),
            is_downloader=lambda kind, service: (kind == "qbittorrent") == (service.name == "qb"))
        self.ns["SiteOper"] = lambda: SimpleNamespace(get=lambda sid: SimpleNamespace(id=sid, name=str(sid), domain=str(sid)))
        self.site_test = Mock(return_value=(True, ""))
        self.ns["SiteChain"] = lambda: SimpleNamespace(test=self.site_test)
        self.candidates = {}
        self.ns["TorrentsChain"] = lambda: SimpleNamespace(browse=lambda domain: self.candidates.get(domain, []))
        p._validate_task_reference = lambda *args, **kwargs: True
        p._evaluate_site_ratio_control = lambda *args, **kwargs: (True, None, {"enabled": False})
        p._BrushFlow__get_subscribe_titles = lambda: set()
        p._BrushFlow__get_average_bandwidth = lambda: (0, 0)
        p._BrushFlow__evaluate_conditions_for_brush = lambda torrent, records: (torrent.allowed, "筛选不匹配")
        p._recalculate_statistics = lambda *args: None
        p._refresh_scheduler = Mock()
        p.eventmanager = SimpleNamespace(send_event=lambda **kwargs: None)
        p._BrushFlow__send_add_message = lambda torrent: None
        p._torrent_to_task_record = lambda torrent, site, task: {
            "size": torrent.size, "time": 1, "deleted": False, "site": site.id,
        }
        self.added = []

        def download(torrent):
            task = p._get_task_config()
            self.added.append(task.id)
            return f"hash-{task.id}-{len(self.added)}"

        p._BrushFlow__download = download

    def test_focus_queue_priority_preserves_paused_and_running(self):
        self.add_task(1)
        self.add_task(2)
        self.add_task(3, enabled=False)
        self.plugin._rotation_mode = "sequential"
        self.data["site_rotation"] = {"site_id": 2, "deadline_at": 9999999999}
        client = Mock()
        self.services["tb"].instance.trc = client
        client.get_torrents.return_value = [
            SimpleNamespace(hashString=h, queue_position=i, labels=[tag], status=status)
            for i, (h, tag, status) in enumerate([
                ("other", "site-1", "download pending"),
                ("disabled", "site-3", "download pending"),
                ("paused", "site-2", "stopped"),
                ("active", "site-2", "downloading"),
                ("focus-a", "site-2", "download pending"),
                ("focus-b", "site-2", "download pending"),
            ])]
        self.plugin._prioritize_download_queue({})
        self.assertEqual([call.kwargs["ids"] for call in client.queue_top.call_args_list],
                         [["focus-b"], ["focus-a"]])

    def test_queue_priority_noop_when_already_ordered(self):
        self.add_task(1)
        client = Mock()
        self.services["tb"].instance.trc = client
        client.get_torrents.return_value = [
            SimpleNamespace(hashString="a", queue_position=4, labels=["site-1"], status="download pending")]
        self.plugin._prioritize_download_queue({})
        client.queue_top.assert_not_called()

    def test_qb_queue_priority_uses_site_tiers_and_records(self):
        self.plugin._build_site_ratio_status = lambda *args: {"available": True, "reached": False}
        self.add_task(1, downloader="qb")
        task = self.add_task(2, downloader="qb")
        task.novice = True
        self.data["task.2.torrents"] = {"novice": {}}
        client = Mock()
        self.services["qb"].instance.qbc = client
        client.torrents_info.return_value = [
            {"hash": "normal", "priority": 1, "tags": "site-1", "state": "queuedDL"},
            {"hash": "novice", "priority": 2, "tags": "", "state": "queuedDL"},
            {"hash": "paused", "priority": 3, "tags": "site-2", "state": "stoppedDL"}]
        self.plugin._prioritize_download_queue({})
        self.assertEqual([call.kwargs["torrent_hashes"] for call in client.torrents_top_priority.call_args_list],
                         [["normal"], ["novice"]])

    def priority_client(self):
        self.plugin._build_site_ratio_status = lambda *args: {"available": True, "reached": False}
        self.add_task(1)
        self.add_task(2).novice = True
        self.plugin._rotation_mode = "parallel"
        client = Mock()
        self.services["tb"].instance.trc = client
        client.get_session.return_value = SimpleNamespace(download_queue_enabled=True)
        rows = [SimpleNamespace(hashString=h, queue_position=i, labels=[tag], status=status)
                for i, (h, tag, status) in enumerate([
                    ("low", "site-1", "downloading"),
                    ("high", "site-2", "download pending"),
                    ("manual", "site-2", "stopped"),
                    ("seeding", "site-1", "seeding"),
                    ("external", "other", "downloading"),
                ])]
        client.get_torrents.return_value = rows
        def stop(ids):
            for row in rows:
                if row.hashString in ids:
                    row.status = "stopped"
            rows[1].status = "downloading"
        def start(ids, bypass_queue=False):
            self.assertFalse(bypass_queue)
            for row in rows:
                if row.hashString in ids:
                    row.status = "download pending"
        client.stop_torrent.side_effect = stop
        client.start_torrent.side_effect = start
        return client, rows

    def test_priority_reclaims_active_slot_and_returns_victim_to_queue(self):
        client, rows = self.priority_client()
        self.plugin._prioritize_download_queue({})
        self.assertEqual({r.hashString for r in rows if r.status == "downloading"}, {"high", "external"})
        self.assertEqual([c.kwargs["ids"] for c in client.start_torrent.call_args_list],
                         [["low"]])
        self.assertFalse(self.data.get("priority_requeue.tb"))

    def test_priority_does_not_preempt_equal_tier_or_sequential_downloads(self):
        for mode, novice in [("parallel", False), ("sequential", True)]:
            with self.subTest(mode=mode):
                client, rows = self.priority_client()
                self.plugin._task_configs["2"].novice = novice
                self.plugin._rotation_mode = mode
                self.data["site_rotation"] = {"site_id": 2, "deadline_at": 9999999999}
                self.plugin._prioritize_download_queue({})
                client.stop_torrent.assert_not_called()

    def test_priority_without_downloader_queue_does_not_stop_downloads(self):
        client, rows = self.priority_client()
        client.get_session.return_value.download_queue_enabled = False
        self.plugin._prioritize_download_queue({})
        client.stop_torrent.assert_not_called()

    def test_priority_failed_confirmation_restores_victim(self):
        client, rows = self.priority_client()
        self.plugin._wait_transmission_allocation = Mock(side_effect=[RuntimeError("offline"), None])
        self.plugin._prioritize_download_queue({})
        self.assertEqual([c.kwargs["ids"] for c in client.start_torrent.call_args_list],
                         [["low"]])
        self.assertFalse(self.data.get("priority_requeue.tb"))

    def test_priority_does_not_restart_ambiguous_pause_after_interruption(self):
        client, rows = self.priority_client()
        client.start_torrent.side_effect = RuntimeError("offline")
        self.plugin._prioritize_download_queue({})
        self.assertEqual(self.data.get("priority_requeue.tb"), ["low"])
        rows[0].status = "stopped"
        client.reset_mock()
        client.start_torrent.side_effect = None
        self.plugin._prioritize_download_queue({})
        client.start_torrent.assert_not_called()
        self.assertEqual(self.data.get("allocation_alert.tb"), ["low"])
        self.assertFalse(self.data.get("priority_requeue.tb"))

    def test_priority_does_not_release_stalled_download_that_uses_no_slot(self):
        client, rows = self.priority_client()
        client.get_session.return_value.queue_stalled_enabled = True
        rows[0].is_stalled = True
        self.plugin._prioritize_download_queue({})
        client.stop_torrent.assert_not_called()

    def test_priority_recovery_respects_disabled_site(self):
        client, rows = self.priority_client()
        self.data["priority_requeue.tb"] = ["low"]
        rows[0].status = "stopped"
        self.plugin._task_configs["1"].enabled = False
        self.plugin._prioritize_download_queue({})
        client.start_torrent.assert_not_called()
        self.assertEqual(self.data.get("allocation_alert.tb"), ["low"])
        self.assertFalse(self.data.get("priority_requeue.tb"))

    def test_priority_eight_slots_follow_tiers_without_repeated_preemption(self):
        client, rows = self.priority_client()
        self.add_task(3)
        self.plugin._build_site_ratio_status = lambda task, data: {"available": task.id == "3", "met": False}
        self.plugin._priority_tier = lambda task, status: {"1": 2, "2": 0, "3": 1}[task.id]
        rows[:] = [SimpleNamespace(hashString=f"low-{i}", queue_position=i,
                                   labels=["site-1"], status="downloading") for i in range(7)]
        rows += [SimpleNamespace(hashString=f"high-{i}", queue_position=7+i,
                                 labels=["site-2"], status="downloading" if i == 0 else "download pending")
                 for i in range(3)]
        rows.append(SimpleNamespace(hashString="ratio", queue_position=10,
                                    labels=["site-3"], status="download pending"))
        def stop(ids):
            for row in rows:
                if row.hashString in ids:
                    row.status = "stopped"
            for row in rows:
                if row.status == "download pending":
                    row.status = "downloading"
        def start(ids, bypass_queue=False):
            self.assertFalse(bypass_queue)
            for torrent_hash in ids:
                row = next(row for row in rows if row.hashString == torrent_hash)
                if row.status == "downloading":
                    continue
                row.status = "downloading" if sum(r.status == "downloading" for r in rows) < 8 else "download pending"
        client.stop_torrent.side_effect = stop
        client.start_torrent.side_effect = start
        self.plugin._prioritize_download_queue({})
        active = [row.hashString for row in rows if row.status == "downloading"]
        self.assertEqual(len(active), 8)
        self.assertTrue({"high-0", "high-1", "high-2", "ratio"}.issubset(active))
        client.stop_torrent.reset_mock()
        self.plugin._prioritize_download_queue({})
        client.stop_torrent.assert_not_called()

    def test_rotation_retries_five_times_ten_minutes_apart_then_skips(self):
        self.add_task(1)
        self.add_task(2)
        self.plugin._rotation_mode = "sequential"
        self.plugin._select_rotation_site({}, now=100)
        self.site_test.return_value = (False, "unavailable")
        self.assertFalse(self.plugin._check_rotation_site(now=100))
        for attempt in range(1, 6):
            due = 100 + attempt * 600
            self.assertFalse(self.plugin._check_rotation_site(now=due - 1))
            self.assertEqual(self.site_test.call_count, attempt)
            self.assertFalse(self.plugin._check_rotation_site(now=due))
        self.assertEqual(self.site_test.call_count, 6)
        self.assertEqual(self.plugin._select_rotation_site({}, now=3100), 2)

    def test_rotation_retry_recovery_and_restart_keep_schedule(self):
        self.plugin._build_site_ratio_status = lambda *args: {"available": True, "reached": True}
        task = self.add_task(1)
        self.plugin._rotation_mode = "sequential"
        self.plugin._select_rotation_site({}, now=100)
        self.site_test.return_value = (False, "")
        self.plugin._check_rotation_site(now=100)
        self.assertEqual(self.plugin._select_rotation_site({}, now=600), 1)
        self.assertFalse(self.plugin._check_rotation_site(now=699))
        self.site_test.return_value = (True, "")
        self.assertTrue(self.plugin._check_rotation_site(now=700))
        state = self.data["site_rotation"]
        self.assertIsNone(state["retry_at"])
        self.assertEqual(state["deadline_at"], 700 + task.rotation_hours * 3600)
        self.assertIn(task.id, self.plugin._pending_brush)
        self.plugin._check_rotation_site(now=701)
        self.assertEqual(self.site_test.call_count, 2)

    def test_only_failed_site_waits_instead_of_busy_loop(self):
        self.add_task(1)
        self.plugin._rotation_mode = "sequential"
        self.plugin._select_rotation_site({}, now=100)
        self.site_test.return_value = (False, "")
        for now in range(100, 3101, 600):
            self.plugin._check_rotation_site(now=now)
        self.assertIsNone(self.plugin._select_rotation_site({}, now=3101))
        self.assertFalse(self.plugin._check_rotation_site(now=3101))
        self.assertEqual(self.site_test.call_count, 6)
        self.assertEqual(self.plugin._select_rotation_site({}, now=3700), 1)

    def test_completed_retention_cap_blocks_at_fifty_and_resumes_after_removal(self):
        task = self.add_task(1, downloader="qb")
        self.add_task(2, site=1, downloader="qb")
        self.rows["qb"] = [{"hash": str(i), "progress": 1, "tags": "site-1"} for i in range(50)]
        self.rows["qb"] += [{"hash": "pending", "progress": 0, "tags": "site-1"},
                            {"hash": "other", "progress": 1, "tags": "site-3"}]
        self.assertEqual(self.plugin._site_completed_count(1), 50)
        with self.plugin._task_scope(task.id):
            allowed, reason = self.plugin._BrushFlow__evaluate_pre_conditions_for_brush()
            self.assertFalse(allowed)
            self.assertIn("50", reason)
            self.rows["qb"].pop(0)
            self.assertTrue(self.plugin._BrushFlow__evaluate_pre_conditions_for_brush()[0])

    def test_completed_retention_counts_transmission_records_not_missing_torrents(self):
        self.add_task(1)
        self.data["task.1.torrents"] = {"done": {}, "queued": {}, "removed": {}}
        self.rows["tb"] = [SimpleNamespace(hashString="done", percent_done=1, labels=[]),
                           SimpleNamespace(hashString="queued", percent_done=0, labels=[])]
        self.assertEqual(self.plugin._site_completed_count(1), 1)

    def test_rss_is_site_specific_even_when_global_defaults_change(self):
        config = {"site_id": 1, "downloader": "tb", "use_global_defaults": True,
                  "override_fields": [], "rss_support": True}
        task = self.ns["BrushTaskConfig"](config, {"rss_support": False})
        self.assertTrue(task.rss_support)
        config["rss_support"] = False
        self.assertFalse(self.ns["BrushTaskConfig"](config, {"rss_support": True}).rss_support)
        self.assertNotIn("rss_support", self.ns["SHARED_TASK_FIELDS"])

    def add_task(self, tid, site=None, downloader="tb", enabled=True):
        task = self.ns["BrushTaskConfig"]({
            "id": str(tid), "name": str(tid), "site_id": site or int(tid),
            "downloader": downloader, "tag": f"site-{site or tid}", "enabled": enabled,
        })
        self.plugin._task_configs[task.id] = task
        self.candidates[str(task.site_id)] = [
            SimpleNamespace(title=f"candidate-{n}", description="", size=100, pubdate=str(n), allowed=True)
            for n in range(3)
        ]
        return task

    def queue(self, *tasks):
        for task in tasks:
            self.plugin._queue_brush(task.id)

    def test_nine_sites_rotate_without_plugin_download_cap(self):
        tasks = [self.add_task(i) for i in range(1, 10)]
        self.queue(*tasks)
        self.plugin._dispatch_brush()
        self.assertEqual(self.added, [str(i) for i in range(1, 10)] * 3)
        self.assertEqual(self.plugin._pending_brush, {})
        for task in tasks:
            report = self.plugin._get_task_data(task.id, "runs")[0]
            self.assertEqual(report["added_count"], 3)
            self.assertEqual(report["source_count"], 3)

    def test_full_downloader_queue_does_not_block_additions(self):
        tasks = [self.add_task(i) for i in range(1, 7)]
        self.rows["tb"] = [SimpleNamespace(hashString=str(i), labels=[f"site-{i}"],
                                          status="download pending", percent_done=0.0) for i in range(1, 6)]
        self.queue(tasks[-1])
        self.plugin._dispatch_brush()
        self.assertEqual(self.added, ["6"] * 3)
        self.assertNotIn("6", self.plugin._pending_brush)

    def test_unfinished_states_and_seeding_do_not_depend_on_status_spelling(self):
        for state in ["download pending", "download_pending", "stopped", "checking", "downloading"]:
            with self.subTest(state=state):
                self.assertTrue(self.cls._unfinished_download(SimpleNamespace(status=state, percent_done=0.1)))
                self.assertFalse(self.cls._unfinished_download(SimpleNamespace(status=state, percent_done=1)))
        for state in ["queuedDL", "pausedDL", "stoppedDL", "error", "checkingDL"]:
            self.assertTrue(self.cls._unfinished_download({"state": state, "progress": 0.1}))
        self.assertFalse(self.cls._unfinished_download({"state": "stalledUP", "progress": 1}))
        self.assertTrue(self.cls._unfinished_download(SimpleNamespace(status="download pending", progress=99)))

    def test_existing_unfinished_site_does_not_limit_downloaders(self):
        first = self.add_task(1)
        second = self.add_task(2, site=1, downloader="qb")
        other = self.add_task(3)
        self.rows["tb"] = [SimpleNamespace(hashString="a", labels=[first.brush_tag], percent_done=0.1)]
        self.queue(second, other)
        self.plugin._dispatch_brush()
        self.assertEqual(self.added, ["2", "3"] * 3)
        self.assertNotIn("2", self.plugin._pending_brush)

    def test_count_deduplicates_hash_and_handles_mixed_downloaders_and_legacy_records(self):
        first = self.add_task(1)
        self.add_task(2, site=1)
        self.add_task(3, downloader="qb", enabled=False)
        self.rows["tb"] = [SimpleNamespace(hashString="same", labels=[first.brush_tag], percent_done=0.2)]
        self.rows["qb"] = [{"hash": "same", "tags": "刷流", "progress": 0.3},
                           {"hash": "legacy", "tags": "", "progress": 0.1},
                           {"hash": "other", "tags": "MOVIEPILOT", "progress": 0.1}]
        self.plugin._save_task_data("3", "torrents", {"legacy": {"deleted": False}})
        self.assertEqual(self.plugin._BrushFlow__get_global_downloading_count(), 3)

    def test_no_candidates_or_filtered_candidates_do_not_block_next_site(self):
        tasks = [self.add_task(i) for i in range(1, 4)]
        self.candidates["1"] = []
        for candidate in self.candidates["2"]:
            candidate.allowed = False
        self.queue(*tasks)
        self.plugin._dispatch_brush()
        self.assertEqual(self.added, ["3"] * 3)

    def test_failure_to_read_downloader_preserves_pending_and_adds_nothing(self):
        task = self.add_task(1)
        self.services["tb"].instance.get_torrents = lambda: (None, True)
        self.queue(task)
        self.plugin._dispatch_brush()
        self.assertEqual(self.added, [])
        self.assertIn("1", self.plugin._pending_brush)
        self.assertFalse(self.plugin._brush_lock.locked())

    def test_busy_task_remains_pending_and_other_sites_continue(self):
        tasks = [self.add_task(i) for i in range(1, 3)]
        lock = threading.Lock()
        lock.acquire()
        self.plugin._task_locks["1"] = lock
        self.queue(*tasks)
        self.plugin._dispatch_brush()
        self.assertEqual(self.added, ["2"] * 3)
        self.assertIn("1", self.plugin._pending_brush)
        lock.release()

    def test_manual_run_respects_existing_pending_priority(self):
        first, second = self.add_task(1), self.add_task(2)
        self.plugin._global_maxdlcount = 1
        self.plugin._save_task_data("1", "rotation", {"last_added": 10})
        self.queue(second)
        self.plugin.brush(first.id)
        self.assertEqual(self.added, ["2", "1"] * 3)

    def test_rotation_survives_empty_recent_run_history(self):
        task = self.add_task(1)
        self.plugin._save_task_data(task.id, "rotation", {"last_added": 123})
        self.plugin._save_task_data(task.id, "runs", [])
        self.assertEqual(self.plugin._site_last_added(1), 123)

    def test_disabled_or_outside_time_window_sites_are_not_added(self):
        disabled = self.add_task(1, enabled=False)
        outside = self.add_task(2)
        eligible = self.add_task(3)
        self.plugin._is_current_time_in_range = lambda task: task.id != "2"
        self.queue(disabled, outside, eligible)
        self.plugin._dispatch_brush()
        self.assertEqual(self.added, ["3"] * 3)

    def test_concurrent_dispatch_does_not_duplicate_candidates(self):
        tasks = [self.add_task(i) for i in range(1, 3)]
        self.plugin._global_maxdlcount = 1
        entered, finish = threading.Event(), threading.Event()
        download = self.plugin._BrushFlow__download

        def blocked(torrent):
            entered.set()
            self.assertTrue(finish.wait(3))
            return download(torrent)

        self.plugin._BrushFlow__download = blocked
        self.queue(*tasks)
        worker = threading.Thread(target=self.plugin._dispatch_brush)
        worker.start()
        self.assertTrue(entered.wait(3))
        self.plugin._dispatch_brush()
        finish.set()
        worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(self.added, ["1", "2"] * 3)

    def test_scheduler_rebuild_keeps_interval_anchor(self):
        self.add_task(1)
        self.plugin._next_promotion_expiry = lambda task: None
        first = self.plugin.get_service()
        second = self.plugin.get_service()
        self.assertEqual([x["kwargs"] for x in first], [x["kwargs"] for x in second])
        self.assertEqual(first[0]["func"], self.plugin._queue_brush)
        self.assertTrue(all("start_date" in x["kwargs"] for x in first))

    def test_qb_accepted_without_hash_reserves_space_and_recovers_identity(self):
        tasks = [self.add_task(i, downloader="qb") for i in range(1, 3)]
        self.plugin._global_disksize = 100 / 1024 ** 3
        self.ns["StringUtils"] = SimpleNamespace(generate_random_str=lambda size: "unique-tag")
        self.plugin._BrushFlow__download = self.cls._BrushFlow__download.__get__(self.plugin)
        downloader = self.services["qb"].instance
        downloader.add_torrent = Mock(return_value=True)
        downloader.get_torrent_id_by_tag = Mock(return_value=None)
        for candidates in self.candidates.values():
            for torrent in candidates:
                torrent.enclosure = "magnet:?xt=urn:btih:" + "a" * 40
                torrent.site_proxy = False
                torrent.site_cookie = None
        self.queue(*tasks)
        self.plugin._dispatch_brush()
        self.assertEqual(downloader.add_torrent.call_count, 1)
        records = self.plugin._get_task_data("1", "torrents")
        self.assertEqual(records["pending:unique-tag"]["size"], 100)
        self.assertEqual(self.plugin._calculate_global_seeding_size(), 100)
        self.plugin._resolve_pending_downloads(records, {"real-hash": {"tags": "site-1, unique-tag"}})
        self.assertNotIn("pending:unique-tag", records)
        self.assertEqual(records["real-hash"]["size"], 100)
        self.assertGreater(self.plugin._site_last_added(1), 0)

    def test_hash_lookup_exception_keeps_capacity_reserved(self):
        tasks = [self.add_task(i, downloader="qb") for i in range(1, 3)]
        self.plugin._global_disksize = 100 / 1024 ** 3
        self.ns["StringUtils"] = SimpleNamespace(generate_random_str=lambda size: "lookup-error")
        self.plugin._BrushFlow__download = self.cls._BrushFlow__download.__get__(self.plugin)
        downloader = self.services["qb"].instance
        downloader.add_torrent = Mock(return_value=True)
        downloader.get_torrent_id_by_tag = Mock(side_effect=RuntimeError("unavailable"))
        for candidates in self.candidates.values():
            for torrent in candidates:
                torrent.enclosure = "magnet:?xt=urn:btih:" + "a" * 40
                torrent.site_proxy = False
                torrent.site_cookie = None
        self.queue(*tasks)
        self.plugin._dispatch_brush()
        self.assertEqual(downloader.add_torrent.call_count, 1)
        self.assertEqual(self.plugin._calculate_global_seeding_size(), 100)

    def test_novice_then_low_ratio_then_normal_get_first_round(self):
        tasks = [self.add_task(i) for i in range(1, 5)]
        tasks[2].novice = True
        tasks[3].novice = True
        tasks[3].novice_until = "2000-01-01T00:00:00+00:00"
        self.plugin._build_site_ratio_status = lambda task, *args, **kwargs: {
            "available": True, "reached": task.id not in {"2", "3"},
        }
        self.queue(*tasks)
        self.plugin._dispatch_brush()
        self.assertEqual(self.added, ["3", "2", "1", "4"] * 3)

    def test_daily_rotation_only_adds_focus_and_keeps_old_torrents_and_checks(self):
        self.plugin._build_site_ratio_status = lambda *args: {"available": True, "reached": True}
        first, second = self.add_task(1), self.add_task(2)
        p = self.plugin
        p._rotation_mode = "sequential"
        p._run_check = Mock()
        with patch.object(self.ns["time"], "time", return_value=1800000000) as clock:
            self.queue(first, second)
            p._dispatch_brush()
            self.assertEqual(self.added, ["1"] * 3)
            self.assertIn("2", p._pending_brush)
            retained = p._get_task_data(first.id, "torrents")
            p.check(second.id)
            self.assertEqual(p._run_check.call_args.args[0], second)
            self.queue(first)
            clock.return_value += 86400
            p._dispatch_brush()
            self.assertEqual(self.added, ["1"] * 3 + ["2"] * 3)
            self.assertEqual(p._get_task_data(first.id, "torrents"), retained)
            p.check(first.id)
            self.assertEqual(p._run_check.call_args.args[0], first)
            self.assertTrue(first.enabled)

    def test_unmet_site_stays_until_target_even_when_met_site_is_novice(self):
        normal, low, novice = (self.add_task(i) for i in range(1, 4))
        novice.novice = True
        p = self.plugin
        p._rotation_mode = "sequential"
        p._build_site_ratio_status = lambda task, *args: {"available": True, "reached": task.id != low.id}
        selected = [p._select_rotation_site({}, now=1800000000 + day * 86400) for day in range(4)]
        self.assertEqual(selected, [low.site_id] * 4)

    def test_daily_focus_survives_reload_and_ratio_changes_until_its_deadline(self):
        self.plugin._build_site_ratio_status = lambda *args: {"available": True, "reached": True}
        first, second = self.add_task(1), self.add_task(2)
        p = self.plugin
        p._rotation_mode = "sequential"
        first.rotation_hours = 48
        start = 1800000000
        self.assertEqual(p._select_rotation_site({}, start), first.site_id)
        reloaded = self.cls()
        reloaded._rotation_mode = "sequential"
        reloaded._task_configs = p._task_configs
        reloaded.get_data = p.get_data
        reloaded.save_data = p.save_data
        reloaded._build_site_ratio_status = p._build_site_ratio_status
        second.novice = True
        self.assertEqual(reloaded._select_rotation_site({}, start + 86400), first.site_id)
        self.assertEqual(self.data["site_rotation"]["deadline_at"], start + 48 * 3600)

    def test_disabled_focus_advances_and_rotation_runs_even_without_pending_tasks(self):
        first, second = self.add_task(1), self.add_task(2)
        p = self.plugin
        p._rotation_mode = "sequential"
        with patch.object(self.ns["time"], "time", return_value=1800000000):
            p._dispatch_brush()
            self.assertEqual(self.data["site_rotation"]["site_id"], first.site_id)
            first.enabled = False
            self.queue(second)
            p._dispatch_brush()
            self.assertEqual(self.added, ["1"] * 3 + ["2"] * 3)
            second.enabled = False
            p._dispatch_brush()
            self.assertEqual(self.data["site_rotation"], {})

    def test_same_site_tasks_share_one_daily_turn_and_expiry_blocks_further_adds(self):
        self.plugin._build_site_ratio_status = lambda *args: {"available": True, "reached": True}
        first, same, other = self.add_task(1), self.add_task(2, site=1), self.add_task(3)
        p = self.plugin
        p._rotation_mode = "sequential"
        with patch.object(self.ns["time"], "time", return_value=1800000000) as clock:
            self.queue(first, same, other)
            p._dispatch_brush()
            self.assertEqual(self.added, ["1", "2"] * 3)
            clock.return_value += 86400
            self.assertFalse(p._is_focus_site(first.site_id))
            self.assertEqual(p._select_rotation_site({}), other.site_id)
            self.assertEqual(self.data["site_rotation"]["visited_site_ids"], [1, 3])

    def test_rotation_hours_inherit_and_override_roundtrip(self):
        config = self.ns["BrushTaskConfig"]
        task = config({"id": "1", "name": "one", "site_id": 1}, {"rotation_hours": 48})
        self.assertEqual(task.rotation_hours, 48)
        self.assertNotIn("rotation_hours", task.to_dict())
        task = config({**task.to_dict(), "override_fields": ["rotation_hours"], "rotation_hours": 12},
                      {"rotation_hours": 48})
        self.assertEqual(config(task.to_dict(), {"rotation_hours": 24}).rotation_hours, 12)

    def test_browse_crossing_focus_deadline_retains_pending_without_adding(self):
        self.plugin._build_site_ratio_status = lambda *args: {"available": True, "reached": True}
        task = self.add_task(1)
        p = self.plugin
        p._rotation_mode = "sequential"
        with patch.object(self.ns["time"], "time", return_value=1800000000) as clock:
            p._select_rotation_site({})
            clock.return_value += 86399

            def browse(domain):
                clock.return_value += 2
                return self.candidates[domain]

            self.ns["TorrentsChain"] = lambda: SimpleNamespace(browse=browse)
            self.queue(task)
            p._dispatch_brush()
        self.assertEqual(self.added, [])
        self.assertIn(task.id, p._pending_brush)
        report = p._get_task_data(task.id, "runs")[0]
        self.assertEqual(report["result"], "outside_focus")
        self.assertNotIn("下载器添加失败", report["reason_counts"])

    def test_torrent_fetch_crossing_focus_deadline_never_submits_to_downloader(self):
        for downloader_name in ("tb", "qb"):
            with self.subTest(downloader=downloader_name):
                self.setUp()
                task = self.add_task(1, downloader=downloader_name)
                p = self.plugin
                p._rotation_mode = "sequential"
                p._build_site_ratio_status = lambda *args: {"available": True, "reached": True}
                p._BrushFlow__download = self.cls._BrushFlow__download.__get__(p)
                self.ns["StringUtils"] = SimpleNamespace(generate_random_str=lambda size: "unique-tag")
                downloader = self.services[downloader_name].instance
                downloader.add_torrent = Mock()
                for torrent in self.candidates["1"]:
                    torrent.enclosure = "https://example.invalid/torrent"
                    torrent.site_proxy = False
                    torrent.site_cookie = None
                    torrent.site_ua = None
                with patch.object(self.ns["time"], "time", return_value=1800000000) as clock:
                    p._select_rotation_site({})
                    clock.return_value += 86399

                    def fetch(**kwargs):
                        clock.return_value += 2
                        return SimpleNamespace(ok=True, content=b"torrent")

                    self.ns["RequestUtils"] = lambda **kwargs: SimpleNamespace(get_res=fetch)
                    self.queue(task)
                    p._dispatch_brush()
                downloader.add_torrent.assert_not_called()
                self.assertIn(task.id, p._pending_brush)
                report = p._get_task_data(task.id, "runs")[0]
                self.assertEqual(report["result"], "outside_focus")
                self.assertNotIn("下载器添加失败", report["reason_counts"])

    def test_settings_without_rotation_mode_preserve_focus_and_deadline(self):
        task = self.add_task(1)
        p = self.plugin
        p._rotation_mode = "sequential"
        p._select_rotation_site({}, now=1800000000)
        previous = copy.deepcopy(self.data["site_rotation"])
        p._show_sidebar_nav = True
        p._global_proxy_delete = False
        p._global_delete_size_range = None
        p._save_config = Mock()
        p._build_status_data = lambda: {}
        self.ns["schemas"] = SimpleNamespace(Response=lambda **values: SimpleNamespace(**values))
        payload = self.ns["BrushFlowSettingsPayload"](global_disksize=100)
        self.assertTrue(p.update_settings(payload).success)
        self.assertEqual(p._rotation_mode, "sequential")
        self.assertEqual(self.data["site_rotation"], previous)

    def test_space_reserves_queued_full_size_and_deduplicates_same_hash(self):
        first, second = self.add_task(1), self.add_task(2)
        self.plugin._global_disksize = 300 / 1024 ** 3
        for task in (first, second):
            self.plugin._save_task_data(task.id, "torrents", {"shared": {"size": 100, "downloaded": 0}})
        self.queue(first, second)
        self.plugin._dispatch_brush()
        self.assertEqual(self.added, ["1", "2"])
        self.assertEqual(self.plugin._calculate_global_seeding_size(), 300)

    def test_global_inheritance_roundtrip_preserves_false_null_and_site_flags(self):
        config = self.ns["BrushTaskConfig"]
        defaults = {"downloader": "qb", "notify": True, "size": "10-100", "leecher": "3"}
        task = config({"id": "1", "name": "one", "site_id": 1, "override_fields": ["notify", "size"],
                       "notify": False, "size": None, "site_skip_tips": True}, defaults)
        self.assertFalse(task.notify)
        self.assertIsNone(task.size)
        self.assertEqual(task.leecher, "3")
        self.assertEqual(task.downloader, "qb")
        stored = task.to_dict()
        self.assertNotIn("leecher", stored)
        restored = config(stored, {**defaults, "leecher": "8"})
        self.assertEqual(restored.leecher, "8")
        self.assertFalse(restored.notify)
        self.assertIsNone(restored.size)
        self.assertTrue(restored.site_skip_tips)

    def test_legacy_explicit_values_migrate_as_overrides_but_no_count_or_speed_caps(self):
        task = self.ns["BrushTaskConfig"]({"downloader": "tb", "seed_inactivetime": 1440,
                                           "site_ratio_target": 9, "maxdlcount": 1, "maxupspeed": 1,
                                           "up_speed": 5, "del_no_free": False})
        self.assertEqual(task.site_ratio_target, 9)
        self.assertIn("site_ratio_target", task.override_fields)
        self.assertIn("seed_inactivetime", task.override_fields)
        self.assertEqual(task.leecher, "1")
        self.assertFalse(task.del_no_free)
        self.assertIsNone(task.maxdlcount)
        self.assertIsNone(task.up_speed)
        self.assertNotIn("maxupspeed", task.to_dict())

    def test_ratio_bytes_fallback_zero_unknown_and_reached_still_allowed(self):
        task = self.add_task(1)
        self.ns["StringUtils"] = SimpleNamespace(get_url_domain=lambda value: value)
        build = self.cls._build_site_ratio_status.__get__(self.plugin)
        row = SimpleNamespace(ratio=0, upload=20, download=100, updated_day=None, updated_time=None)
        status = build(task, {"1": row})
        self.assertEqual(status["current"], 0.2)
        self.assertTrue(status["estimated"])
        self.assertEqual(self.plugin._priority_tier(task, status), 1)
        row.download = 0
        status = build(task, {"1": row})
        self.assertTrue(status["unlimited"])
        self.assertTrue(status["reached"])
        row.upload = 0
        self.assertFalse(build(task, {"1": row})["available"])
        self.plugin._build_site_ratio_status = lambda *args, **kwargs: {"available": True, "reached": True}
        self.assertTrue(self.cls._evaluate_site_ratio_control(self.plugin, task)[0])

    def test_candidate_selection_combines_demand_seeders_and_promotion_margin(self):
        from datetime import timedelta, timezone
        task = self.add_task(1)
        evaluate = self.cls._BrushFlow__evaluate_conditions_for_brush.__get__(self.plugin)
        torrent = SimpleNamespace(title="sample", description="", site_name="one", page_url=None,
                                  size=100, downloadvolumefactor=0, uploadvolumefactor=1,
                                  hit_and_run=False, seeders=10, peers=1, freedate=None)
        self.data["task.1.metrics"] = {"seconds": 0, "downloaded": None, "downloading": None,
                                       "gap": True, "partial": True}
        with self.plugin._task_scope(task.id):
            self.assertTrue(evaluate(torrent, {})[0])
            torrent.peers = 0
            self.assertEqual(evaluate(torrent, {})[1], "下载人数低于下限")
            torrent.peers = 1
            torrent.seeders = 50
            self.assertEqual(evaluate(torrent, {})[1], "下载/做种人数比不足")
            torrent.seeders = 51
            self.assertEqual(evaluate(torrent, {})[1], "做种人数不在范围内")
            torrent.seeders = 10
            torrent.freedate = (datetime.now(timezone.utc) + timedelta(minutes=9)).isoformat()
            self.assertEqual(evaluate(torrent, {})[1], "促销剩余时间不足")
            torrent.freedate = (datetime.now(timezone.utc) + timedelta(minutes=11)).isoformat()
            self.assertTrue(evaluate(torrent, {})[0])

    def test_station_candidate_queue_prefers_fresh_demand_over_newest_or_old_ratio(self):
        from datetime import timedelta, timezone
        task = self.add_task(1)
        now = datetime.now(timezone.utc)
        rows = self.candidates["1"]
        for torrent, title, peers, seeds, age in zip(
            rows, ("newest-weak", "fresh-demand", "old-high-ratio"),
            (1, 20, 100), (10, 5, 1), (1, 30, 7 * 1440),
        ):
            torrent.title, torrent.peers, torrent.seeders = title, peers, seeds
            torrent.pubdate = (now - timedelta(minutes=age)).isoformat()
        self.plugin._task_context.candidates = {}
        report = self.plugin._new_run_report("brush")
        with self.plugin._task_scope(task.id):
            self.plugin._BrushFlow__brush_site_torrents(
                SimpleNamespace(id=1, name="one", domain="1"), {}, {}, set(), report, 0,
            )
        self.assertEqual(report["added_titles"], ["fresh-demand"])

    def test_publication_filter_rejects_unknown_and_respects_explicit_timezone(self):
        from datetime import timedelta, timezone
        task = self.add_task(1)
        task.pubtime, task.timezone_offset = "0-120", 8
        evaluate = self.cls._BrushFlow__evaluate_conditions_for_brush.__get__(self.plugin)
        torrent = SimpleNamespace(title="sample", description="", site_name="one", page_url=None,
                                  size=100, downloadvolumefactor=0, uploadvolumefactor=1,
                                  hit_and_run=False, seeders=10, peers=1, freedate=None,
                                  pubdate="invalid")
        with self.plugin._task_scope(task.id):
            self.assertEqual(evaluate(torrent, {})[1], "发布时间未知")
            published = datetime.now(timezone.utc) - timedelta(minutes=60)
            for value in (published.isoformat(), published.astimezone(timezone(timedelta(hours=8))).isoformat()):
                torrent.pubdate = value
                self.assertTrue(evaluate(torrent, {})[0])

    def test_unavailable_downloader_keeps_pending_without_blocking_other_sites(self):
        unavailable = self.add_task(1)
        healthy = self.add_task(2, downloader="qb")
        self.services["tb"].instance.get_torrents = lambda: (None, True)
        self.queue(unavailable, healthy)
        self.plugin._dispatch_brush()
        self.assertEqual(self.added, ["2"] * 3)
        self.assertIn("1", self.plugin._pending_brush)

    def test_global_save_preserves_overrides_and_apply_all_clears_only_shared_fields(self):
        task = self.add_task(1)
        task.novice = True
        task.site_skip_tips = True
        self.plugin._show_sidebar_nav = True
        self.plugin._global_proxy_delete = False
        self.plugin._global_delete_size_range = None
        self.plugin._save_config = Mock()
        self.plugin._build_status_data = lambda: {}
        self.ns["schemas"] = SimpleNamespace(Response=lambda **values: SimpleNamespace(**values))
        settings_model = self.ns["BrushFlowSettingsPayload"]
        payload = settings_model(global_defaults={"downloader": "qb", "leecher": "8"})
        self.assertTrue(self.plugin.update_settings(payload).success)
        updated = self.plugin._task_configs["1"]
        self.assertEqual(updated.downloader, "tb")
        self.assertEqual(updated.leecher, "8")
        payload = settings_model(global_defaults={"downloader": "qb", "leecher": "9"}, apply_defaults_to_existing=True)
        self.assertTrue(self.plugin.update_settings(payload).success)
        updated = self.plugin._task_configs["1"]
        self.assertEqual(updated.downloader, "qb")
        self.assertEqual(updated.override_fields, [])
        self.assertTrue(updated.novice)
        self.assertTrue(updated.site_skip_tips)
        self.assertEqual(updated.tag, "site-1")

    def test_settings_do_not_mutate_tasks_while_a_round_is_running(self):
        task = self.add_task(1)
        self.ns["schemas"] = SimpleNamespace(Response=lambda **values: SimpleNamespace(**values))
        self.plugin._brush_lock.acquire()
        try:
            response = self.plugin.update_settings(self.ns["BrushFlowSettingsPayload"]())
            self.assertFalse(response.success)
            self.assertIs(self.plugin._task_configs["1"], task)
        finally:
            self.plugin._brush_lock.release()

    def test_new_promotion_check_is_scheduled_before_next_site_is_added(self):
        first, second = self.add_task(1), self.add_task(2)
        self.candidates["1"] = self.candidates["1"][:1]
        self.candidates["2"] = self.candidates["2"][:1]
        expiry = datetime.now().astimezone()
        self.plugin._next_promotion_expiry = lambda task: expiry if task.id in self.added else None
        order = []
        download = self.plugin._BrushFlow__download
        self.plugin._refresh_scheduler = lambda: order.append("schedule")
        def record_download(torrent):
            order.append(self.plugin._get_task_config().id)
            return download(torrent)
        self.plugin._BrushFlow__download = record_download
        self.queue(first, second)
        self.plugin._dispatch_brush()
        self.assertEqual(order[:3], ["1", "schedule", "2"])

    def test_global_downloader_change_cannot_orphan_existing_torrents(self):
        self.add_task(1)
        self.plugin._save_task_data("1", "torrents", {"managed": {"size": 100, "deleted": False}})
        self.ns["schemas"] = SimpleNamespace(Response=lambda **values: SimpleNamespace(**values))
        payload = self.ns["BrushFlowSettingsPayload"](global_defaults={"downloader": "qb"}, apply_defaults_to_existing=True)
        response = self.plugin.update_settings(payload)
        self.assertFalse(response.success)
        self.assertEqual(self.plugin._task_configs["1"].downloader, "tb")
        self.assertEqual(self.plugin._calculate_global_seeding_size(), 100)


class BrushFlowRatioRotationTests(unittest.TestCase):
    setUp = BrushFlowTests.setUp
    add_task = BrushFlowTests.add_task
    queue = BrushFlowTests.queue

    def test_verified_low_ratio_preempts_unknown_novice_even_during_retry(self):
        p = self.plugin
        p._rotation_mode = "sequential"
        self.add_task(1).novice = True
        low = self.add_task(2)
        p._build_site_ratio_status = lambda task, *args: {
            "available": task.site_id == 2, "reached": False, "deficit_fraction": .885}
        for retry_at in (None, 700):
            self.data["site_rotation"] = {"site_id": 1, "phase": "ratio", "started_at": 10,
                                          "retry_at": retry_at, "health_checked": True}
            self.assertEqual(p._select_rotation_site({}, 100), 2)
            self.assertIsNone(self.data["site_rotation"].get("retry_at"))
        self.assertLess(p._priority_tier(low, {"available": True, "reached": False}),
                        p._priority_tier(p._task_configs["1"], {"available": False}))

    def test_stale_novice_does_not_beat_verified_low_ratio(self):
        p = self.plugin
        p._rotation_mode = "sequential"
        self.add_task(1).novice = True
        self.add_task(2)
        p._build_site_ratio_status = lambda task, *args: {
            "available": True, "stale": task.site_id == 1, "reached": False}
        self.assertEqual(p._select_rotation_site({}, 100), 2)

    def test_verified_novice_still_precedes_other_unmet_sites(self):
        p = self.plugin
        p._rotation_mode = "sequential"
        self.add_task(1).novice = True
        self.add_task(2)
        p._build_site_ratio_status = lambda task, *args: {
            "available": True, "reached": False, "deficit_fraction": .9 if task.site_id == 2 else .2}
        self.assertEqual(p._select_rotation_site({}, 100), 1)

    def test_failed_site_does_not_reclaim_unknown_focus_after_cooldown(self):
        p = self.plugin
        p._rotation_mode = "sequential"
        for site_id in (1, 2, 3):
            self.add_task(site_id)
        available = {1}
        reached = set()
        p._build_site_ratio_status = lambda task, *args: {
            "available": task.site_id in available, "reached": task.site_id in reached,
            "deficit_fraction": .9 if task.site_id == 1 else .5}
        p._select_rotation_site({}, 100)
        self.site_test.return_value = (False, "")
        for now in range(100, 3101, 600):
            p._check_rotation_site(now)
        self.assertEqual(p._select_rotation_site({}, 3101), 2)
        self.site_test.return_value = (True, "")
        self.assertTrue(p._check_rotation_site(3101))
        self.assertEqual(p._select_rotation_site({}, 3700), 2)
        self.assertEqual(p._select_rotation_site({}, 4300), 2)
        available.add(3)
        self.assertEqual(p._select_rotation_site({}, 4301), 3)
        reached.add(3)
        self.assertEqual(p._select_rotation_site({}, 4302), 1)
        self.assertNotIn("1", self.data["site_rotation"]["retry_cooldowns"])

    def test_failed_site_does_not_interrupt_healthy_round_robin_dwell(self):
        p = self.plugin
        p._rotation_mode = "sequential"
        self.add_task(1)
        self.add_task(2)
        p._build_site_ratio_status = lambda task, *args: {
            "available": True, "reached": task.site_id == 2, "deficit_fraction": .9}
        self.data["site_rotation"] = {"site_id": 2, "phase": "round_robin", "started_at": 100,
                                      "deadline_at": 5000, "retry_cooldowns": {"1": 700}}
        self.assertEqual(p._select_rotation_site({}, 701), 2)
        self.assertEqual(p._select_rotation_site({}, 5000), 1)

    def test_failed_account_focus_yields_despite_retained_failure_markers(self):
        p = self.plugin
        p._rotation_mode = "sequential"
        self.add_task(1)
        self.add_task(2)
        p._build_site_ratio_status = lambda task, *args: {
            "available": task.site_id == 1, "reached": False}
        self.data["site_rotation"] = {"site_id": 2, "phase": "ratio", "started_at": 100,
                                      "retry_cooldowns": {"1": 700}}
        self.data["ratio_refresh.2"] = {"success": False, "failures": 6, "next_attempt": 5000}
        self.assertEqual(p._select_rotation_site({}, 1000), 1)

    def test_retrying_selected_site_remains_focus_in_status_only(self):
        p = self.plugin
        p._rotation_mode = "sequential"
        self.add_task(1)
        self.add_task(2)
        self.data["site_rotation"] = {"site_id": 1, "phase": "ratio", "retry_at": 700}
        p._get_site_name = lambda site_id: str(site_id)
        p._get_statistic_info = lambda task_id: {}
        self.assertFalse(p._is_focus_site(1))
        self.assertTrue(p._task_summary("1")["is_focus"])
        self.assertFalse(p._task_summary("2")["is_focus"])

    def test_unmet_priority_then_configured_round_robin_preserves_records(self):
        p = self.plugin
        p._rotation_mode = "sequential"
        tasks = [self.add_task(i) for i in (1, 2, 3)]
        tasks[2].novice = True
        met = set()
        p._build_site_ratio_status = lambda task, *args: {
            "available": True, "reached": task.site_id in met, "deficit_fraction": task.site_id / 10}
        self.data["task.3.torrents"] = {"retained": {"deleted": False, "uploaded": 10}}
        self.assertEqual(p._select_rotation_site({}, 100), 3)
        self.assertIsNone(self.data["site_rotation"]["deadline_at"])
        self.assertEqual(p._select_rotation_site({}, 100000), 3)
        met.add(3)
        self.assertEqual(p._select_rotation_site({}, 100001), 2)
        met.add(2)
        self.assertEqual(p._select_rotation_site({}, 100002), 1)
        met.add(1)
        self.assertEqual(p._select_rotation_site({}, 100003), 1)
        self.assertEqual(self.data["site_rotation"]["phase"], "round_robin")
        self.assertEqual(p._select_rotation_site({}, 100003 + 86400), 2)
        self.assertEqual(p._select_rotation_site({}, 100003 + 172800), 3)
        self.assertEqual(self.data["task.3.torrents"], {"retained": {"deleted": False, "uploaded": 10}})
        met.remove(2)
        self.assertEqual(p._select_rotation_site({}, 100003 + 172801), 2)

    def test_unknown_or_stale_account_is_not_treated_as_reached(self):
        p = self.plugin
        p._rotation_mode = "sequential"
        self.add_task(1)
        self.add_task(2)
        p._build_site_ratio_status = lambda task, *args: {
            "available": True, "reached": True, "stale": task.site_id == 2}
        self.assertEqual(p._select_rotation_site({}, 100), 2)
        self.assertEqual(self.data["site_rotation"]["phase"], "ratio")

    def test_same_site_different_targets_do_not_restart_focus(self):
        p = self.plugin
        p._rotation_mode = "sequential"
        self.add_task(1)
        self.add_task(2, site=1)
        p._build_site_ratio_status = lambda task, *args: {"available": True, "reached": task.id == "1"}
        p._select_rotation_site({}, 100)
        p._select_rotation_site({}, 200)
        self.assertEqual(self.data["site_rotation"]["started_at"], 100)

    def test_refresh_is_background_rate_limited_and_failed_data_is_unknown(self):
        p = self.plugin
        p._rotation_mode = "sequential"
        task = self.add_task(1)
        self.data["site_rotation"] = {"site_id": 1, "phase": "ratio", "health_checked": True}
        jobs = []
        self.ns["ThreadHelper"] = lambda: SimpleNamespace(submit=lambda job: jobs.append(job))
        self.ns["SitesHelper"] = lambda: SimpleNamespace(get_indexer=lambda domain: {"id": 1})
        fetch = Mock(return_value=SimpleNamespace(userid=1, err_msg="unavailable"))
        self.ns["SiteChain"] = lambda: SimpleNamespace(refresh_userdata=fetch)
        with patch.object(self.ns["time"], "time", return_value=100):
            p._refresh_focus_ratio()
            p._refresh_focus_ratio()
            self.assertEqual(len(jobs), 1)
            fetch.assert_not_called()
            jobs.pop()()
            self.assertFalse(self.data["ratio_refresh.1"]["success"])
            self.assertEqual(self.data["ratio_refresh.1"]["next_attempt"], 700)
            status = self.cls._build_site_ratio_status(p, task, {})
            self.assertFalse(status["available"])
            self.assertFalse(status["reached"])
        fetch.return_value = SimpleNamespace(userid=1, err_msg=None, ratio=1, upload=100, download=100)
        with patch.object(self.ns["time"], "time", return_value=700):
            p._refresh_focus_ratio()
            jobs.pop()()
            self.assertTrue(self.data["ratio_refresh.1"]["success"])
            self.assertEqual(self.data["ratio_refresh.1"]["next_attempt"], 2500)

    def test_repeated_account_failure_skips_to_another_unmet_site(self):
        p = self.plugin
        p._rotation_mode = "sequential"
        self.add_task(1).novice = True
        self.add_task(2)
        p._select_rotation_site({}, 100)
        self.data["ratio_refresh.1"] = {"success": False, "failures": 6, "next_attempt": 4000}
        self.assertEqual(p._select_rotation_site({}, 1000), 2)

    def test_incomplete_account_data_retries_five_times_then_skips(self):
        p = self.plugin
        p._rotation_mode = "sequential"
        self.add_task(1)
        self.add_task(2)
        self.data["site_rotation"] = {"site_id": 1, "phase": "ratio", "health_checked": True}
        self.ns["ThreadHelper"] = lambda: SimpleNamespace(submit=lambda job: job())
        self.ns["SitesHelper"] = lambda: SimpleNamespace(get_indexer=lambda domain: {"id": 1})
        fetch = Mock(return_value=SimpleNamespace(userid=1, err_msg=None, ratio=0, upload=0, download=0))
        self.ns["SiteChain"] = lambda: SimpleNamespace(refresh_userdata=fetch)
        for attempt in range(6):
            now = 100 + attempt * 600
            with patch.object(self.ns["time"], "time", return_value=now):
                p._refresh_focus_ratio()
                p._refresh_focus_ratio()
            refresh = self.data["ratio_refresh.1"]
            self.assertFalse(refresh["success"])
            self.assertEqual(fetch.call_count, attempt + 1)
            self.assertEqual(refresh["failures"], attempt + 1)
            self.assertEqual(refresh["next_attempt"], now + (3600 if attempt == 5 else 600))
            self.assertEqual(p._select_rotation_site({}, now), 2 if attempt == 5 else 1)

    def test_account_refresh_accepts_same_valid_values_as_ratio_status(self):
        for values in ({"ratio": 2}, {"ratio": 0, "upload": 0, "download": 100},
                       {"ratio": None, "upload": 100, "download": 0}):
            with self.subTest(values=values):
                self.setUp()
                p = self.plugin
                p._rotation_mode = "sequential"
                self.add_task(1)
                self.data["site_rotation"] = {"site_id": 1, "phase": "ratio", "health_checked": True}
                self.ns["ThreadHelper"] = lambda: SimpleNamespace(submit=lambda job: job())
                self.ns["SitesHelper"] = lambda: SimpleNamespace(get_indexer=lambda domain: {"id": 1})
                self.ns["SiteChain"] = lambda: SimpleNamespace(refresh_userdata=lambda **kwargs:
                                                               SimpleNamespace(userid=1, err_msg=None, **values))
                with patch.object(self.ns["time"], "time", return_value=100):
                    p._refresh_focus_ratio()
                self.assertTrue(self.data["ratio_refresh.1"]["success"])

    def test_unknown_account_bytes_do_not_become_unlimited_or_zero_ratio(self):
        for upload, download in ((100, None), (100, "invalid"), (100, -1), (100, False),
                                 (None, 100), ("invalid", 100), (-1, 100), (True, 0)):
            for ratio in (None, 0):
                with self.subTest(upload=upload, download=download, ratio=ratio):
                    data = SimpleNamespace(ratio=ratio, upload=upload, download=download)
                    self.assertIsNone(self.cls._site_ratio_values(data))
        for ratio in (True, False):
            with self.subTest(ratio=ratio):
                self.assertIsNone(self.cls._site_ratio_values(SimpleNamespace(ratio=ratio)))

    def test_known_ratio_with_missing_bytes_preserves_priority_without_download_budget(self):
        task = self.add_task(1)
        self.ns["StringUtils"] = SimpleNamespace(get_url_domain=lambda value: value)
        self.plugin._build_site_ratio_status = self.cls._build_site_ratio_status.__get__(self.plugin)
        for ratio in (1, 3):
            for upload, download in ((100, None), (None, 100)):
                with self.subTest(ratio=ratio, upload=upload, download=download):
                    row = SimpleNamespace(ratio=ratio, upload=upload, download=download)
                    self.plugin._task_context.site_user_data = {"1": row}
                    status = self.plugin._build_site_ratio_status(task, {"1": row})
                    self.assertTrue(status["available"])
                    self.assertFalse(status["unlimited"])
                    self.assertEqual(status["current"], ratio)
                    self.assertEqual(status["reached"], ratio >= task.site_ratio_target)
                    self.assertIsNone(status["deficit_bytes"])
                    self.assertIsNone(status["download_headroom"])
                    self.assertEqual(self.plugin._priority_tier(task, status), 1 if ratio == 1 else 2)
                    task.freeleech = ""
                    torrent = SimpleNamespace(title="sample", site_name="one", page_url=None,
                                              size=1, downloadvolumefactor=1)
                    with self.plugin._task_scope(task.id):
                        allowed, reason = self.cls._BrushFlow__evaluate_conditions_for_brush(self.plugin, torrent, {})
                    self.assertFalse(allowed)
                    self.assertIn("仅选择免费种子", reason)

    def test_focus_source_failure_starts_five_retries_then_skips(self):
        p = self.plugin
        p._rotation_mode = "sequential"
        task = self.add_task(1)
        self.add_task(2)
        p._refresh_focus_ratio = Mock()
        self.data["site_rotation"] = {"site_id": 1, "phase": "ratio", "health_checked": True}
        self.candidates["1"] = []
        self.site_test.return_value = (False, "private")
        self.queue(task)
        with patch.object(self.ns["time"], "time", return_value=100):
            p._dispatch_brush()
        state = self.data["site_rotation"]
        self.assertFalse(state["health_checked"])
        self.assertEqual(state["retry_count"], 0)
        self.assertEqual(state["retry_at"], 700)
        self.assertFalse(p._check_rotation_site(699))
        self.assertEqual(self.site_test.call_count, 1)
        for attempt in range(1, 6):
            self.assertFalse(p._check_rotation_site(100 + attempt * 600))
            self.assertEqual(self.data["site_rotation"]["retry_count"], attempt)
        self.assertEqual(self.site_test.call_count, 6)
        self.assertEqual(p._select_rotation_site({}, 3100), 2)

    def test_source_failure_does_not_overwrite_other_focus_or_pending_retry(self):
        task = self.add_task(1)
        self.candidates["1"] = []
        self.site_test.return_value = (False, "private")
        for mode, state in (
            ("parallel", {"site_id": 1, "health_checked": True}),
            ("sequential", {"site_id": 2, "health_checked": True}),
            ("sequential", {"site_id": 1, "health_checked": False, "retry_at": 700, "retry_count": 3}),
        ):
            with self.subTest(mode=mode, state=state):
                self.plugin._rotation_mode = mode
                self.data["site_rotation"] = copy.deepcopy(state)
                self.plugin._task_context.candidates = {}
                with self.plugin._task_scope(task.id):
                    self.plugin._BrushFlow__brush_site_torrents(
                        SimpleNamespace(name="one", domain="1"), {}, {}, set(),
                        self.plugin._new_run_report("brush"), 0,
                    )
                self.assertEqual(self.data["site_rotation"], state)

    def test_empty_source_distinguishes_connection_failure_from_empty_results(self):
        for healthy in (True, False, None):
            with self.subTest(healthy=healthy):
                self.setUp()
                task = self.add_task(1)
                self.candidates["1"] = []
                self.site_test.return_value = (healthy, "private-token-do-not-propagate")
                if healthy is None:
                    self.site_test.side_effect = RuntimeError("private-url-do-not-propagate")
                self.queue(task)
                self.plugin._dispatch_brush()
                report = self.plugin._get_task_data(task.id, "runs")[0]
                self.assertEqual(report["success"], bool(healthy))
                self.assertEqual(report["result"], "no_candidates" if healthy else "source_unavailable")
                self.assertNotIn("private", str(report))
                self.assertTrue(report["reason_counts"])


class BrushFlowRuntimeTests(unittest.TestCase):
    setUp = BrushFlowTests.setUp
    add_task = BrushFlowTests.add_task

    def budget_client(self, ceiling=8):
        if "1" not in self.plugin._task_configs:
            self.add_task(1)
        client = Mock()
        session = SimpleNamespace(download_queue_enabled=True, download_queue_size=ceiling,
                                  speed_limit_up_enabled=True, speed_limit_up=2500,
                                  speed_limit_down_enabled=True, speed_limit_down=10000,
                                  queue_stalled_enabled=False)
        client.get_session.return_value = session
        client.set_session.side_effect = lambda **values: [setattr(session, key, value) for key, value in values.items()]
        self.services["tb"].instance.trc = client
        rows = [SimpleNamespace(hashString=f"owned-{i}", labels=["site-1"], queue_position=i,
                                status="downloading" if i < ceiling else "download pending")
                for i in range(10)]
        self.plugin._rotation_mode = "parallel"
        return client, session, rows

    def configure_settings_api(self):
        self.plugin._global_proxy_delete = False
        self.plugin._global_delete_size_range = None
        self.plugin._save_config = Mock()
        self.plugin._build_status_data = lambda: {}
        self.ns["schemas"] = SimpleNamespace(Response=lambda **values: SimpleNamespace(**values))

    def test_initial_owned_download_budget_never_exceeds_user_ceiling(self):
        for ceiling in (1, 3, 8):
            with self.subTest(ceiling=ceiling):
                self.data.clear()
                client, session, rows = self.budget_client(ceiling)
                self.plugin._adjust_download_budget(client, rows, "tb")
                self.assertGreaterEqual(session.download_queue_size, 1)
                self.assertLessEqual(session.download_queue_size, ceiling)
                self.assertEqual(self.data["download_budget.tb"]["ceiling"], ceiling)
                self.assertEqual(session.download_queue_size, min(ceiling, 4))

    def test_sequential_budget_respects_ceiling_without_stopping_or_starting_torrents(self):
        self.add_task(2)
        for ceiling in (1, 3, 8):
            with self.subTest(ceiling=ceiling):
                self.data.clear()
                client, session, rows = self.budget_client(ceiling)
                rows[0].labels = ["site-2"]
                original_states = [(row.hashString, row.status) for row in rows]
                client.get_torrents.return_value = rows
                self.plugin._rotation_mode = "sequential"
                self.data["site_rotation"] = {"site_id": 1, "health_checked": True}
                self.plugin._prioritize_download_queue({})
                self.assertEqual(session.download_queue_size, min(ceiling, 4))
                self.assertLessEqual(session.download_queue_size, ceiling)
                self.assertEqual(self.data["download_budget.tb"]["ceiling"], ceiling)
                self.assertEqual(self.data["download_slots"]["budget"], min(ceiling, 4))
                self.assertEqual(self.data["download_slots"]["limit"], ceiling)
                client.stop_torrent.assert_not_called()
                client.start_torrent.assert_not_called()
                self.assertEqual([(row.hashString, row.status) for row in rows], original_states)

    def test_foreign_torrent_restores_user_ceiling_and_releases_budget(self):
        client, session, rows = self.budget_client()
        self.plugin._adjust_download_budget(client, rows, "tb")
        self.assertEqual(session.download_queue_size, 4)
        rows.append(SimpleNamespace(hashString="normal-download", labels=[], status="download pending"))
        self.plugin._adjust_download_budget(client, rows, "tb")
        self.assertEqual(session.download_queue_size, 8)
        self.assertFalse(self.data["download_budget.tb"])
        self.assertFalse(self.data["download_slots"])
        client.set_session.reset_mock()
        self.plugin._adjust_download_budget(client, rows, "tb")
        client.set_session.assert_not_called()

    def test_deleted_hash_without_labels_does_not_claim_download_budget(self):
        client, session, rows = self.budget_client()
        self.data["task.1.torrents"] = {"manual-readd": {"deleted": True}}
        rows.append(SimpleNamespace(hashString="manual-readd", labels=[], status="download pending"))
        self.plugin._adjust_download_budget(client, rows, "tb")
        client.set_session.assert_not_called()
        self.assertEqual(session.download_queue_size, 8)
        self.assertFalse(self.data.get("download_budget.tb"))

    def test_deleted_hash_returning_as_foreign_releases_download_budget(self):
        client, session, rows = self.budget_client()
        self.plugin._adjust_download_budget(client, rows, "tb")
        self.assertEqual(session.download_queue_size, 4)
        self.data["task.1.torrents"] = {"manual-readd": {"deleted": True}}
        rows.append(SimpleNamespace(hashString="manual-readd", labels=[], status="download pending"))
        self.plugin._adjust_download_budget(client, rows, "tb")
        self.assertEqual(session.download_queue_size, 8)
        self.assertFalse(self.data["download_budget.tb"])
        self.assertFalse(self.data["download_slots"])

    def test_deleted_hash_is_prioritized_only_when_currently_tagged(self):
        for downloader in ("tb", "qb"):
            for tagged in (False, True):
                with self.subTest(downloader=downloader, tagged=tagged):
                    self.setUp()
                    self.add_task(1, downloader=downloader)
                    self.data["task.1.torrents"] = {"manual-readd": {"deleted": True}}
                    client = Mock()
                    if downloader == "tb":
                        self.services[downloader].instance.trc = client
                        client.get_torrents.return_value = [
                            SimpleNamespace(hashString="unrelated", queue_position=0, labels=[], status="download pending"),
                            SimpleNamespace(hashString="manual-readd", queue_position=1,
                                            labels=["site-1"] if tagged else [], status="download pending")]
                        client.get_session.return_value = SimpleNamespace(download_queue_enabled=True, download_queue_size=8)
                        move = client.queue_top
                    else:
                        self.services[downloader].instance.qbc = client
                        client.torrents_info.return_value = [
                            {"hash": "unrelated", "priority": 0, "tags": "", "state": "queuedDL"},
                            {"hash": "manual-readd", "priority": 1, "tags": "site-1" if tagged else "", "state": "queuedDL"}]
                        move = client.torrents_top_priority
                    self.plugin._prioritize_download_queue({})
                    if tagged:
                        self.assertEqual(move.call_count, 1)
                    else:
                        move.assert_not_called()
                    client.stop_torrent.assert_not_called()
                    client.start_torrent.assert_not_called()

    def test_deleted_hash_history_does_not_override_explicit_current_site(self):
        client, session, _ = self.budget_client()
        self.add_task(2)
        self.data["task.1.torrents"] = {"shared": {"deleted": True}}
        row = SimpleNamespace(hashString="shared", queue_position=0, labels=["site-2"], status="downloading")
        allocate = Mock(return_value=["shared"])
        with patch.dict(self.ns, {"allocate_downloads": allocate}):
            self.plugin._rebalance_transmission_downloads(client, [row], {"shared": (1, 0)}, "tb")
        self.assertEqual(allocate.call_args.args[0][0]["site_id"], 2)
        client.stop_torrent.assert_not_called()
        client.start_torrent.assert_not_called()

    def test_manual_queue_limit_replaces_old_ceiling_and_is_not_restored_over(self):
        for mode in ("parallel", "sequential"):
            with self.subTest(mode=mode):
                self.setUp()
                client, session, rows = self.budget_client()
                self.plugin._rotation_mode = mode
                self.plugin._adjust_download_budget(client, rows, "tb")
                session.download_queue_size = 3
                self.plugin._adjust_download_budget(client, rows, "tb")
                self.assertEqual(self.data["download_budget.tb"]["ceiling"], 3)
                self.assertEqual(session.download_queue_size, 3)
                session.download_queue_size = 6
                client.set_session.reset_mock()
                self.plugin._restore_download_budgets()
                client.set_session.assert_not_called()
                self.assertEqual(session.download_queue_size, 6)
                self.assertFalse(self.data["download_budget.tb"])

    def test_stop_restores_owned_budget(self):
        client, session, rows = self.budget_client()
        self.plugin._adjust_download_budget(client, rows, "tb")
        self.plugin.stop_service()
        self.assertEqual(session.download_queue_size, 8)
        self.assertTrue(self.plugin._stopping)
        self.assertFalse(self.data["download_budget.tb"])
        self.assertFalse(self.data["download_slots"])

    def test_restore_failure_retains_budget_until_downloader_recovers(self):
        for failure in ("service", "instance", "client", "session_read", "session_write"):
            with self.subTest(failure=failure):
                self.setUp()
                client, session, rows = self.budget_client()
                self.plugin._adjust_download_budget(client, rows, "tb")
                self.assertEqual(session.download_queue_size, 4)
                budget = copy.deepcopy(self.data["download_budget.tb"])
                service = self.services["tb"]
                instance = service.instance
                write_session = client.set_session.side_effect
                if failure == "service":
                    self.services["tb"] = None
                elif failure == "instance":
                    service.instance = None
                elif failure == "client":
                    del service.instance.trc
                elif failure == "session_read":
                    client.get_session.side_effect = RuntimeError("unavailable")
                else:
                    client.set_session.side_effect = RuntimeError("unavailable")
                self.plugin._restore_download_budgets()
                self.assertEqual(self.data["download_budget.tb"], budget)
                self.assertEqual(self.data["download_budget.tb"]["ceiling"], 8)
                self.assertEqual(self.data["download_budget.tb"]["applied"], 4)
                self.assertEqual(session.download_queue_size, 4)
                self.services["tb"] = service
                service.instance = instance
                service.instance.trc = client
                client.get_session.side_effect = None
                client.set_session.side_effect = write_session
                self.plugin._restore_download_budgets()
                self.assertEqual(session.download_queue_size, 8)
                self.assertFalse(self.data["download_budget.tb"])
                self.assertFalse(self.data["download_slots"])

    def test_disabling_restores_owned_budget_in_both_modes(self):
        for mode in ("parallel", "sequential"):
            with self.subTest(mode=mode):
                self.setUp()
                client, session, rows = self.budget_client()
                self.plugin._rotation_mode = mode
                self.plugin._adjust_download_budget(client, rows, "tb")
                self.assertEqual(session.download_queue_size, 4)
                self.configure_settings_api()
                response = self.plugin.update_settings(self.ns["BrushFlowSettingsPayload"](enabled=False))
                self.assertTrue(response.success)
                self.assertEqual(session.download_queue_size, 8)
                self.assertFalse(self.data["download_budget.tb"])
                self.assertFalse(self.data["download_slots"])

    def test_switching_to_sequential_keeps_owned_budget(self):
        client, session, rows = self.budget_client()
        self.plugin._adjust_download_budget(client, rows, "tb")
        budget = copy.deepcopy(self.data["download_budget.tb"])
        client.set_session.reset_mock()
        self.configure_settings_api()
        response = self.plugin.update_settings(self.ns["BrushFlowSettingsPayload"](
            enabled=True, rotation_mode="sequential"))
        self.assertTrue(response.success)
        self.assertEqual(self.plugin._rotation_mode, "sequential")
        self.assertEqual(session.download_queue_size, 4)
        self.assertEqual(self.data["download_budget.tb"], budget)
        client.set_session.assert_not_called()
        client.stop_torrent.assert_not_called()
        client.start_torrent.assert_not_called()

    def test_reenabling_sequential_uses_current_user_ceiling(self):
        client, session, rows = self.budget_client()
        self.plugin._rotation_mode = "sequential"
        self.plugin._adjust_download_budget(client, rows, "tb")
        self.configure_settings_api()
        payload = self.ns["BrushFlowSettingsPayload"]
        self.assertTrue(self.plugin.update_settings(payload(enabled=False)).success)
        self.assertEqual(session.download_queue_size, 8)
        session.download_queue_size = 3
        self.assertTrue(self.plugin.update_settings(payload(enabled=True)).success)
        self.plugin._adjust_download_budget(client, rows, "tb")
        self.assertEqual(self.plugin._rotation_mode, "sequential")
        self.assertEqual(self.data["download_budget.tb"]["ceiling"], 3)
        self.assertEqual(session.download_queue_size, 3)
        client.stop_torrent.assert_not_called()
        client.start_torrent.assert_not_called()

    def test_nullable_counters_remain_unknown_through_downloader_adapter(self):
        for downloader in ("qb", "tb"):
            with self.subTest(downloader=downloader):
                self.setUp()
                task = self.add_task(1, downloader=downloader)
                self.data["task.1.torrents"] = {"unknown": {"deleted": False}}
                row = ({"hash": "unknown", "state": "downloading", "progress": 0}
                       if downloader == "qb" else SimpleNamespace(
                           hashString="unknown", status="downloading", percent_done=0))
                self.rows[downloader] = [row]
                with self.plugin._task_scope(task.id):
                    info = self.plugin._BrushFlow__get_torrent_info(row)
                self.assertEqual(info["uploaded"], 0)
                self.assertIsNone(info["sample_uploaded"])
                self.assertIsNone(info["sample_downloaded"])
                for at in (1000, 1030):
                    with patch.object(self.ns["time"], "time", return_value=at):
                        self.plugin._sample_transfers()
                metrics = self.data["task.1.metrics"]
                self.assertTrue(metrics["partial"])
                self.assertTrue(metrics["gap"])
                self.assertTrue(self.data["metrics"]["partial"])

    def test_locked_task_marks_global_window_partial(self):
        self.add_task(1)
        self.add_task(2, downloader="qb")
        self.data["task.2.torrents"] = {"known": {"deleted": False}}
        self.rows["qb"] = [{"hash": "known", "state": "uploading", "progress": 1,
                            "uploaded": 100, "downloaded": 1000, "upspeed": 1200}]
        with patch.object(self.ns["time"], "time", return_value=1000):
            self.plugin._sample_transfers()
        self.data["task.1.metrics"] = {"sampled_at": 1000, "seeding": 17, "upspeed": 300000}
        self.rows["qb"][0]["uploaded"] = 600
        lock = threading.Lock()
        self.plugin._task_locks["1"] = lock
        lock.acquire()
        try:
            with patch.object(self.ns["time"], "time", return_value=1030):
                self.plugin._sample_transfers()
        finally:
            lock.release()
        self.assertTrue(self.data["metrics"]["partial"])
        self.assertTrue(self.data["metrics"]["gap"])
        self.assertEqual(self.data["metrics"]["uploaded"], 500)
        self.assertEqual(self.data["task.2.metrics"]["seeding"], 1)
        self.assertEqual(self.data["task.1.metrics"]["sampled_at"], 1030)
        for key in ("upspeed", "dlspeed", "downloading", "queued", "seeding", "paused"):
            self.assertIsNone(self.data["metrics"][key])
            self.assertIsNone(self.data["task.1.metrics"][key])

    def test_unavailable_downloader_reports_unknown_current_counts(self):
        self.add_task(1)
        self.plugin._read_downloads = lambda **kwargs: {}
        with patch.object(self.ns["time"], "time", return_value=1030):
            self.plugin._sample_transfers()
        for metrics in (self.data["metrics"], self.data["task.1.metrics"]):
            self.assertTrue(metrics["gap"])
            self.assertEqual(metrics["sampled_at"], 1030)
            for key in ("upspeed", "dlspeed", "downloading", "queued", "seeding", "paused"):
                self.assertIsNone(metrics[key])

    def test_first_transfer_sample_preserves_known_current_counts(self):
        self.add_task(1, downloader="qb")
        self.data["task.1.torrents"] = {"new": {"deleted": False}}
        self.rows["qb"] = [{"hash": "new", "state": "uploading", "progress": 1,
                            "uploaded": 100, "downloaded": 1000, "upspeed": 1200}]
        self.plugin._sample_transfers()
        for metrics in (self.data["metrics"], self.data["task.1.metrics"]):
            self.assertTrue(metrics["partial"])
            self.assertFalse(metrics["gap"])
            self.assertEqual(metrics["seeding"], 1)
            self.assertEqual(metrics["upspeed"], 1200)
            self.assertEqual(metrics["downloading"], 0)
            self.assertEqual(metrics["queued"], 0)

    def test_empty_downloader_snapshot_preserves_known_zero_counts(self):
        self.add_task(1)
        self.plugin._sample_transfers()
        for metrics in (self.data["metrics"], self.data["task.1.metrics"]):
            self.assertFalse(metrics["partial"])
            self.assertFalse(metrics["gap"])
            for key in ("upspeed", "dlspeed", "downloading", "queued", "seeding", "paused"):
                self.assertEqual(metrics[key], 0)

    def test_partial_active_station_prevents_budget_growth(self):
        client, session, rows = self.budget_client()
        self.add_task(2)
        session.download_queue_size = 4
        self.data["download_budget.tb"] = {"ceiling": 8, "applied": 4, "changed_at": 100}
        self.data["task.1.metrics"] = {"seconds": 600, "uploaded": 0, "downloaded": 0,
                                       "downloading": 4, "gap": False, "partial": False}
        self.data["task.2.metrics"] = {"seconds": 600, "uploaded": 0, "downloaded": None,
                                       "queued": 2, "gap": True, "partial": True}
        with patch.object(self.ns["time"], "time", return_value=1000):
            self.plugin._adjust_download_budget(client, rows, "tb")
        client.set_session.assert_not_called()
        self.assertEqual(session.download_queue_size, 4)

    def test_locked_station_does_not_reuse_old_metrics_for_budget_growth(self):
        client, session, rows = self.budget_client()
        session.download_queue_size = 4
        self.data["download_budget.tb"] = {"ceiling": 8, "applied": 4, "changed_at": 100}
        self.data["task.1.metrics"] = {"sampled_at": 700, "seconds": 600, "uploaded": 0, "downloaded": 0,
                                       "downloading": 4, "gap": False, "partial": False}
        lock = threading.Lock()
        self.plugin._task_locks["1"] = lock
        with patch.object(self.ns["time"], "time", return_value=1000):
            lock.acquire()
            try:
                self.plugin._sample_transfers()
            finally:
                lock.release()
            self.plugin._adjust_download_budget(client, rows, "tb")
        client.set_session.assert_not_called()
        self.assertEqual(session.download_queue_size, 4)

    def test_interrupted_journal_does_not_restart_ambiguous_manual_pause(self):
        client, session, rows = self.budget_client()
        rows[0].status = "stopped"
        self.data["priority_requeue.tb"] = [rows[0].hashString]
        self.plugin._rebalance_transmission_downloads(client, rows, {row.hashString: (2, 0) for row in rows}, "tb")
        client.start_torrent.assert_not_called()
        client.stop_torrent.assert_not_called()

    def test_ambiguous_paused_journal_does_not_block_other_station_allocation(self):
        self.add_task(1)
        self.add_task(2)
        client = Mock()
        client.get_session.return_value = SimpleNamespace(download_queue_enabled=True, download_queue_size=1,
                                                          queue_stalled_enabled=False)
        rows = [SimpleNamespace(hashString="low", labels=["site-1"], status="downloading", queue_position=0),
                SimpleNamespace(hashString="high", labels=["site-2"], status="download pending", queue_position=1),
                SimpleNamespace(hashString="manual", labels=["site-1"], status="stopped", queue_position=2)]
        states = {row.hashString: row.status for row in rows}

        def stop(ids):
            self.assertEqual(ids, ["low"])
            states.update(low="stopped", high="downloading")

        def start(ids, bypass_queue):
            self.assertEqual(ids, ["low"])
            self.assertFalse(bypass_queue)
            states["low"] = "download pending"

        client.stop_torrent.side_effect = stop
        client.start_torrent.side_effect = start
        client.get_torrents.side_effect = lambda ids, arguments: [
            SimpleNamespace(hashString=h, status=states[h]) for h in ids]
        self.data["priority_requeue.tb"] = ["manual"]
        self.plugin._rotation_mode = "parallel"
        with patch.object(self.ns["time"], "sleep"):
            for _ in range(2):
                self.plugin._rebalance_transmission_downloads(client, rows, {"low": (2, 0), "high": (0, 0)}, "tb")
        client.stop_torrent.assert_called_once_with(ids=["low"])
        self.assertEqual(states["manual"], "stopped")
        self.assertEqual(states["high"], "downloading")

    def test_async_stop_waits_for_auto_promotion_before_resuming_victim(self):
        self.add_task(1)
        self.add_task(2)
        client = Mock()
        client.get_session.return_value = SimpleNamespace(download_queue_enabled=True, download_queue_size=1,
                                                          queue_stalled_enabled=False)
        rows = [SimpleNamespace(hashString="low", labels=["site-1"], status="downloading", queue_position=0),
                SimpleNamespace(hashString="high", labels=["site-2"], status="download pending", queue_position=1),
                SimpleNamespace(hashString="manual", labels=["site-1"], status="stopped", queue_position=2)]
        states = {row.hashString: row.status for row in rows}
        phase = {"name": "initial", "polls": 0}
        events = []

        def stop(ids):
            self.assertEqual(ids, ["low"])
            phase.update(name="stopping", polls=0)
            events.append("stop")

        def read(ids, arguments):
            phase["polls"] += 1
            if phase["name"] == "stopping":
                if phase["polls"] >= 2:
                    states["low"] = "stopped"
                if phase["polls"] >= 3:
                    states["high"] = "downloading"
                    events.append("promoted")
            elif phase["name"] == "resuming" and phase["polls"] >= 2:
                states["low"] = "download pending"
            return [SimpleNamespace(hashString=h, status=states[h]) for h in ids]

        def start(ids, bypass_queue):
            self.assertEqual(ids, ["low"])
            self.assertFalse(bypass_queue)
            self.assertEqual(states["high"], "downloading")
            phase.update(name="resuming", polls=0)
            events.append("resume")

        client.stop_torrent.side_effect = stop
        client.get_torrents.side_effect = read
        client.start_torrent.side_effect = start
        self.plugin._rotation_mode = "parallel"
        with patch.object(self.ns["time"], "sleep") as sleep:
            self.plugin._rebalance_transmission_downloads(client, rows, {"low": (2, 0), "high": (0, 0)}, "tb")
        self.assertEqual(events, ["stop", "promoted", "resume"])
        self.assertEqual(states["low"], "download pending")
        self.assertEqual(states["manual"], "stopped")
        self.assertGreaterEqual(sleep.call_count, 3)
        self.assertFalse(self.data["priority_requeue.tb"])

    def test_async_allocation_timeout_keeps_failure_visible(self):
        client = Mock()
        client.get_torrents.return_value = [SimpleNamespace(hashString="low", status="downloading")]
        with patch.object(self.ns["time"], "sleep"):
            with self.assertRaises(RuntimeError):
                self.plugin._wait_transmission_allocation(client, ["low"], [])
        self.assertEqual(client.get_torrents.call_count, 12)


if __name__ == "__main__":
    unittest.main()
