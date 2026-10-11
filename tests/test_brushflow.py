import ast
import copy
import threading
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock


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
    }
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

    def test_nine_sites_share_five_slots_and_never_served_sites_go_first(self):
        tasks = [self.add_task(i) for i in range(1, 10)]
        self.queue(*tasks)
        self.plugin._dispatch_brush()
        self.assertEqual(self.added, ["1", "2", "3", "4", "5"])
        self.assertEqual(set(self.plugin._pending_brush), {"6", "7", "8", "9"})
        self.rows["tb"] = [SimpleNamespace(hashString=f"hash-{i}-{i}", labels=[f"site-{i}"],
                                          percent_done=0.5) for i in range(1, 6)]
        self.queue(*tasks[:5])
        self.plugin._dispatch_brush()
        self.assertEqual(len(self.added), 5)
        for torrent in self.rows["tb"]:
            torrent.percent_done = 1
        self.plugin._dispatch_brush()
        self.assertEqual(self.added[5:9], ["6", "7", "8", "9"])
        self.assertEqual(len(self.added), 10)

    def test_full_queue_retains_requests_until_completion(self):
        tasks = [self.add_task(i) for i in range(1, 7)]
        self.rows["tb"] = [SimpleNamespace(hashString=str(i), labels=[f"site-{i}"],
                                          status="download pending", percent_done=0.5) for i in range(1, 6)]
        self.queue(tasks[-1])
        self.plugin._dispatch_brush()
        self.assertEqual(self.added, [])
        self.assertIn("6", self.plugin._pending_brush)
        self.rows["tb"][0].percent_done = 1
        self.plugin._dispatch_brush()
        self.assertEqual(self.added, ["6"])

    def test_unfinished_states_and_seeding_do_not_depend_on_status_spelling(self):
        for state in ["download pending", "download_pending", "stopped", "checking", "downloading"]:
            with self.subTest(state=state):
                self.assertTrue(self.cls._unfinished_download(SimpleNamespace(status=state, percent_done=0.1)))
                self.assertFalse(self.cls._unfinished_download(SimpleNamespace(status=state, percent_done=1)))
        for state in ["queuedDL", "pausedDL", "stoppedDL", "error", "checkingDL"]:
            self.assertTrue(self.cls._unfinished_download({"state": state, "progress": 0.1}))
        self.assertFalse(self.cls._unfinished_download({"state": "stalledUP", "progress": 1}))
        self.assertTrue(self.cls._unfinished_download(SimpleNamespace(status="download pending", progress=99)))

    def test_shared_site_across_downloaders_has_one_slot(self):
        first = self.add_task(1)
        second = self.add_task(2, site=1, downloader="qb")
        other = self.add_task(3)
        self.rows["tb"] = [SimpleNamespace(hashString="a", labels=[first.brush_tag], percent_done=0.1)]
        self.queue(second, other)
        self.plugin._dispatch_brush()
        self.assertEqual(self.added, ["3"])
        self.assertIn("2", self.plugin._pending_brush)

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
        self.assertEqual(self.added, ["3"])

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
        self.assertEqual(self.added, ["2"])
        self.assertIn("1", self.plugin._pending_brush)
        lock.release()

    def test_manual_run_respects_existing_pending_priority(self):
        first, second = self.add_task(1), self.add_task(2)
        self.plugin._global_maxdlcount = 1
        self.plugin._save_task_data("1", "rotation", {"last_added": 10})
        self.queue(second)
        self.plugin.brush(first.id)
        self.assertEqual(self.added, ["2"])

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
        self.assertEqual(self.added, ["3"])

    def test_concurrent_dispatch_cannot_overfill_a_slot(self):
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
        self.assertEqual(len(self.added), 1)

    def test_scheduler_rebuild_keeps_interval_anchor(self):
        self.add_task(1)
        self.plugin._next_promotion_expiry = lambda task: None
        first = self.plugin.get_service()
        second = self.plugin.get_service()
        self.assertEqual([x["kwargs"] for x in first], [x["kwargs"] for x in second])
        self.assertEqual(first[0]["func"], self.plugin._queue_brush)
        self.assertTrue(all("start_date" in x["kwargs"] for x in first))

    def test_qb_accepted_without_hash_still_reserves_slot(self):
        tasks = [self.add_task(i, downloader="qb") for i in range(1, 3)]
        self.plugin._global_maxdlcount = 1
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
        self.assertIn("2", self.plugin._pending_brush)
        self.assertGreater(self.plugin._site_last_added(1), 0)


if __name__ == "__main__":
    unittest.main()
