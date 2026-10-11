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
            "available": True, "reached": task.id != "2",
        }
        self.queue(*tasks)
        self.plugin._dispatch_brush()
        self.assertEqual(self.added, ["3", "2", "1", "4"] * 3)

    def test_daily_rotation_only_adds_focus_and_keeps_old_torrents_and_checks(self):
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

    def test_daily_priority_cannot_starve_other_sites_and_round_repeats(self):
        normal, low, novice = (self.add_task(i) for i in range(1, 4))
        novice.novice = True
        p = self.plugin
        p._rotation_mode = "sequential"
        p._build_site_ratio_status = lambda task, *args: {"available": True, "reached": task.id != low.id}
        selected = [p._select_rotation_site({}, now=1800000000 + day * 86400) for day in range(4)]
        self.assertEqual(selected, [novice.site_id, low.site_id, normal.site_id, novice.site_id])

    def test_daily_focus_survives_reload_and_ratio_changes_until_its_deadline(self):
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
            self.assertEqual(self.added, ["2"] * 3)
            second.enabled = False
            p._dispatch_brush()
            self.assertEqual(self.data["site_rotation"], {})

    def test_same_site_tasks_share_one_daily_turn_and_expiry_blocks_further_adds(self):
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


if __name__ == "__main__":
    unittest.main()
