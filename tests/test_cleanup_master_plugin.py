"""Exercise plugin orchestration with the actual records-only policy engine."""
import ast
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path
from threading import Event, Lock
from types import ModuleType, SimpleNamespace
import sys
import unittest
from copy import deepcopy
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1] / "plugins.v2/emptyfoldercleaner"
PACKAGE = "cleanup_plugin_tests"
package = ModuleType(PACKAGE)
package.__path__ = [str(ROOT)]
sys.modules[PACKAGE] = package


def load(name):
    spec = importlib.util.spec_from_file_location(PACKAGE + "." + name, ROOT / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


kernel = load("record_cleanup")
host = load("host_adapter")
SOURCE = ROOT / "__init__.py"
TREE = ast.parse(SOURCE.read_text())
CLASS = next(node for node in TREE.body if isinstance(node, ast.ClassDef) and node.name == "EmptyFolderCleaner")


class Base:
    def __init__(self):
        self.data, self.saved_config, self.messages = {}, None, []

    def get_data(self, key):
        return self.data.get(key)

    def save_data(self, key, value):
        self.data[key] = value

    def update_config(self, value):
        self.saved_config = value

    def post_message(self, **value):
        self.messages.append(value)


class Log:
    level = 20
    def __getattr__(self, _name):
        return lambda *_args, **_kwargs: None


class Scheduler:
    running = False
    def __init__(self, **_):
        self.jobs = []
    def add_job(self, function, *_args, **_kwargs):
        self.jobs.append(function)
    def get_jobs(self):
        return self.jobs
    def print_jobs(self):
        pass
    def start(self):
        self.running = True
    def remove_all_jobs(self):
        self.jobs.clear()
    def shutdown(self, **_):
        self.running = False


NAMESPACE = dict(
    __name__=PACKAGE + ".plugin", __package__=PACKAGE, _PluginBase=Base,
    Any=object, List=list, Dict=dict, Tuple=tuple, Optional=__import__("typing").Optional,
    Event=Event, Lock=Lock, Path=Path, datetime=datetime, timedelta=timedelta,
    os=__import__("os"), errno=__import__("errno"), time=__import__("time"),
    logger=Log(), settings=SimpleNamespace(TZ="UTC"),
    pytz=SimpleNamespace(timezone=lambda _: timezone.utc), BackgroundScheduler=Scheduler,
    CronTrigger=SimpleNamespace(from_crontab=lambda value: value),
    NotificationType=SimpleNamespace(SiteMessage="site"),
)
exec(compile(ast.fix_missing_locations(ast.Module(body=[CLASS], type_ignores=[])), str(SOURCE), "exec"), NAMESPACE)
Plugin = NAMESPACE["EmptyFolderCleaner"]

START = 1_000_000.0
DAY = 86400.0


def record(number=7, *, destination=True):
    return kernel.CleanupRecord(
        key=f"file:{number}", revision=f"rev:{number}", record_id=number,
        source_path=f"/downloads/show/S01E{number:02}.mkv",
        destination_paths=(f"/library/show/S01E{number:02}.mkv",) if destination else (),
        media_source="themoviedb", media_id="123", media_type="电视剧", season=1,
        episodes=(number,),
    )


class Adapter:
    def __init__(self, records=(), cursor=0, *, source=kernel.Presence.MISSING):
        self.page = host.CandidatePage(tuple(records), cursor, True)
        self.calls, self.applied, self.purged = [], [], []
        self.source = source
        self.observe_error = False
        self.mutation_failure = False

    def list_candidates(self, **kwargs):
        self.calls.append(kwargs)
        return self.page

    def observe(self, item):
        if self.observe_error:
            raise OSError("private server URL must not appear")
        return kernel.CleanupEvidence(
            source=self.source, destinations=tuple(kernel.Presence.MISSING for _ in item.destination_paths),
            other_versions=kernel.Presence.MISSING, media_library=kernel.Presence.MISSING,
            destination_lookup_complete=True, media_lookup_complete=True,
            downloaders_complete=True, downloader_task_present=self.source is kernel.Presence.PRESENT,
            transfer_task_active=False, storage_ready=True, identity_verified=True, revision=item.revision,
            source_signature=(1, 100, 33188, 1024, 900, 900, 1) if self.source is kernel.Presence.PRESENT else (),
        )

    observe_destination = observe

    def mutate(self, item, _evidence, options):
        if self.mutation_failure:
            return kernel.MutationResult("needs_review", "host_cleanup_failed")
        self.applied.append((item.record_id, options.allow_subscription_retry))
        return kernel.MutationResult("applied", "records_updated")

    def repair_destination(self, *_):
        raise AssertionError("obsolete transfer-only cleanup must never execute")

    def purge_source(self, item, _evidence, options):
        self.purged.append((item.record_id, options.preserve_source_tasks, options.allow_subscription_retry))
        return kernel.MutationResult("applied", "source_purged")


def plugin(adapter, data=None, **config):
    instance = Plugin()
    instance.data = data if data is not None else {}
    instance.init_plugin({"enabled": False, "records_enabled": True, **config})
    instance._build_record_adapter = lambda: adapter
    return instance


def all_text(value):
    if isinstance(value, dict):
        return " ".join(str(v) if k in ("text", "title") else all_text(v) for k, v in value.items())
    if isinstance(value, (list, tuple)):
        return " ".join(all_text(item) for item in value)
    return ""


class CleanupPluginIntegrationTests(unittest.TestCase):
    def run_at(self, instance, now):
        with patch.object(NAMESPACE["time"], "time", return_value=now):
            return instance.clean_records()

    def test_new_default_is_bounded_five_minute_but_existing_config_and_folder_history_survive(self):
        old_history = [{"clean_time": "2026-10-01", "removed_count": 2, "target_dirs": 1}]
        instance = plugin(Adapter(), {"history": old_history}, cron="0 0 * * 1",
                          target_dirs="/Data/Downloads", exclude_dirs="keep", custom_future={"v": 7})
        config = instance._config_payload()
        self.assertEqual(config["cron"], "0 0 * * 1")
        self.assertEqual(config["target_dirs"], "/Data/Downloads")
        self.assertEqual(config["custom_future"], {"v": 7})
        self.assertEqual(config["records_cron"], "*/5 * * * *")
        self.assertEqual(config["records_batch_size"], 100)
        self.assertFalse(config["records_reopen_subscription"])
        self.assertIs(instance.data["history"], old_history)
        custom = plugin(Adapter(), records_cron="15 4 * * *")
        self.assertEqual(custom._records_cron, "15 4 * * *")

    def test_run_once_runs_both_features_and_folder_error_does_not_hide_records(self):
        instance = plugin(Adapter(), target_dirs="/downloads")
        calls = []
        def folder():
            calls.append("folder")
            raise OSError("folder inaccessible")
        instance.clean_empty_folders = folder
        instance.clean_records = lambda: calls.append("records") or {"status": "completed"}
        result = instance.run_once()
        self.assertEqual(calls, ["folder", "records"])
        self.assertEqual(result["folders"], "failed")
        self.assertEqual(result["records"]["status"], "completed")

    def test_folder_validation_failure_is_reported_without_disabling_records(self):
        instance = plugin(Adapter(), target_dirs="/definitely-not-an-existing-cleanup-root")
        result = instance.run_once()
        self.assertEqual(result["folders"], "failed")
        self.assertEqual(result["records"]["status"], "completed")

    def test_reload_stops_old_event_and_keeps_a_new_run_available(self):
        instance = plugin(Adapter())
        previous = instance._event
        instance.init_plugin({"enabled": False, "records_enabled": True})
        self.assertTrue(previous.is_set())
        self.assertFalse(instance._event.is_set())
        instance.stop_service()
        self.assertTrue(instance._event.is_set())
        self.assertEqual(self.run_at(instance, START)["status"], "cancelled")

    def test_restart_keeps_cursor_and_observation_then_wraps_after_last_page(self):
        adapter = Adapter([record(7, destination=False)], cursor=7)
        instance = plugin(adapter)
        first = self.run_at(instance, START)
        self.assertEqual(first["status"], "completed")
        self.assertEqual(instance.data["records_state"]["cursor"], 7)
        saved = instance.data["records_state"]["full_missing"]
        self.assertEqual(saved["candidates"]["file:7"]["first_seen"], START)
        adapter.page = host.CandidatePage((record(8, destination=False),), 0, True)
        reopened = plugin(adapter, instance.data)
        self.run_at(reopened, START + 3600)
        self.assertEqual(adapter.calls[-1]["cursor"], 7)
        self.assertEqual(reopened.data["records_state"]["cursor"], 0)
        self.assertEqual(set(reopened.data["records_state"]["full_missing"]["candidates"]), {"file:7", "file:8"})

    def test_default_preview_never_mutates_even_after_stability(self):
        adapter = Adapter([record(destination=False)])
        instance = plugin(adapter)
        self.run_at(instance, START)
        report = self.run_at(instance, START + DAY)
        self.assertEqual(report["summary"]["eligible"], 1)
        self.assertEqual(adapter.applied, [])
        self.assertTrue(report["dry_run"])

    def test_actual_record_write_waits_a_day_and_does_not_claim_file_deletion(self):
        adapter = Adapter([record(destination=False)])
        instance = plugin(adapter, records_dry_run=False, records_reopen_subscription=False)
        self.run_at(instance, START)
        self.assertEqual(adapter.applied, [])
        report = self.run_at(instance, START + DAY)
        self.assertEqual(adapter.applied, [(7, False)])
        self.assertEqual(report["summary"]["applied"], 1)
        self.assertNotIn("deleted_files", report)

    def test_source_purge_has_independent_day_and_never_reopens_subscription(self):
        adapter = Adapter([record()], source=kernel.Presence.PRESENT)
        instance = plugin(adapter, records_dry_run=False, records_reopen_subscription=True)
        self.run_at(instance, START)
        state = instance.data["records_state"]
        self.assertEqual(state["full_missing"]["candidates"], {})
        self.assertIn("file:7", state["source_purge"]["candidates"])
        self.run_at(instance, START + DAY)
        self.assertEqual(adapter.applied, [])
        self.assertEqual(adapter.purged, [(7, False, False)])

    def test_old_transfer_only_state_cannot_authorize_source_deletion(self):
        adapter = Adapter([record()], source=kernel.Presence.PRESENT)
        previous = kernel.RecordCleanupEngine(adapter.observe_destination, decision=kernel.destination_decision).run(
            [record()], {}, now=START)
        data = {"records_state": {"version": 1, "cursor": 0, "destination_only": previous.state}}
        instance = plugin(adapter, data, records_dry_run=False)
        report = self.run_at(instance, START + DAY)
        self.assertEqual(adapter.purged, [])
        self.assertNotIn("destination_only", instance.data["records_state"])
        self.assertEqual(report["summary"]["waiting"], 1)
        self.assertEqual(instance.data["records_state"]["source_purge"]["candidates"]["file:7"]["first_seen"], START + DAY)

    def test_source_purge_preview_never_calls_mutation_and_existing_subscription_option_is_preserved(self):
        adapter = Adapter([record()], source=kernel.Presence.PRESENT)
        instance = plugin(adapter, records_reopen_subscription=True)
        self.run_at(instance, START)
        report = self.run_at(instance, START + DAY)
        self.assertEqual(report["summary"]["eligible"], 1)
        self.assertEqual(adapter.purged, [])
        self.assertTrue(instance._config_payload()["records_reopen_subscription"])
        missing_adapter = Adapter([record(destination=False)])
        enabled = plugin(missing_adapter, records_dry_run=False, records_reopen_subscription=True)
        self.run_at(enabled, START)
        self.run_at(enabled, START + DAY)
        self.assertEqual(missing_adapter.applied, [(7, True)])

    def test_partial_source_delete_survives_restart_and_never_reopens_subscription(self):
        adapter = Adapter([record()], source=kernel.Presence.PRESENT)
        instance = plugin(adapter, records_dry_run=False, records_reopen_subscription=True)
        observed_intents = []
        def partial(item, _evidence, options):
            # The durable intent must precede even the first physical action.
            observed_intents.append(deepcopy(instance.data["records_state"]["purged_source"][item.key]))
            adapter.source = kernel.Presence.MISSING
            return kernel.MutationResult("needs_review", "source_purged_history_pending")
        adapter.purge_source = partial
        self.run_at(instance, START)
        self.run_at(instance, START + DAY)
        self.assertEqual(observed_intents[0]["revision"], record().revision)
        self.assertIn(record().key, instance.data["records_state"]["purged_source"])
        restarted = plugin(adapter, deepcopy(instance.data), records_dry_run=False, records_reopen_subscription=True)
        self.run_at(restarted, START + DAY + 1)
        self.run_at(restarted, START + 2 * DAY + 1)
        self.assertEqual(adapter.applied, [(7, False)])
        self.assertNotIn(record().key, restarted.data["records_state"]["purged_source"])

    def test_exception_after_source_delete_keeps_durable_intent(self):
        adapter = Adapter([record()], source=kernel.Presence.PRESENT)
        instance = plugin(adapter, records_dry_run=False, records_reopen_subscription=True)
        def interrupted(*_):
            adapter.source = kernel.Presence.MISSING
            raise OSError("process interrupted after unlink")
        adapter.purge_source = interrupted
        self.run_at(instance, START)
        self.run_at(instance, START + DAY)
        self.assertIn(record().key, instance.data["records_state"]["purged_source"])
        restarted = plugin(adapter, deepcopy(instance.data), records_dry_run=False, records_reopen_subscription=True)
        self.run_at(restarted, START + DAY + 1)
        self.run_at(restarted, START + 2 * DAY + 1)
        self.assertEqual(adapter.applied, [(7, False)])

    def test_source_intent_write_or_readback_failure_prevents_any_physical_purge(self):
        for failure in ("raise", "silent", "readback"):
            with self.subTest(failure=failure):
                adapter = Adapter([record()], source=kernel.Presence.PRESENT)
                instance = plugin(adapter, records_dry_run=False)
                self.run_at(instance, START)
                save, get = instance.save_data, instance.get_data
                armed = []
                def save_fault(key, value):
                    if key == "records_state" and value.get("purged_source"):
                        armed.append(True)
                        if failure == "raise":
                            raise OSError("intent store unavailable")
                        if failure == "silent":
                            return
                    save(key, value)
                def get_fault(key):
                    if key == "records_state" and armed and failure == "readback":
                        raise OSError("intent readback unavailable")
                    return get(key)
                instance.save_data, instance.get_data = save_fault, get_fault
                result = self.run_at(instance, START + DAY)
                self.assertEqual(adapter.purged, [])
                self.assertEqual(result["status"], "failed")

    def test_invalid_intent_state_cannot_fall_back_to_retry_enabled(self):
        for value in ("broken", {"version": 1, "purged_source": []},
                      {"version": 1, "purged_source": {"file:7": {"revision": ""}}}):
            with self.subTest(value=value):
                adapter = Adapter([record(destination=False)])
                instance = plugin(adapter, {"records_state": value}, records_dry_run=False,
                                  records_reopen_subscription=True)
                result = self.run_at(instance, START)
                self.assertEqual(result["status"], "failed")
                self.assertEqual(adapter.applied, [])
                self.assertEqual(adapter.calls, [])

    def test_intent_read_failure_prevents_all_mutations(self):
        adapter = Adapter([record(destination=False)])
        instance = plugin(adapter, records_dry_run=False, records_reopen_subscription=True)
        get = instance.get_data
        def unavailable(key):
            if key == "records_state":
                raise OSError("state cannot be read")
            return get(key)
        instance.get_data = unavailable
        report = self.run_at(instance, START)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(adapter.calls, [])
        self.assertEqual(adapter.applied, [])

    def test_changed_record_revision_keeps_intent_and_never_falls_back_to_retry_true(self):
        adapter = Adapter([record(destination=False)])
        data = {"records_state": {"version": 1, "purged_source": {record().key: {
            "revision": "earlier-revision", "source_signature": [1, 100, 33188, 1024, 900, 900, 1],
        }}}}
        instance = plugin(adapter, data, records_dry_run=False, records_reopen_subscription=True)
        self.run_at(instance, START)
        report = self.run_at(instance, START + DAY)
        self.assertEqual(adapter.applied, [])
        self.assertEqual(report["items"][0]["reason"], "source_intent_revision_changed")
        self.assertIn(record().key, instance.data["records_state"]["purged_source"])

    def test_committed_cleanup_removes_intent_only_after_confirmed_state_write(self):
        adapter = Adapter([record(destination=False)])
        data = {"records_state": {"version": 1, "purged_source": {record().key: {
            "revision": record().revision, "source_signature": [1, 100, 33188, 1024, 900, 900, 1],
        }}}}
        instance = plugin(adapter, data, records_dry_run=False, records_reopen_subscription=True)
        self.run_at(instance, START)
        save = instance.save_data
        def fail_clear(key, value):
            if key == "records_state" and not value.get("purged_source"):
                raise OSError("clear intent failed")
            save(key, value)
        instance.save_data = fail_clear
        report = self.run_at(instance, START + DAY)
        self.assertEqual(adapter.applied, [(7, False)])
        self.assertEqual(report["status"], "failed")
        self.assertIn(record().key, instance.data["records_state"]["purged_source"])

    def test_page_read_failure_reports_failure_and_does_not_move_cursor_or_state(self):
        adapter = Adapter([record(destination=False)], cursor=7)
        instance = plugin(adapter)
        self.run_at(instance, START)
        prior_state = instance.data["records_state"]
        adapter.page = host.CandidatePage(cursor=7, reason="candidate_read_failed")
        report = self.run_at(instance, START + DAY)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["error"], "candidate_read_failed")
        self.assertIs(instance.data["records_state"], prior_state)
        self.assertEqual(instance.data["records_state"]["cursor"], 7)

    def test_state_write_failure_is_visible_and_never_publishes_advanced_cursor(self):
        instance = plugin(Adapter([record(destination=False)], cursor=7))
        save = instance.save_data
        failed = []
        def interrupted_save(key, value):
            if key == "records_state" and not failed:
                failed.append(True)
                raise OSError("storage failure")
            save(key, value)
        instance.save_data = interrupted_save
        report = self.run_at(instance, START)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["cursor_after"], 0)
        self.assertEqual(instance.data["records_state"]["cursor"], 0)

    def test_individual_observer_failure_is_visible_but_does_not_starve_later_pages(self):
        adapter = Adapter([record(destination=False)], cursor=7)
        adapter.observe_error = True
        instance = plugin(adapter)
        report = self.run_at(instance, START)
        self.assertEqual(report["status"], "review")
        self.assertEqual(report["summary"]["needs_review"], 1)
        self.assertEqual(report["items"][0]["reason"], "observation_failed")
        self.assertNotIn("private server", repr(report))
        self.assertEqual(instance.data["records_state"]["cursor"], 7)

    def test_individual_mutation_failure_keeps_record_uncompleted_and_reports_reason(self):
        adapter = Adapter([record(destination=False)], cursor=7)
        instance = plugin(adapter, records_dry_run=False)
        self.run_at(instance, START)
        adapter.mutation_failure = True
        adapter.page = host.CandidatePage((record(destination=False),), 0, True)
        report = self.run_at(instance, START + DAY)
        self.assertEqual(report["status"], "review")
        self.assertEqual(instance.data["records_state"]["full_missing"]["completed"], {})
        self.assertEqual(instance.data["records_state"]["cursor"], 0)
        self.assertEqual(report["items"][0]["reason"], "host_cleanup_failed")

    def test_repeated_click_while_busy_does_not_replace_report_or_state(self):
        adapter = Adapter([record(destination=False)])
        instance = plugin(adapter)
        instance.data["records_report"] = {"status": "running", "run_id": "first"}
        instance._records_lock.acquire()
        try:
            result = self.run_at(instance, START)
        finally:
            instance._records_lock.release()
        self.assertEqual(result["status"], "busy")
        self.assertEqual(instance.data["records_report"], {"status": "running", "run_id": "first"})
        self.assertEqual(adapter.calls, [])

    def test_cancelled_batch_never_advances_cursor(self):
        adapter = Adapter([record(destination=False)], cursor=7)
        instance = plugin(adapter)
        original = adapter.observe
        def stop(item):
            instance._event.set()
            return original(item)
        adapter.observe = stop
        result = self.run_at(instance, START)
        self.assertEqual(result["status"], "cancelled")
        self.assertEqual(instance.data.get("records_state", {}).get("cursor", 0), 0)
        self.assertEqual(adapter.applied, [])

    def test_page_renders_record_outcomes_and_existing_folder_history_together(self):
        adapter = Adapter([record(destination=False)])
        instance = plugin(adapter, {"history": [dict(clean_time="2026-10-01", removed_count=2, target_dirs=1)]})
        self.run_at(instance, START)
        text = all_text(instance.get_page())
        self.assertIn("观察", text)
        self.assertIn("S01E07.mkv", text)
        self.assertIn("2026-10-01", text)
        self.assertIn("记录", text)
        self.assertIn("空文件夹", text)

    def test_report_history_is_bounded_and_retains_no_full_duplicate_item_lists(self):
        instance = plugin(Adapter())
        for i in range(25):
            self.run_at(instance, START + i)
        self.assertEqual(len(instance.data["records_history"]), 20)
        self.assertTrue(all("items" not in item for item in instance.data["records_history"]))

    def test_source_cleanup_report_explains_protection_and_partial_outcomes(self):
        instance = plugin(Adapter())
        expected = {
            "source_signature_unverified": "文件身份或独占硬链接状态",
            "source_purge_evidence_changed": "源文件、媒体身份或归属证据已变化",
            "source_shared_by_task": "其他种子任务共用",
            "torrent_not_exclusive_single_file": "独占单文件种子",
            "torrent_file_incomplete": "尚未完整下载",
            "task_inventory_incomplete": "种子任务清单不完整",
            "file_inventory_incomplete": "种子文件清单不完整",
            "task_inventory_limit": "种子任务数超出",
            "file_inventory_limit": "种子文件数超出",
            "file_path_ambiguous": "文件路径无法精确对应",
            "invalid_source_identity": "源文件身份或大小无法确认",
            "source_purge_not_authorized": "本次运行不允许",
            "media_lookup_incomplete": "媒体库范围",
            "downloader_remove_unconfirmed": "任务移除未全部确认",
            "source_unlink_unconfirmed": "源文件删除结果未确认",
            "source_purged_history_pending": "源文件已删除",
            "source_purge_failed": "可能部分完成",
        }
        for reason, text in expected.items():
            with self.subTest(reason=reason):
                report = {"status": "review", "dry_run": False, "items": [{
                    "file_id": 7, "file": "S01E07.mkv", "branch": "source_purge",
                    "status": "needs_review", "reason": reason,
                }]}
                rendered = all_text(instance._record_report_page(report))
                self.assertIn(text, rendered)
                self.assertNotIn(f"（{reason}）", rendered)
                if reason in ("source_purged_history_pending", "source_unlink_unconfirmed",
                              "downloader_remove_unconfirmed", "source_purge_failed",
                              "source_purge_evidence_changed"):
                    self.assertNotIn("未修改", rendered)
        pending = instance._record_report_page({"status": "review", "dry_run": False, "items": [{
            "branch": "source_purge", "status": "needs_review", "reason": "source_purged_history_pending",
        }]})
        self.assertIn("下轮缺源核对", all_text(pending))
        self.assertIn("不更新订阅进度", all_text(pending))


if __name__ == "__main__":
    unittest.main()
