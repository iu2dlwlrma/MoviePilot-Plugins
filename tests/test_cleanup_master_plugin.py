"""Exercise plugin API/orchestration without accessing MoviePilot or business data."""
import ast
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
import sys
from threading import Event, Lock, Thread
import time
from types import SimpleNamespace, ModuleType
import unittest
from unittest.mock import patch
import uuid

SOURCE = Path(__file__).resolve().parents[1] / "plugins.v2/emptyfoldercleaner/__init__.py"
package = ModuleType("cleanup_plugin_tests")
package.__path__ = [str(SOURCE.parent)]
sys.modules[package.__name__] = package
TREE = ast.parse(SOURCE.read_text())
CLASS = next(node for node in TREE.body if isinstance(node, ast.ClassDef) and node.name == "EmptyFolderCleaner")

class Base:
    def __init__(self):
        self.data, self.saved_config, self.messages = {}, None, []
        self.stored_config = None
        self.directory = tempfile.TemporaryDirectory()
    def get_data_path(self): return Path(self.directory.name)
    def get_data(self, key): return deepcopy(self.data.get(key))
    def save_data(self, key, value): self.data[key] = deepcopy(value)
    def get_config(self): return deepcopy(self.stored_config if self.stored_config is not None else self._loaded_config)
    def update_config(self, value):
        self.saved_config = deepcopy(value)
        self.stored_config = deepcopy(value)
        return True
    def post_message(self, **value): self.messages.append(value)

class Log:
    level = 20
    def __getattr__(self, _): return lambda *args, **kwargs: None

class Scheduler:
    running = False
    def __init__(self, **_): self.jobs = []
    def add_job(self, function, *_args, **_kwargs): self.jobs.append(function)
    def get_jobs(self): return self.jobs
    def print_jobs(self): pass
    def start(self): self.running = True
    def remove_all_jobs(self): self.jobs.clear()
    def shutdown(self, **_): self.running = False

class Response:
    def __init__(self, success, data=None, message=None):
        self.success, self.data, self.message = success, data, message

NAMESPACE = dict(
    __name__="cleanup_plugin_tests.plugin", __package__="cleanup_plugin_tests",
    _PluginBase=Base, Any=object, List=list, Dict=dict, Tuple=tuple,
    Optional=__import__("typing").Optional, Event=Event, Lock=Lock, Thread=Thread,
    Path=Path, datetime=datetime, timedelta=timedelta, os=os, errno=errno,
    time=time, fcntl=fcntl, hashlib=hashlib, json=json, uuid=uuid, deepcopy=deepcopy,
    logger=Log(), settings=SimpleNamespace(TZ="UTC"), Response=Response,
    pytz=SimpleNamespace(timezone=lambda _: timezone.utc), BackgroundScheduler=Scheduler,
    CronTrigger=SimpleNamespace(from_crontab=lambda value: value if len(value.split()) == 5 else (_ for _ in ()).throw(ValueError())),
    NotificationType=SimpleNamespace(SiteMessage="site"),
)
exec(compile(ast.fix_missing_locations(ast.Module(body=[CLASS], type_ignores=[])), str(SOURCE), "exec"), NAMESPACE)
Plugin = NAMESPACE["EmptyFolderCleaner"]

class Adapter:
    def __init__(self, *, hold=False):
        self.entered, self.release = Event(), Event()
        self.hold, self.options = hold, None
    def reconcile_batch(self, state, *, persist, now=None, dry_run=True, options=None, stop_requested=None):
        self.options = options
        self.entered.set()
        if self.hold: self.release.wait(2)
        snapshot = {**state, "cleanup_v2": {"cursor": 9}}
        persist(snapshot)
        return SimpleNamespace(state=snapshot, report={
            "status": "cancelled" if stop_requested() else "completed", "dry_run": dry_run,
            "processed": 1, "items": [{"key": "file:7", "file": "PRIVATE_MEDIA.mkv", "branch": "file_only", "status": "applied", "reason": "missing_file_row_removed"}],
            "summary": {"applied": 1}, "counts": {"download_files": 1, "source_files": 0, "tasks": 0, "download_history": 0, "transfer_history": 0},
            "observed_pending": 0, "cursor_before": 0, "cursor_after": 9, "cycle_complete": False,
            "scan": {"cycle_id": "cycle", "checked": 1, "complete": False, "total": None},
        })

class PluginTests(unittest.TestCase):
    def test_every_registered_api_preserves_the_host_superuser_boundary(self):
        fastapi = ModuleType("fastapi")
        auth = ModuleType("app.api.dependencies.auth")
        def superuser_guard(): pass
        fastapi.Depends = lambda dependency: SimpleNamespace(dependency=dependency)
        auth.get_current_active_superuser = superuser_guard
        plugin = self.instance()
        with patch.dict(sys.modules, {"fastapi": fastapi, "app.api.dependencies.auth": auth}):
            apis = plugin.get_api()
        self.assertEqual(len(apis), 7)
        for api in apis:
            self.assertEqual(api["auth"], "bear")
            self.assertTrue(api.get("dependencies"), api["path"])
            self.assertIs(api["dependencies"][0].dependency, superuser_guard)

    def instance(self, config=None, adapter=None):
        obj = Plugin()
        def cleanup():
            obj.stop_service()
            if adapter is not None:
                adapter.release.set()
            if obj._run_thread and obj._run_thread.is_alive():
                obj._run_thread.join(3)
            obj.directory.cleanup()
        self.addCleanup(cleanup)
        obj.init_plugin(config or {"enabled": False, "records_enabled": True})
        obj._build_record_adapter = lambda: adapter or Adapter()
        obj._refresh_services = lambda: None
        return obj

    def test_settings_patch_preserves_unknowns_and_does_not_consume_onlyonce(self):
        obj = self.instance({"enabled": False, "cron": "0 0 * * 1", "target_dirs": "/old", "onlyonce": False, "future": {"secret": "keep"}, "records_reopen_subscription": True})
        obj._loaded_config["onlyonce"] = True
        obj._onlyonce = True
        before = obj.get_settings().data
        result = obj.update_settings({"revision": before["revision"], "changes": {"notify": True}})
        self.assertTrue(result.success)
        self.assertEqual(obj.saved_config, {"enabled": False, "cron": "0 0 * * 1", "target_dirs": "/old", "onlyonce": True, "future": {"secret": "keep"}, "records_reopen_subscription": True, "notify": True})
        self.assertIsNone(obj._scheduler)

    def test_stale_revision_and_unknown_patch_never_replace_configuration(self):
        obj = self.instance()
        self.assertFalse(obj.update_settings({"revision": "stale", "changes": {"notify": True}}).success)
        self.assertIsNone(obj.saved_config)
        revision = obj.get_settings().data["revision"]
        self.assertFalse(obj.update_settings({"revision": revision, "changes": {"future": "overwrite"}}).success)
        self.assertIsNone(obj.saved_config)

    def test_settings_merge_reads_actual_storage_and_confirms_the_write(self):
        obj = self.instance({"notify": False, "future": "old"})
        obj.stored_config = {"notify": False, "future": "external"}
        revision = obj.get_settings().data["revision"]
        self.assertTrue(obj.update_settings({"revision": revision, "changes": {"notify": True}}).success)
        self.assertEqual(obj.saved_config, {"notify": True, "future": "external"})
        revision = obj.get_settings().data["revision"]
        obj.update_config = lambda value: None
        result = obj.update_settings({"revision": revision, "changes": {"notify": False}})
        self.assertFalse(result.success)
        self.assertTrue(obj._notify)

    def test_invalid_config_rejects_the_whole_patch(self):
        obj = self.instance()
        revision = obj.get_settings().data["revision"]
        for changes in ({"notify": "false"}, {"records_batch_size": 101}, {"cron": "broken"}, {"onlyonce": True}):
            self.assertFalse(obj.update_settings({"revision": revision, "changes": changes}).success)
        self.assertIsNone(obj.saved_config)

    def test_reconcile_persists_with_readback_and_forbids_subscription_mutations(self):
        adapter = Adapter()
        obj = self.instance({"records_enabled": True, "records_dry_run": False, "records_reopen_subscription": True}, adapter)
        result = obj.clean_records()
        self.assertEqual(result["status"], "completed")
        self.assertFalse(adapter.options.allow_subscription_retry)
        self.assertEqual(obj.data["records_state"]["cleanup_v2"]["cursor"], 9)
        self.assertEqual(obj.data["records_report"]["counts"]["download_files"], 1)

    def test_persist_readback_failure_is_failed_not_completed(self):
        obj = self.instance()
        real_save = obj.save_data
        obj.save_data = lambda key, value: None if key == "records_state" else real_save(key, value)
        self.assertEqual(obj.clean_records()["status"], "failed")

    def test_home_and_status_have_only_statistics_not_media_items(self):
        obj = self.instance()
        obj.clean_records()
        self.assertNotIn("PRIVATE_MEDIA", repr(obj.get_status().data))
        self.assertNotIn("PRIVATE_MEDIA", repr(obj.get_page()))
        self.assertEqual(obj.get_status().data["records"]["processed"], 1)
        self.assertFalse(obj.get_status().data["records"]["scan"]["complete"])

    def test_details_are_paginated_from_an_immutable_snapshot(self):
        obj = self.instance()
        obj.clean_records()
        first = obj.get_details(kind="records", page=1, page_size=1).data
        obj.data["records_report"]["items"] = [{"file": "new.mkv"}]
        same = obj.get_details(kind="records", page=1, page_size=1, snapshot_id=first["snapshot_id"]).data
        self.assertEqual(same["items"][0]["file"], "PRIVATE_MEDIA.mkv")
        self.assertEqual(same["scope"], "latest_batch")

    def test_legacy_folder_history_is_preserved_and_paginated(self):
        obj = self.instance()
        obj.data["history"] = [{"clean_time": str(i), "removed_count": i} for i in range(50)]
        result = obj.get_history(kind="folders", page=2, page_size=20).data
        self.assertEqual(result["total"], 50)
        self.assertEqual(len(result["items"]), 20)
        self.assertEqual(len(obj.data["history"]), 50)

    def test_explicit_empty_record_cron_never_registers_default_schedule(self):
        obj = self.instance({"enabled": True, "records_enabled": True, "records_cron": ""})
        self.assertEqual(obj.get_service(), [])
        self.assertEqual(obj.get_settings().data["config"]["records_cron"], "")

    def test_home_selects_latest_folder_history_and_omits_non_statistical_fields(self):
        obj = self.instance()
        obj.data["history"] = [{"clean_time": "2026-10-29", "removed_count": 8, "private_file": "SECRET.mkv"},
                               {"clean_time": "2026-10-01", "removed_count": 1}]
        result = obj.get_status().data
        self.assertEqual(result["folders"]["removed_count"], 8)
        self.assertNotIn("SECRET.mkv", repr(result))

    def test_snapshot_keeps_all_bounded_results_instead_of_silently_truncating(self):
        obj = self.instance()
        obj._save_record_report({"status": "completed", "items": [{"file": str(i), "branch": "file_only"} for i in range(250)]})
        result = obj.get_details(page=13, page_size=20).data
        self.assertEqual(result["total"], 250)
        self.assertEqual(result["items"][-1]["file"], "249")

    def test_duplicate_manual_runs_share_one_job_and_cancel_reports_real_outcome(self):
        adapter = Adapter(hold=True)
        obj = self.instance(adapter=adapter)
        first = obj.start_run({"scope": "records", "request_id": "click1"})
        self.assertTrue(adapter.entered.wait(1))
        second = obj.start_run({"scope": "records", "request_id": "click2"})
        self.assertEqual(first.data["run_id"], second.data["run_id"])
        self.assertTrue(obj.cancel_run({"run_id": first.data["run_id"]}).success)
        adapter.release.set()
        obj._run_thread.join(2)
        self.assertEqual(obj.get_status().data["run"]["status"], "cancelled")

    def test_file_lock_prevents_a_different_instance_starting_another_job(self):
        adapter = Adapter(hold=True)
        obj = self.instance(adapter=adapter)
        obj.start_run({"scope": "records", "request_id": "first"})
        self.assertTrue(adapter.entered.wait(1))
        other = self.instance()
        other.directory.cleanup()
        other.get_data_path = obj.get_data_path
        self.assertFalse(other.start_run({"scope": "records", "request_id": "other"}).success)
        adapter.release.set()
        obj._run_thread.join(2)

    def test_restarted_unlocked_job_is_interrupted_without_losing_history(self):
        obj = self.instance()
        obj.data["cleanup_run"] = {"run_id": "old", "status": "running", "scope": "records"}
        self.assertEqual(obj.get_status().data["run"]["status"], "interrupted")
        self.assertEqual(obj.data["cleanup_run"]["run_id"], "old")

    def test_old_cancel_cannot_stop_or_overwrite_a_replacement_job(self):
        obj = self.instance()
        obj.data["cleanup_run"] = {"run_id": "old", "status": "running", "scope": "records"}
        obj._run_event = Event()
        obj._run_event_id = "new"
        new_event = obj._run_event
        original_get = obj.get_data
        reads = 0
        def interleaved(key):
            nonlocal reads
            result = original_get(key)
            if key == "cleanup_run":
                reads += 1
                if reads == 1:
                    obj.data["cleanup_run"] = {"run_id": "new", "status": "running", "scope": "records"}
            return result
        obj.get_data = interleaved
        obj.cancel_run({"run_id": "old"})
        self.assertFalse(new_event.is_set())
        self.assertEqual(obj.data["cleanup_run"]["run_id"], "new")

    def test_cancel_does_not_revive_a_completed_receipt(self):
        obj = self.instance()
        obj.data["cleanup_run"] = {"run_id": "old", "status": "running", "scope": "records"}
        original_get = obj.get_data
        reads = 0
        def interleaved(key):
            nonlocal reads
            result = original_get(key)
            if key == "cleanup_run":
                reads += 1
                if reads == 1: obj.data["cleanup_run"]["status"] = "completed"
            return result
        obj.get_data = interleaved
        obj.cancel_run({"run_id": "old"})
        self.assertEqual(obj.data["cleanup_run"]["status"], "completed")

    def test_cross_instance_cancel_and_replacement_use_the_same_file_transaction(self):
        first = self.instance()
        first.data["cleanup_run"] = {"run_id": "old", "status": "running", "scope": "records"}
        first._run_event, first._run_event_id = Event(), "old"
        adapter = Adapter(hold=True)
        second = self.instance(adapter=adapter)
        second.get_data_path = first.get_data_path
        second.data = first.data
        at_write, release, replacing, replaced = Event(), Event(), Event(), Event()
        original = first._persist_checked
        def paused_write(key, value):
            if key == "cleanup_cancel":
                at_write.set()
                release.wait(1)
            return original(key, value)
        first._persist_checked = paused_write
        cancel = Thread(target=lambda: first.cancel_run({"run_id": "old"}))
        def replace():
            replacing.set()
            second._state_transaction(lambda: second._write_run_if_current(
                {"run_id": "old", "status": "completed", "scope": "records"}))
            second.start_run({"scope": "records", "request_id": "replacement"})
            replaced.set()
        replacement = Thread(target=replace)
        def join_test_threads():
            release.set()
            if cancel.ident: cancel.join(1)
            if replacement.ident: replacement.join(1)
        self.addCleanup(join_test_threads)
        cancel.start()
        self.assertTrue(at_write.wait(1))
        replacement.start()
        self.assertTrue(replacing.wait(1))
        try:
            self.assertFalse(replaced.wait(.05), "replacement must wait for the cancellation transaction")
        finally:
            release.set()
        cancel.join(1); replacement.join(1)
        self.assertTrue(adapter.entered.wait(1))
        self.assertNotEqual(second.data["cleanup_run"]["run_id"], "old")
        self.assertFalse(second._run_event.is_set())
        adapter.release.set()
        second._run_thread.join(1)

    def test_hot_reload_cannot_turn_an_old_folder_preview_into_deletion(self):
        obj = self.instance({"target_dirs": "", "dry_run": True, "notify": False})
        root = Path(obj.directory.name) / "scan"
        (root / "empty").mkdir(parents=True)
        obj._load_config({"target_dirs": str(root), "dry_run": True, "notify": False})
        original_excluded = obj._EmptyFolderCleaner__is_excluded
        calls = 0
        def reload_after_stop_check(path, *args):
            nonlocal calls
            if path.name == "empty":
                calls += 1
                if calls == 2:
                    obj.init_plugin({"target_dirs": str(root), "dry_run": False, "notify": True})
            return original_excluded(path, *args)
        obj._EmptyFolderCleaner__is_excluded = reload_after_stop_check
        obj.clean_empty_folders()
        self.assertTrue((root / "empty").is_dir())
        self.assertTrue(obj.data["history"][-1]["dry_run"])
        self.assertEqual(obj.messages, [])

    def test_record_run_keeps_its_original_preview_mode_during_reload(self):
        adapter = Adapter()
        obj = self.instance({"records_enabled": True, "records_dry_run": True}, adapter)
        def reload():
            obj._load_config({"records_enabled": True, "records_dry_run": False})
            return adapter
        obj._build_record_adapter = reload
        result = obj.clean_records()
        self.assertTrue(result["dry_run"])

    def test_failed_batch_exposes_a_safe_recovery_reason_in_details(self):
        obj = self.instance()
        def fail(): raise OSError("PRIVATE_AUTH_VALUE")
        obj._build_record_adapter = fail
        self.assertEqual(obj.clean_records()["status"], "failed")
        details = obj.get_details().data
        self.assertEqual(details["run_error"], "record_run_failed")
        self.assertNotIn("PRIVATE_AUTH_VALUE", repr(details))

    def test_budget_yield_is_continuable_instead_of_claiming_completion(self):
        obj = self.instance()
        obj._run_records_batch = lambda stop, config: {"status": "yielded", "error": "batch_host_budget"}
        obj.clean_records()
        self.assertEqual(obj.get_status().data["run"]["status"], "yielded")
        obj._save_record_report({"status": "yielded", "error": "batch_host_budget", "items": []})
        self.assertEqual(obj.get_details().data["run_error"], "batch_host_budget")

    def test_request_retry_after_another_completed_run_reuses_its_original_job(self):
        obj = self.instance()
        first = obj.start_run({"scope": "records", "request_id": "first"})
        obj._run_thread.join(1)
        second = obj.start_run({"scope": "records", "request_id": "second"})
        obj._run_thread.join(1)
        retried = obj.start_run({"scope": "records", "request_id": "first"})
        self.assertEqual(retried.data["run_id"], first.data["run_id"])
        self.assertTrue(retried.data["reused"])
        self.assertEqual(obj.data["cleanup_run"]["run_id"], second.data["run_id"])

    def test_reusing_a_request_id_for_a_different_scope_is_rejected(self):
        obj = self.instance()
        obj.start_run({"scope": "records", "request_id": "stable"})
        obj._run_thread.join(1)
        self.assertFalse(obj.start_run({"scope": "folders", "request_id": "stable"}).success)

    def test_failed_thread_creation_has_a_failed_receipt_and_never_claims_started(self):
        class BrokenThread:
            def __init__(self, *args, **kwargs): pass
            def start(self): raise RuntimeError("no capacity")
            def is_alive(self): return False
        obj = self.instance()
        with patch.dict(NAMESPACE, {"Thread": BrokenThread}):
            result = obj.start_run({"scope": "records", "request_id": "failed"})
        self.assertFalse(result.success)
        self.assertEqual(result.data["status"], "failed")
        self.assertTrue(result.data["not_started"])
        self.assertEqual(obj.data["cleanup_run"]["status"], "failed")
        again = obj.start_run({"scope": "records", "request_id": "failed"})
        self.assertFalse(again.success)
        self.assertTrue(again.data["reused"])

    def test_recent_request_receipts_are_bounded_while_aliases_reuse_one_worker(self):
        adapter = Adapter(hold=True)
        obj = self.instance(adapter=adapter)
        original = obj.start_run({"scope": "records", "request_id": "original"})
        self.assertTrue(adapter.entered.wait(1))
        for index in range(60):
            result = obj.start_run({"scope": "records", "request_id": str(index)})
            self.assertEqual(result.data["run_id"], original.data["run_id"])
        self.assertEqual(len(obj.data["cleanup_requests"]), 50)

if __name__ == "__main__": unittest.main()
