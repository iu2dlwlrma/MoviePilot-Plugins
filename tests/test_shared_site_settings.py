"""Exercise the shared settings transaction and scheduler with real plugin methods."""
import ast
from copy import deepcopy
from contextlib import contextmanager, nullcontext
from datetime import datetime
import math
import json
from pathlib import Path
import sys
import sqlite3
import threading
import time
from types import ModuleType, SimpleNamespace
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1] / 'plugins.v3/brushflow'
models = ModuleType('shared_site_models')
sys.modules[models.__name__] = models
exec(compile((ROOT / 'models.py').read_text(), 'models.py', 'exec'), models.__dict__)
source = ast.parse((ROOT / '__init__.py').read_text())
plugin = next(node for node in source.body if isinstance(node, ast.ClassDef) and node.name == 'OriechoBrushFlow')
methods = {
    'update_settings', '_apply_settings', '_current_config', '_load_task_config',
    '_downloader_change_has_active_torrents', '_validate_task_reference',
    '_build_status_data', '_global_dynamic_delete_enabled', 'get_state', 'get_service',
    '_priority_tier', '_select_rotation_site_locked', '_active_manual_focus_site_id',
    '_request_manual_focus', '_site_retry_until', 'update_task_state',
    'create_task', 'update_task', 'check', 'update_focus',
    'delete_task', 'clear_task_data', '_task_data_write_scope', '_is_task_busy',
    '_settings_yield_requested', '_consume_settings_yield',
}
nodes = [ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0)]
nodes.extend(node for node in source.body if isinstance(node, ast.Assign) and any(
    isinstance(target, ast.Name) and target.id in {'TASK_CONFIG_FIELDS', 'GLOBAL_LIMIT_FIELDS', 'GLOBAL_DYNAMIC_DELETE_FIELDS'}
    for target in node.targets))
nodes.append(next(node for node in source.body if isinstance(node, ast.ClassDef) and node.name == 'BrushTaskConfig'))
nodes.append(ast.ClassDef(name='OriechoBrushFlow', bases=[], keywords=[], decorator_list=[],
                         body=[node for node in plugin.body if isinstance(node, ast.FunctionDef) and node.name in methods]))
namespace = {
    'DEFAULT_TASK_SETTINGS': models.DEFAULT_TASK_SETTINGS, 'SHARED_TASK_FIELDS': models.SHARED_TASK_FIELDS,
    'BrushFlowSettingsPayload': models.BrushFlowSettingsPayload,
    'BrushTaskPayload': models.BrushTaskPayload,
    'schemas': SimpleNamespace(Response=lambda **kwargs: SimpleNamespace(**kwargs)),
    'uuid': uuid, 'math': math, 'threading': threading, 'time': time, 'datetime': datetime,
    'SimpleNamespace': SimpleNamespace,
    'contextmanager': contextmanager,
    'ThreadHelper': lambda: SimpleNamespace(submit=lambda *args: None),
    'logger': SimpleNamespace(info=lambda *args: None, warning=lambda *args: None),
}
exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])), '__init__.py', 'exec'), namespace)
Task = namespace['BrushTaskConfig']
Plugin = namespace['OriechoBrushFlow']


class Flow(Plugin):
    DATA_SCHEMA_VERSION = 3
    TASK_DATA_NAMES = ('torrents', 'archived', 'unmanaged', 'statistic', 'runs', 'rotation', 'metrics', 'source_scan')

    def __init__(self, tasks=(), defaults=None):
        self._global_defaults = {key: models.DEFAULT_TASK_SETTINGS[key] for key in models.SHARED_TASK_FIELDS}
        self._global_defaults.update(defaults or {'downloader': 'tb'})
        self._task_configs = {task.id: task for task in tasks}
        self._enabled = True
        self._show_sidebar_nav = True
        self._rotation_mode = 'sequential'
        self._manual_focus_site_id = None
        self._selected_site_ids = None
        self._global_proxy_delete = False
        self._global_delete_size_range = None
        self._global_disksize = None
        self._brush_lock = threading.Lock()
        self._settings_lock = threading.Lock()
        self._settings_request_lock = threading.Lock()
        self._settings_request_active = False
        self._settings_yield = threading.Event()
        self._runtime_lock = threading.Lock()
        self._task_locks = {task.id: threading.Lock() for task in tasks}
        self._runtime = {task.id: {'state': 'idle'} for task in tasks}
        self._schedule_started = datetime.now()
        self.records = {}
        self.config_writes = []
        self.refreshes = 0
        self.fail_save = False
        self.checked = []
        self.sites = [dict(id=i, name=f'Site {i}', public=False) for i in (1, 2, 3)]
        namespace['SitesHelper'] = lambda: SimpleNamespace(get_indexers=lambda: self.sites)
        namespace['SiteOper'] = lambda: SimpleNamespace(get=lambda site_id: next(
            (SimpleNamespace(**site) for site in self.sites if site['id'] == site_id), None))
        namespace['DownloaderHelper'] = lambda: SimpleNamespace(get_configs=lambda: {
            'tb': SimpleNamespace(name='tb'), 'qb': SimpleNamespace(name='qb')})

    def _get_task_data(self, task_id, key):
        return self.records.get(f'task.{task_id}.{key}')

    def _task_data_key(self, task_id, key):
        return f'task.{task_id}.{key}'

    def _save_task_data(self, task_id, key, value):
        self.records[self._task_data_key(task_id, key)] = deepcopy(value)

    def del_data(self, key):
        self.records.pop(key, None)

    def _cleanup_unused_task_tag(self, task):
        pass

    def get_data(self, key):
        return self.records.get(key)

    def save_data(self, key, value):
        self.records[key] = deepcopy(value)

    def update_config(self, config):
        if self.fail_save:
            raise RuntimeError('offline storage failure')
        self.config_writes.append(deepcopy(config))

    def _save_config(self):
        self.update_config(self._current_config())

    def _refresh_scheduler(self):
        self.refreshes += 1

    def _restore_download_budgets(self):
        pass

    def _latest_site_user_data_by_domain(self):
        return {}

    def _build_rotation_status(self):
        return self.get_data('site_rotation') or {}

    def _build_manual_focus_status(self):
        return {}

    def _task_summary(self, task_id, **kwargs):
        task = self._task_configs[task_id]
        return {'id': task.id, 'site_id': task.site_id, 'enabled': task.enabled, 'priority': 2}

    def _build_site_ratio_status(self, task, data):
        return data[task.site_id]

    def _next_promotion_expiry(self, task):
        return None

    def _queue_brush(self, *args):
        pass

    def _dispatch_brush(self, *args):
        pass

    def _get_task_config(self, task_id):
        return self._task_configs.get(task_id)

    def _new_run_report(self, operation):
        return {}

    def _set_runtime(self, task_id, **values):
        self._runtime.setdefault(task_id, {}).update(values)

    def _task_scope(self, task_id):
        return nullcontext()

    def _run_check(self, task, report):
        self.checked.append((task.id, task.downloader))

    def _now_iso(self):
        return 'offline'

    def _append_run(self, task_id, report):
        pass

    def _build_task_detail(self, task_id):
        return self._task_configs[task_id].effective_dict()


def task(site_id=1, **values):
    return Task({'id': f'task-{site_id}', 'name': f'Site {site_id}', 'site_id': site_id,
                 'enabled': True, **values}, {'downloader': 'tb'})


class SharedSiteSettingsTests(unittest.TestCase):
    def payload(self, **values):
        return models.BrushFlowSettingsPayload.model_validate(values)

    def test_settings_distinguishes_omitted_selection_and_explicit_empty(self):
        self.assertIsNone(self.payload().selected_site_ids)
        self.assertEqual(self.payload(selected_site_ids=[]).selected_site_ids, [])
        self.assertEqual(self.payload(selected_site_ids=[2, 1, 2]).selected_site_ids, [2, 1])

    def test_selection_rejects_non_positive_or_boolean_ids(self):
        for value in ([0], [-1], [True], [1.5]):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.payload(selected_site_ids=value)

    def test_shared_fields_ignore_independent_flags_and_preserve_site_metadata(self):
        configured = task(use_global_defaults=False, override_fields=['size', 'downloader'],
                          downloader='qb', size='20-40', rss_support=True, timezone_offset=8,
                          site_hr_active=True, site_skip_tips=True, tag='keep-original')
        self.assertEqual(configured.downloader, 'tb')
        self.assertEqual(configured.size, models.DEFAULT_TASK_SETTINGS['size'])
        self.assertEqual(configured.tag, 'keep-original')
        self.assertEqual(configured.timezone_offset, 8)
        self.assertTrue(configured.rss_support and configured.site_hr_active and configured.site_skip_tips)
        stored = configured.to_dict()
        self.assertTrue(set(stored).isdisjoint(models.SHARED_TASK_FIELDS))

    def test_removed_fields_are_absent_from_runtime_and_serialization(self):
        configured = task(novice=True, novice_until='2099-01-01', maxupspeed=100, site_ratio_control=False)
        for name in ('novice', 'novice_until', 'site_ratio_control', 'maxupspeed', 'maxdlspeed', 'maxdlcount', 'up_speed', 'dl_speed'):
            self.assertFalse(hasattr(configured, name), name)
            self.assertNotIn(name, configured.effective_dict())
            self.assertNotIn(name, models.BrushTaskPayload.model_fields)

    def test_legacy_task_write_apis_cannot_replace_shared_rules_or_site_metadata(self):
        flow = Flow([task(1, rss_support=True, tag='stable')])
        before = deepcopy(flow._current_config())
        request = models.BrushTaskPayload.model_validate({'name': 'edit', 'site_id': 1,
                                                          'use_global_defaults': False, 'size': '999'})
        self.assertFalse(flow.update_task('task-1', request).success)
        self.assertFalse(flow.create_task(request).success)
        self.assertEqual(flow._current_config(), before)
        self.assertEqual(flow.config_writes, [])

    def test_first_status_defaults_all_sites_but_explicit_empty_stays_empty(self):
        flow = Flow()
        self.assertEqual(flow._build_status_data()['selected_site_ids'], [1, 2, 3])
        self.assertTrue(flow.update_settings(self.payload(selected_site_ids=[])).success)
        self.assertEqual(flow._build_status_data()['selected_site_ids'], [])
        self.assertEqual(flow.config_writes[-1]['selected_site_ids'], [])

    def test_status_of_existing_tasks_returns_only_enabled_site_ids(self):
        flow = Flow([task(1), task(2, enabled=False)])
        self.assertEqual(flow._build_status_data()['selected_site_ids'], [1])

    def test_select_reuse_disable_and_repeat_preserve_every_history_key(self):
        old = task(1, tag='original-tag', rss_support=True, timezone_offset=9)
        flow = Flow([old, task(2)])
        flow.records = {'source_budget.1': {'requests': [10, 40]},
                        'task.task-1.source_scan': {'lanes': {'regular': {'page': 7}}},
                        'task.task-1.torrents': {'h': {'deleted': False}},
                        'task.task-1.unknown': {'retained': True}}
        before = deepcopy(flow.records)
        payload = self.payload(selected_site_ids=[1, 3], global_defaults={'downloader': 'tb', 'size': '30-90'})
        self.assertTrue(flow.update_settings(payload).success)
        ids = {t.site_id: t.id for t in flow._task_configs.values()}
        self.assertEqual(ids[1], old.id)
        self.assertEqual(len(ids), 3)
        self.assertFalse(flow._task_configs['task-2'].enabled)
        self.assertTrue(flow._task_configs[ids[3]].enabled)
        self.assertEqual(flow._task_configs[old.id].tag, 'original-tag')
        self.assertTrue(flow._task_configs[old.id].rss_support)
        self.assertEqual(flow._task_configs[old.id].timezone_offset, 9)
        for item in flow._task_configs.values():
            self.assertEqual(item.size, '30-90')
        self.assertTrue(flow.update_settings(payload).success)
        self.assertEqual({t.site_id: t.id for t in flow._task_configs.values()}, ids)
        for key, value in before.items():
            self.assertEqual(flow.records[key], value)

    def test_omitted_selection_preserves_paused_tasks(self):
        flow = Flow([task(1), task(2, enabled=False)])
        self.assertTrue(flow.update_settings(self.payload(global_defaults={'downloader': 'tb', 'size': '50'})).success)
        self.assertEqual([(t.site_id, t.enabled) for t in flow._task_configs.values()], [(1, True), (2, False)])

    def test_invalid_selected_site_rejects_all_changes(self):
        flow = Flow([task(1)])
        before = deepcopy(flow._current_config())
        result = flow.update_settings(self.payload(selected_site_ids=[1, 999], global_defaults={'downloader': 'tb', 'size': '50'}))
        self.assertFalse(result.success)
        self.assertEqual(flow._current_config(), before)
        self.assertEqual(flow.config_writes, [])
        self.assertEqual(flow.records, {})

    def test_active_downloads_block_switch_even_when_site_deselected(self):
        flow = Flow([task(1)])
        flow.records['task.task-1.torrents'] = {'hash': {'deleted': False}}
        before = deepcopy(flow._current_config())
        result = flow.update_settings(self.payload(selected_site_ids=[], global_defaults={'downloader': 'qb'}))
        self.assertFalse(result.success)
        self.assertEqual(flow._current_config(), before)
        self.assertEqual(flow.config_writes, [])

    def test_locked_task_rejects_without_partial_selection(self):
        flow = Flow([task(1)])
        with flow._task_locks['task-1']:
            self.assertFalse(flow.update_settings(self.payload(selected_site_ids=[1, 2])).success)
        self.assertEqual(len(flow._task_configs), 1)
        self.assertEqual(flow.config_writes, [])
        self.assertTrue(flow.update_settings(self.payload(selected_site_ids=[1, 2])).success)

    def test_persist_failure_leaves_runtime_and_business_data_unchanged(self):
        flow = Flow([task(1)])
        before = deepcopy(flow._current_config())
        flow.fail_save = True
        with self.assertRaises(RuntimeError):
            flow.update_settings(self.payload(selected_site_ids=[2], global_defaults={'downloader': 'tb', 'size': '50'}))
        self.assertEqual(flow._current_config(), before)
        self.assertEqual(flow.records, {})
        self.assertEqual(flow.refreshes, 0)

    def test_focus_save_failure_preserves_selection_and_request(self):
        flow = Flow([task(1), task(2)])
        flow._stopping = False
        flow._manual_focus_site_id = 1
        flow.records['manual_focus_request'] = {'site_id': 1, 'pending': False}
        before, records = deepcopy(flow._current_config()), deepcopy(flow.records)
        queued = []
        flow._queue_brush = queued.append
        flow.fail_save = True
        with self.assertRaises(RuntimeError):
            flow.update_focus(models.BrushFlowFocusPayload(manual_focus_site_id=2))
        self.assertEqual(flow._current_config(), before)
        self.assertEqual(flow.records, records)
        self.assertEqual(queued, [])

    def test_state_save_failure_restores_enabled_flag(self):
        flow = Flow([task(1)])
        before = deepcopy(flow._current_config())
        flow.fail_save = True
        with self.assertRaises(RuntimeError):
            flow.update_task_state('task-1', models.BrushTaskStatePayload(enabled=False))
        self.assertEqual(flow._current_config(), before)
        self.assertEqual(flow.refreshes, 0)

    def test_delete_save_failure_preserves_task_and_history(self):
        flow = Flow([task(1)])
        flow.records['task.task-1.source_scan'] = {'page': 9}
        before, records = deepcopy(flow._current_config()), deepcopy(flow.records)
        flow.fail_save = True
        with self.assertRaises(RuntimeError):
            flow.delete_task('task-1')
        self.assertEqual(flow._current_config(), before)
        self.assertEqual(flow.records, records)
        self.assertEqual(flow.refreshes, 0)

    def test_concurrent_settings_and_state_keep_sqlite_and_runtime_consistent(self):
        flow = Flow([task(1)])
        db = sqlite3.connect(':memory:', check_same_thread=False)
        self.addCleanup(db.close)
        db.execute('CREATE TABLE config(value TEXT)')
        db.execute('INSERT INTO config VALUES (?)', (json.dumps(flow._current_config()),))
        db.commit()
        committed = threading.Event()
        release_settings = threading.Event()
        state_reached_boundary = threading.Event()
        results, errors = {}, []
        original_lock = flow._runtime_lock

        class ObservedLock:
            # A real lock plus an event lets the test schedule either the old
            # unlocked write or the corrected waiting write without sleep races.
            def __enter__(self):
                if threading.current_thread().name == 'state-write':
                    state_reached_boundary.set()
                original_lock.acquire()

            def __exit__(self, *args):
                original_lock.release()

        flow._runtime_lock = ObservedLock()

        def persist(config):
            db.execute('UPDATE config SET value=?', (json.dumps(config),))
            db.commit()
            if threading.current_thread().name == 'settings-write':
                committed.set()
                if not release_settings.wait(5):
                    raise RuntimeError('test release timeout')

        def run(name, operation):
            try:
                results[name] = operation()
            except BaseException as error:
                errors.append(error)
            finally:
                if name == 'state':
                    state_reached_boundary.set()

        flow.update_config = persist
        settings_thread = threading.Thread(name='settings-write', daemon=True, target=run, args=(
            'settings', lambda: flow.update_settings(self.payload(selected_site_ids=[1],
                global_defaults={'downloader': 'tb', 'size': '50'}))))
        state_thread = threading.Thread(name='state-write', daemon=True, target=run, args=(
            'state', lambda: flow.update_task_state('task-1', models.BrushTaskStatePayload(enabled=False))))
        try:
            settings_thread.start()
            self.assertTrue(committed.wait(5))
            state_thread.start()
            self.assertTrue(state_reached_boundary.wait(5))
        finally:
            release_settings.set()
            settings_thread.join(5)
            if state_thread.ident:
                state_thread.join(5)
        self.assertFalse(settings_thread.is_alive() or state_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(results['settings'].success and results['state'].success)
        persisted = json.loads(db.execute('SELECT value FROM config').fetchone()[0])
        self.assertEqual(persisted, flow._current_config())
        self.assertEqual(persisted['global_defaults']['size'], '50')
        self.assertFalse(flow._task_configs['task-1'].enabled)

    def test_delete_cannot_race_settings_and_resurrect_a_deleted_task(self):
        flow = Flow([task(1)])
        flow.records['task.task-1.source_scan'] = {'page': 9}
        delete_read = threading.Event()
        release_delete = threading.Event()
        settings_progress = threading.Event()
        release_settings = threading.Event()
        original_read, original_write = flow._get_task_data, flow.update_config
        results, errors = {}, []

        def read(task_id, name):
            value = original_read(task_id, name)
            if threading.current_thread().name == 'delete-write' and name == 'torrents':
                delete_read.set()
                if not release_delete.wait(5):
                    raise RuntimeError('test delete timeout')
            return value

        def persist(config):
            original_write(config)
            if threading.current_thread().name == 'settings-write':
                settings_progress.set()
                if not release_settings.wait(5):
                    raise RuntimeError('test settings timeout')

        def run(name, operation):
            try:
                results[name] = operation()
            except BaseException as error:
                errors.append(error)
            finally:
                if name == 'settings':
                    settings_progress.set()

        flow._get_task_data, flow.update_config = read, persist
        delete_thread = threading.Thread(name='delete-write', daemon=True, target=run,
                                         args=('delete', lambda: flow.delete_task('task-1')))
        settings_thread = threading.Thread(name='settings-write', daemon=True, target=run,
            args=('settings', lambda: flow.update_settings(self.payload(selected_site_ids=[1, 2],
                                          global_defaults={'downloader': 'tb', 'size': '50'}))))
        try:
            delete_thread.start()
            self.assertTrue(delete_read.wait(5))
            settings_thread.start()
            self.assertTrue(settings_progress.wait(5))
            release_delete.set()
            delete_thread.join(5)
        finally:
            release_delete.set()
            release_settings.set()
            delete_thread.join(5)
            if settings_thread.ident:
                settings_thread.join(5)
        self.assertFalse(delete_thread.is_alive() or settings_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(results['delete'].success)
        self.assertNotIn('task-1', flow._task_configs)
        self.assertEqual(flow.config_writes[-1], flow._current_config())

    def test_delete_and_clear_reject_busy_background_without_waiting(self):
        for action in ('delete_task', 'clear_task_data'):
            for lock_name in ('brush', 'task'):
                with self.subTest(action=action, lock=lock_name):
                    flow = Flow([task(1)])
                    flow.records['task.task-1.source_scan'] = {'page': 9}
                    before = deepcopy(flow.records)
                    lock = flow._brush_lock if lock_name == 'brush' else flow._task_locks['task-1']
                    responses = []
                    with lock:
                        worker = threading.Thread(daemon=True, target=lambda: responses.append(getattr(flow, action)('task-1')))
                        worker.start()
                        worker.join(1)
                        self.assertFalse(worker.is_alive(), 'API must not wait for the background task')
                    self.assertFalse(responses[0].success)
                    self.assertEqual(flow.records, before)

    def test_deselecting_manual_focus_clears_focus_without_deleting_data(self):
        flow = Flow([task(1), task(2)])
        flow._manual_focus_site_id = 1
        flow.records['source_budget.1'] = {'requests': [10]}
        result = flow.update_settings(self.payload(selected_site_ids=[2]))
        self.assertTrue(result.success)
        self.assertIsNone(flow._manual_focus_site_id)
        self.assertEqual(flow.records['source_budget.1'], {'requests': [10]})

    def test_legacy_active_downloader_is_retained_across_reload_until_safe(self):
        flow = Flow()
        row = {'id': 'legacy', 'name': 'Legacy', 'site_id': 1, 'enabled': True,
               'downloader': 'qb', 'use_global_defaults': False, 'rss_support': True, 'tag': 'stable'}
        flow.records['task.legacy.torrents'] = {'hash': {'deleted': False}}
        loaded = flow._load_task_config(row)
        self.assertEqual(loaded.downloader, 'qb')
        self.assertFalse(loaded.enabled)
        self.assertTrue(loaded.downloader_conflict)
        reloaded = flow._load_task_config(loaded.to_dict())
        self.assertEqual(reloaded.downloader, 'qb')
        self.assertEqual(reloaded.tag, 'stable')
        flow._task_configs = {'legacy': reloaded}
        self.assertFalse(flow.update_task_state('legacy', models.BrushTaskStatePayload(enabled=True)).success)
        jobs = flow.get_service()
        self.assertTrue(any(job['id'] == 'Task_legacy_Check' for job in jobs))
        self.assertFalse(any(job['id'] == 'Task_legacy_Brush' for job in jobs))
        flow.check('legacy')
        self.assertEqual(flow.checked, [('legacy', 'qb')])
        self.assertFalse(flow.update_settings(self.payload(selected_site_ids=[1])).success)
        flow.records['task.legacy.torrents']['hash']['deleted'] = True
        self.assertTrue(flow.update_settings(self.payload(selected_site_ids=[1])).success)
        self.assertEqual(flow._task_configs['legacy'].downloader, 'tb')
        self.assertTrue(flow._task_configs['legacy'].enabled)

    def test_novice_no_longer_preempts_ratio_and_legacy_phase_keeps_visit_cooldown(self):
        flow = Flow([task(1, novice=True), task(2)])
        ratios = {1: {'available': True, 'stale': False, 'reached': True},
                  2: {'available': True, 'stale': False, 'reached': False}}
        self.assertEqual(flow._priority_tier(flow._task_configs['task-1'], ratios[1]), 2)
        flow.records['site_rotation'] = {'phase': 'novice', 'site_id': 1, 'visited_site_ids': [1],
                                         'retry_cooldowns': {'1': 200}}
        self.assertEqual(flow._select_rotation_site_locked(ratios, now=100), 2)
        state = flow.records['site_rotation']
        self.assertEqual(state['phase'], 'ratio')
        self.assertIn(1, state['visited_site_ids'])
        self.assertEqual(state['retry_cooldowns']['1'], 200)


if __name__ == '__main__':
    unittest.main(verbosity=2)
