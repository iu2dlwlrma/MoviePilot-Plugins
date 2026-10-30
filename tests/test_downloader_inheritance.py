"""Offline tests: real config/model/state methods; no host bootstrap or network.

Run with Python + Pydantic v2. SOURCE_TEXT / MODELS_TEXT may be injected for
in-memory execution in the host container without writing files there.
"""
import ast
import math
from pathlib import Path
import sys
import threading
from types import ModuleType, SimpleNamespace
import unittest
import uuid

ROOT = Path(globals().get('__file__', '.')).resolve().parents[1] / 'plugins.v3' / 'brushflow'
source_text = globals().get('SOURCE_TEXT') or (ROOT / '__init__.py').read_text()
models_text = globals().get('MODELS_TEXT') or (ROOT / 'models.py').read_text()
models = ModuleType('offline_downloader_models')
sys.modules[models.__name__] = models
exec(compile(models_text, 'models.py', 'exec'), models.__dict__)
module = ast.parse(source_text)
task_class = next(node for node in module.body if isinstance(node, ast.ClassDef) and node.name == 'BrushTaskConfig')
plugin_class = next(node for node in module.body if isinstance(node, ast.ClassDef) and node.name == 'OriechoBrushFlow')
methods = {'update_task_state', '_validate_task_reference', '_downloader_change_has_active_torrents'}
selected = [node for node in plugin_class.body if isinstance(node, ast.FunctionDef) and node.name in methods]
fields = next(node for node in module.body if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == 'TASK_CONFIG_FIELDS' for target in node.targets))
extracted = ast.Module(body=[
    ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0),
    fields,
    task_class,
    ast.ClassDef(name='OriechoBrushFlow', bases=[], keywords=[], body=selected, decorator_list=[]),
], type_ignores=[])
namespace = {
    'DEFAULT_TASK_SETTINGS': models.DEFAULT_TASK_SETTINGS,
    'SHARED_TASK_FIELDS': models.SHARED_TASK_FIELDS,
    'uuid': uuid, 'math': math,
    'schemas': SimpleNamespace(Response=lambda **kwargs: SimpleNamespace(**kwargs)),
}
exec(compile(ast.fix_missing_locations(extracted), '__init__.py', 'exec'), namespace)
BrushTaskConfig = namespace['BrushTaskConfig']
OriechoBrushFlow = namespace['OriechoBrushFlow']
BASE = {'id': 'offline', 'name': '离线测试', 'site_id': 1, 'enabled': False, 'use_global_defaults': True}


class OfflineStateFlow(OriechoBrushFlow):
    def __init__(self, task, configs=None, site=None):
        self._task_configs = {task.id: task}
        self._runtime_lock = threading.Lock()
        self.config_writes = 0
        self.scheduler_refreshes = 0
        self.records = {}
        namespace['DownloaderHelper'] = lambda: SimpleNamespace(get_configs=lambda: configs or {})
        namespace['SiteOper'] = lambda: SimpleNamespace(get=lambda site_id: site)

    def _current_config(self):
        return {"tasks": [task.to_dict() for task in self._task_configs.values()]}

    def update_config(self, config):
        self.config_writes += 1

    def _refresh_scheduler(self):
        self.scheduler_refreshes += 1

    def _build_task_detail(self, task_id):
        task = self._task_configs[task_id]
        return {'enabled': task.enabled, 'downloader': task.downloader}

    def _get_task_data(self, task_id, name):
        return self.records

    def _log_and_notify_error(self, message):
        pass


class DownloaderInheritanceTests(unittest.TestCase):
    def test_legacy_empty_downloader_does_not_become_inferred_override(self):
        for value in ('', '   ', None):
            with self.subTest(value=value):
                task = BrushTaskConfig({**BASE, 'downloader': value}, {'downloader': 'tb'})
                self.assertEqual(task.downloader, 'tb')
                self.assertNotIn('downloader', task.override_fields)

    def test_explicit_blank_override_inherits_all_shared_parameters(self):
        for value in ('', '   ', None):
            with self.subTest(value=value):
                task = BrushTaskConfig({**BASE, 'downloader': value, 'size': None,
                                        'override_fields': ['downloader', 'size']},
                                       {'downloader': 'tb', 'size': '20-100'})
                self.assertEqual(task.downloader, 'tb')
                self.assertEqual(task.override_fields, [])
                self.assertEqual(task.size, '20-100')

    def test_missing_downloader_with_stale_override_inherits(self):
        task = BrushTaskConfig({**BASE, 'override_fields': ['downloader']}, {'downloader': 'tb'})
        self.assertEqual(task.downloader, 'tb')
        self.assertNotIn('downloader', task.override_fields)

    def test_explicit_task_downloader_cannot_override_common_downloader(self):
        for overrides in (None, ['downloader']):
            row = {**BASE, 'downloader': ' qb '}
            if overrides is not None:
                row['override_fields'] = overrides
            task = BrushTaskConfig(row, {'downloader': 'tb'})
            self.assertEqual(task.downloader, 'tb')
            self.assertEqual(task.override_fields, [])

    def test_legacy_independent_flag_also_inherits_common_downloader(self):
        task = BrushTaskConfig({**BASE, 'use_global_defaults': False, 'downloader': ''}, {'downloader': 'tb'})
        self.assertEqual(task.downloader, 'tb')
        task = BrushTaskConfig({**BASE, 'use_global_defaults': False, 'downloader': 'qb'}, {'downloader': 'tb'})
        self.assertEqual(task.downloader, 'tb')

    def test_model_removes_all_legacy_override_fields(self):
        for value in ('', '   ', None):
            with self.subTest(value=value):
                payload = models.BrushTaskPayload.model_validate({**BASE, 'downloader': value,
                                                                  'override_fields': ['downloader', 'size']})
                self.assertEqual(payload.override_fields, [])
                task = BrushTaskConfig(payload.model_dump(), {'downloader': 'tb'})
                self.assertEqual(task.downloader, 'tb')

    def test_model_missing_downloader_removes_stale_override(self):
        payload = models.BrushTaskPayload.model_validate({**BASE, 'override_fields': ['downloader']})
        self.assertEqual(payload.override_fields, [])

    def test_model_keeps_downloader_value_for_common_settings_but_removes_override(self):
        payload = models.BrushTaskPayload.model_validate({**BASE, 'downloader': ' qb ', 'override_fields': ['downloader']})
        self.assertEqual(payload.downloader, 'qb')
        self.assertEqual(payload.override_fields, [])

    def test_model_normalizes_legacy_independent_flag_to_common_settings(self):
        for value in ('', '   ', None):
            with self.subTest(value=value):
                payload = models.BrushTaskPayload.model_validate({**BASE, 'use_global_defaults': False, 'downloader': value})
                self.assertTrue(payload.use_global_defaults)
                self.assertEqual(payload.override_fields, [])

    def test_invalid_task_cannot_be_enabled_or_saved(self):
        cases = [
            ('', {'tb': object()}, SimpleNamespace(public=False)),
            ('missing', {'tb': object()}, SimpleNamespace(public=False)),
            ('tb', {'tb': object()}, None),
            ('tb', {'tb': object()}, SimpleNamespace(public=True)),
        ]
        for name, configs, site in cases:
            with self.subTest(downloader=name, site=site):
                task = BrushTaskConfig(BASE, {'downloader': name})
                flow = OfflineStateFlow(task, configs=configs, site=site)
                result = flow.update_task_state(task.id, models.BrushTaskStatePayload(enabled=True))
                self.assertFalse(result.success)
                self.assertFalse(task.enabled)
                self.assertEqual(flow.config_writes, 0)
                self.assertEqual(flow.scheduler_refreshes, 0)

    def test_valid_inherited_downloader_can_be_enabled(self):
        task = BrushTaskConfig({**BASE, 'downloader': '', 'override_fields': ['downloader']}, {'downloader': 'tb'})
        flow = OfflineStateFlow(task, configs={'tb': object()}, site=SimpleNamespace(public=False))
        result = flow.update_task_state(task.id, models.BrushTaskStatePayload(enabled=True))
        self.assertTrue(result.success)
        self.assertEqual(result.data, {'enabled': True, 'downloader': 'tb'})
        self.assertEqual(flow.config_writes, 1)
        self.assertEqual(flow.scheduler_refreshes, 1)

    def test_invalid_task_can_still_be_paused(self):
        task = BrushTaskConfig({**BASE, 'enabled': True, 'use_global_defaults': False, 'downloader': ''})
        flow = OfflineStateFlow(task)
        result = flow.update_task_state(task.id, models.BrushTaskStatePayload(enabled=False))
        self.assertTrue(result.success)
        self.assertFalse(task.enabled)
        self.assertEqual(flow.config_writes, 1)

    def test_active_torrent_downloader_change_guard_is_preserved(self):
        task = BrushTaskConfig(BASE, {'downloader': 'tb'})
        other = BrushTaskConfig(BASE, {'downloader': 'qb'})
        flow = OfflineStateFlow(task)
        flow.records = {'hash': {'deleted': False}}
        self.assertTrue(flow._downloader_change_has_active_torrents(task, other))
        self.assertFalse(flow._downloader_change_has_active_torrents(task, task))
        flow.records = {'hash': {'deleted': True}}
        self.assertFalse(flow._downloader_change_has_active_torrents(task, other))


if __name__ == '__main__':
    unittest.main(verbosity=2)
