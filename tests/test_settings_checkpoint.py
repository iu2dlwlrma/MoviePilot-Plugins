"""Real dispatcher/settings concurrency at persisted source-page checkpoints."""
from copy import deepcopy
import threading
import time
from types import SimpleNamespace
import unittest

import test_brush_paging as paging
import test_shared_site_settings as shared


class CheckpointFlow(paging.OfflineFlow, shared.Flow):
    def __init__(self, source):
        defaults = {'downloader': 'tb', 'del_no_free': False}
        tasks = [shared.Task({'id': 'task-1', 'name': 'Site 1', 'site_id': 1, 'enabled': True}, defaults)]
        shared.Flow.__init__(self, tasks, defaults=defaults)
        paging.OfflineFlow.__init__(self, tasks, source)
        self._settings_lock = threading.Lock()
        self._settings_request_lock = threading.Lock()
        self._settings_request_active = False
        self._settings_yield = threading.Event()
        shared.namespace['SiteOper'] = lambda: SimpleNamespace(get=self.sites.get)
        shared.namespace['DownloaderHelper'] = lambda: SimpleNamespace(get_configs=lambda: {'tb': SimpleNamespace(name='tb')})

    _validate_task_reference = shared.Plugin._validate_task_reference
    _read_downloads = lambda self, **kwargs: {'tb': {}}
    _build_status_data = lambda self: self._current_config()


class SettingsCheckpointTests(unittest.TestCase):
    def setUp(self):
        paging.clock.now = 100000.0

    def payload(self):
        return shared.models.BrushFlowSettingsPayload(selected_site_ids=[1],
            global_defaults={'downloader': 'tb', 'size': '0-100', 'del_no_free': False})

    def start_source(self):
        source = paging.SourceBoundary({'https://task-1.invalid': {
            0: [paging.torrent('first', peers=30), paging.torrent('remaining')],
            1: [paging.torrent('later')]}})
        flow = CheckpointFlow(source)
        entered, release = threading.Event(), threading.Event()
        def hold_response():
            entered.set()
            if not release.wait(8):
                raise RuntimeError('test source release timeout')
        source.after_browse = hold_response
        dispatcher = threading.Thread(target=flow._dispatch_brush, daemon=True)
        dispatcher.start()
        self.assertTrue(entered.wait(2))
        def cleanup():
            release.set()
            dispatcher.join(3)
        self.addCleanup(cleanup)
        return flow, source, dispatcher, release

    def save_at_response(self, flow, release):
        boundary = threading.Event()
        original_set = flow._settings_yield.set
        def signal():
            original_set()
            boundary.set()
        flow._settings_yield.set = signal
        results, errors = [], []
        def save():
            try:
                results.append(flow.update_settings(self.payload()))
            except BaseException as err:
                errors.append(err)
            finally:
                boundary.set()
        writer = threading.Thread(target=save, daemon=True)
        writer.start()
        self.assertTrue(boundary.wait(2))
        release.set()
        writer.join(5)
        self.assertFalse(writer.is_alive())
        return results, errors

    def test_continuous_candidates_yield_and_save_without_losing_cursor_or_budget(self):
        flow, source, dispatcher, release = self.start_source()
        results, errors = self.save_at_response(flow, release)
        dispatcher.join(3)
        self.assertEqual(errors, [])
        self.assertTrue(results[0].success)
        self.assertFalse(dispatcher.is_alive())
        self.assertFalse(flow._settings_yield.is_set())
        self.assertIn('task-1', flow._pending_brush)
        self.assertEqual(len(flow.storage['source_budget.1']['requests']), 1)
        source.after_browse = None
        paging.clock.advance()
        flow._dispatch_brush()
        self.assertCountEqual(flow.added, [('task-1', 'first'), ('task-1', 'remaining')])
        paging.clock.advance()
        flow._dispatch_brush()
        self.assertIn(('task-1', 'later'), flow.added)
        self.assertEqual(len(flow.added), 3)
        self.assertEqual(paging.logger.errors, [])

    def test_late_response_consumes_timeout_intent_without_saving_rejected_payload(self):
        flow, source, dispatcher, release = self.start_source()
        before = deepcopy(flow._current_config())
        started = time.monotonic()
        result = flow.update_settings(self.payload())
        elapsed = time.monotonic() - started
        self.assertFalse(result.success)
        self.assertLess(elapsed, 4)
        self.assertTrue(flow._settings_yield.is_set(), 'late response must still yield once')
        self.assertEqual(flow.config_writes, [])
        release.set()
        dispatcher.join(3)
        self.assertFalse(dispatcher.is_alive())
        self.assertEqual(flow._current_config(), before)
        self.assertFalse(flow._settings_yield.is_set())
        self.assertEqual(flow.added, [('task-1', 'first')])
        self.assertTrue(flow.update_settings(self.payload()).success)
        self.assertEqual(len(flow.config_writes), 1)

    def test_old_dispatcher_cleanup_cannot_clear_a_new_save_request(self):
        flow, source, dispatcher, release = self.start_source()
        self.assertFalse(flow.update_settings(self.payload()).success)
        cleanup_entered, finish_cleanup = threading.Event(), threading.Event()
        saving, finish_save = threading.Event(), threading.Event()
        original_consume, original_write = flow._consume_settings_yield, flow.update_config

        def hold_cleanup():
            cleanup_entered.set()
            if not finish_cleanup.wait(5):
                raise RuntimeError('test cleanup release timeout')
            original_consume()

        def hold_save(config):
            saving.set()
            if not finish_save.wait(5):
                raise RuntimeError('test save release timeout')
            original_write(config)

        flow._consume_settings_yield = hold_cleanup
        flow.update_config = hold_save
        results = []
        writer = threading.Thread(target=lambda: results.append(flow.update_settings(self.payload())), daemon=True)
        try:
            release.set()
            self.assertTrue(cleanup_entered.wait(2))
            writer.start()
            self.assertTrue(saving.wait(2))
            competing = flow.update_settings(self.payload())
            self.assertFalse(competing.success)
            finish_cleanup.set()
            dispatcher.join(2)
            self.assertFalse(dispatcher.is_alive())
            self.assertTrue(flow._settings_yield.is_set())
            self.assertTrue(flow._settings_request_active)
        finally:
            finish_cleanup.set()
            finish_save.set()
            if writer.ident:
                writer.join(3)
        self.assertTrue(results[0].success)
        self.assertEqual(len(flow.config_writes), 1)
        self.assertFalse(flow._settings_yield.is_set())
        self.assertFalse(flow._settings_request_active)

    def test_storage_failure_releases_request_and_scan_remains_resumable(self):
        flow, source, dispatcher, release = self.start_source()
        before = deepcopy(flow._current_config())
        flow.fail_save = True
        results, errors = self.save_at_response(flow, release)
        dispatcher.join(3)
        self.assertEqual(results, [])
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)
        self.assertEqual(flow._current_config(), before)
        self.assertFalse(flow._settings_yield.is_set())
        self.assertFalse(flow._settings_lock.locked())
        flow.fail_save = False
        self.assertTrue(flow.update_settings(self.payload()).success)
        source.after_browse = None
        paging.clock.advance()
        flow._dispatch_brush()
        self.assertCountEqual(flow.added, [('task-1', 'first'), ('task-1', 'remaining')])


if __name__ == '__main__':
    unittest.main()
