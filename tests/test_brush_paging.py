"""Offline regression tests executing unchanged AST methods from the supplied plugin.

Only MoviePilot/network/downloader/persistence boundaries are replaced. The real
scheduler, execution/report path, source queues, subscription/selection rules,
space checks, and task records run without importing the MoviePilot application.
Run: PYTHONDONTWRITEBYTECODE=1 python3 <this file>
Set BRUSHFLOW_SOURCE to a plugin __init__.py to check another source snapshot.
"""
import ast
import copy
import hashlib
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import os
from pathlib import Path
import re
import threading
import time
from types import SimpleNamespace
import unittest
import uuid

ROOT = Path(__file__).resolve().parent
DEFAULT_SOURCE = ROOT / 'brushflow' / '__init__.py'
if not DEFAULT_SOURCE.exists():
    DEFAULT_SOURCE = ROOT.parent / 'plugins.v3' / 'brushflow' / '__init__.py'
SOURCE = Path(os.environ.get('BRUSHFLOW_SOURCE', DEFAULT_SOURCE))
METHODS = {
    '_dispatch_brush', '_execute_brush', '_run_brush', '_load_all_torrent_tasks',
    '_settings_yield_requested', '_consume_settings_yield',
    '_calculate_global_seeding_size', '__brush_site_torrents', '_torrent_to_task_record',
    '_new_run_report', '_now_iso', '__evaluate_size_condition_for_brush',
    '__evaluate_pre_conditions_for_brush', '__evaluate_conditions_for_brush',
    '__filter_torrents_contains_subscribe', '__bytes_to_gb',
    '__calculate_seeding_torrents_size',
}
METHODS.update({'__download', '_queue_brush', '_change_source_scan', '_restore_source_scans', '_brush_task_enabled'})
module = ast.parse(SOURCE.read_text(encoding='utf-8'), filename=str(SOURCE))
original_class = next(node for node in module.body if isinstance(node, ast.ClassDef) and node.name == 'OriechoBrushFlow')
selected = [node for node in original_class.body if isinstance(node, ast.FunctionDef) and node.name in METHODS]

extracted = ast.Module(body=[
    ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0),
    ast.ClassDef(name='OriechoBrushFlow', bases=[], keywords=[], body=selected, decorator_list=[]),
], type_ignores=[])

spec = importlib.util.spec_from_file_location('offline_brush_rules', SOURCE.parent / 'rules.py')
rules = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rules)


class SourceBoundary:
    """Finite canned pages: unexpected pagination is observable and cannot hang."""
    def __init__(self, pages, rss=None):
        self.pages = pages
        self.rss_rows = rss or {}
        self.browse_requests = []
        self.rss_requests = []
        self.outcomes = {}
        self.after_browse = None

    def browse(self, *, domain, page=None):
        self.browse_requests.append((domain, page))
        observed = namespace.get('observation')
        outcome, has_more = self.outcomes.get((domain, page or 0), ('success', None))
        rows = list(self.pages.get(domain, {}).get(page or 0, []))
        if observed is not None:
            observed.attempted = outcome != 'deferred'
            observed.outcome = outcome
            observed.has_more = has_more
            observed.raw_count = len(rows)
        if self.after_browse:
            self.after_browse()
        if outcome == 'exception':
            raise RuntimeError('source error')
        return rows

    def rss(self, *, domain):
        self.rss_requests.append(domain)
        return list(self.rss_rows.get(domain, []))


class LogBoundary:
    def __init__(self):
        self.errors = []

    def info(self, *args):
        pass

    def warning(self, *args):
        pass

    def error(self, message):
        self.errors.append(message)


class FakeClock:
    def __init__(self):
        self.now = 100000.0
    def time(self):
        return self.now
    def sleep(self, seconds):
        raise AssertionError('Dispatcher must not sleep')
    def advance(self, seconds=30):
        self.now += seconds

clock = FakeClock()

@contextmanager
def capture_site_search_observation():
    value = SimpleNamespace(attempted=False, outcome='skipped', error=None, raw_count=None, has_more=None)
    namespace['observation'] = value
    try:
        yield value
    finally:
        namespace.pop('observation', None)

logger = LogBoundary()
namespace = {
    'Counter': Counter, 'datetime': datetime, 're': re, 'threading': threading,
    'time': clock, 'hashlib': hashlib, 'capture_site_search_observation': capture_site_search_observation, 'uuid': uuid, 'logger': logger,
    'settings': SimpleNamespace(TZ='UTC'),
    'EventType': SimpleNamespace(PluginTriggered='plugin_triggered'),
    'demand_rank': rules.demand_rank,
    'publication_age_minutes': rules.publication_age_minutes,
    'evaluate_peer_demand': rules.evaluate_peer_demand,
}
if (SOURCE.parent / 'paging.py').exists():
    paging_spec = importlib.util.spec_from_file_location('offline_brush_paging', SOURCE.parent / 'paging.py')
    paging = importlib.util.module_from_spec(paging_spec)
    paging_spec.loader.exec_module(paging)
    namespace.update({name: getattr(paging, name) for name in dir(paging) if not name.startswith('_')})
exec(compile(ast.fix_missing_locations(extracted), str(SOURCE), 'exec'), namespace)
OriechoBrushFlow = namespace['OriechoBrushFlow']


def task(task_id, site_id, **overrides):
    values = dict(
        id=task_id, name=task_id, site_id=site_id, enabled=True, downloader='local',
        check_interval=5, rss_support=False, size=None, disksize=None,
        except_subscribe=True, timezone_offset=0, freeleech='free',
        site_ratio_target=None, del_no_free=False, hr='yes', include=None,
        exclude=None, seeder=None, leecher='1', min_peer_seed_ratio=0.1,
        pubtime=None, site_hr_active=False,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def torrent(title, site='a', peers=10, **overrides):
    values = dict(
        title=title, site_name=site, description='', size=1024**3,
        page_url=f'https://{site}.invalid/t/{title}', enclosure=None,
        pubdate=datetime.now(timezone.utc).isoformat(), peers=peers, seeders=1,
        date_elapsed=0, freedate=None, uploadvolumefactor=1,
        downloadvolumefactor=0, hit_and_run=False, volume_factor=0,
        freedate_diff=None,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


class OfflineFlow(OriechoBrushFlow):
    def __init__(self, tasks, source, subscriptions=(), storage=None, queue=True):
        self._task_configs = {item.id: item for item in tasks}
        self._pending_brush = {}
        self.storage = copy.deepcopy(storage or {})
        self._task_context = SimpleNamespace()
        self._task_locks = {}
        self._brush_lock = threading.Lock()
        self._settings_lock = threading.Lock()
        self._settings_request_lock = threading.Lock()
        self._settings_request_active = False
        self._settings_yield = threading.Event()
        self._runtime_lock = threading.Lock()
        self._stopping = False
        self._global_disksize = None
        self._rotation_mode = 'parallel'
        self.records = {item.id: {} for item in tasks}
        self.added = []
        self.reports = []
        self.subscriptions = set(subscriptions)
        self.downloader = object()
        self.service_info = SimpleNamespace(name='local')
        self.eventmanager = SimpleNamespace(send_event=lambda **kwargs: None)
        self.sites = {
            item.site_id: SimpleNamespace(id=item.site_id, name=item.id, domain=f'https://{item.id}.invalid')
            for item in tasks
        }
        namespace['TorrentsChain'] = lambda: source
        namespace['SiteOper'] = lambda: SimpleNamespace(get=self.sites.get)
        namespace['SiteChain'] = lambda: SimpleNamespace(test=lambda domain: (True, ''))
        logger.errors.clear()
        if queue:
            for item in tasks:
                self._queue_brush(item.id)

    @contextmanager
    def _task_scope(self, task_id):
        self.current_task_id = task_id
        yield

    def _get_task_config(self, task_id=None):
        return self._task_configs.get(task_id or self.current_task_id)

    def _get_task_data(self, task_id, key):
        return self.records[task_id] if key == 'torrents' else self.get_data(f'task.{task_id}.{key}')

    def _current_task_data(self, key, default):
        value = self._get_task_data(self.current_task_id, key)
        return default if value is None else value

    def _save_current_task_data(self, key, value):
        self._save_task_data(self.current_task_id, key, value)

    def _save_task_data(self, task_id, key, value):
        if key == 'torrents':
            self.records[task_id] = value
        else:
            self.save_data(f'task.{task_id}.{key}', value)

    def get_data(self, key):
        return copy.deepcopy(self.storage.get(key))

    def save_data(self, key, value):
        self.storage[key] = copy.deepcopy(value)

    def _append_run(self, task_id, report):
        self.reports.append((task_id, report))

    def _OriechoBrushFlow__download(self, item):
        self.added.append((self.current_task_id, item.title))
        return item.page_url

    def _OriechoBrushFlow__get_subscribe_titles(self):
        return self.subscriptions

    get_state = lambda self: True
    _sample_transfers = lambda self: None
    _latest_site_user_data_by_domain = lambda self: {}
    _select_rotation_site = lambda self, data: None
    _refresh_focus_ratio = lambda self: None
    _check_rotation_site = lambda self: True
    _prioritize_download_queue = lambda self, data: None
    _is_focus_site = lambda self, site_id: True
    _priority_tier = lambda self, task, status: 0
    _build_site_ratio_status = lambda self, task, data: {'deficit_fraction': 0}
    _site_last_added = lambda self, site_id: 0
    _next_promotion_expiry = lambda self, task: None
    _read_downloads = lambda self, **kwargs: {'local': {}}
    _set_runtime = lambda self, *args, **kwargs: None
    _refresh_scheduler = lambda self: None
    _validate_task_reference = lambda self, task: True
    _is_current_time_in_range = lambda self, task: True
    _evaluate_site_ratio_status = lambda self, task, **kwargs: (True, None, {'enabled': False})
    _resolve_pending_downloads = lambda self, rows, snapshot: False
    _site_completed_count = lambda self, site_id: 0
    _reserve_download = lambda self, *args: None
    _recalculate_statistics = lambda self, task_id: None
    _OriechoBrushFlow__send_add_message = lambda self, item: None


class PagingRegression(unittest.TestCase):
    def setUp(self):
        clock.now = 100000.0

    def scan(self, flow, task_id='a'):
        return flow.storage.get(f'task.{task_id}.source_scan', {})

    def test_later_page_qualified_resources_are_reached_after_cooldown(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('first')], 1: [torrent('later')]}})
        flow = OfflineFlow([task('a', 1)], source)
        flow._dispatch_brush()
        self.assertEqual(flow.added, [('a', 'first')])
        self.assertIn('a', flow._pending_brush)
        clock.advance()
        flow._dispatch_brush()
        self.assertEqual(flow.added, [('a', 'first'), ('a', 'later')])
        clock.advance()
        flow._dispatch_brush()
        self.assertEqual(source.browse_requests, [('https://a.invalid', None), ('https://a.invalid', 1), ('https://a.invalid', 2)])
        self.assertNotIn('a', flow._pending_brush)

    def test_whole_page_filtered_still_continues(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('Subscribed.One')], 1: [torrent('later')]}})
        flow = OfflineFlow([task('a', 1)], source, subscriptions=['Subscribed'])
        flow._dispatch_brush()
        clock.advance()
        flow._dispatch_brush()
        self.assertEqual(flow.added, [('a', 'later')])

    def test_wait_does_no_request_sleep_or_history_spam(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('first')]}})
        flow = OfflineFlow([task('a', 1)], source)
        flow._dispatch_brush()
        for _ in range(5):
            flow._dispatch_brush()
        self.assertEqual(len(source.browse_requests), 1)
        self.assertEqual(len(flow.reports), 1)
        self.assertIn('a', flow._pending_brush)

    def test_stop_after_response_prevents_download_and_keeps_current_page(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('first')]}})
        flow = OfflineFlow([task('a', 1)], source)
        source.after_browse = lambda: setattr(flow, '_stopping', True)
        flow._dispatch_brush()
        self.assertEqual(flow.added, [])
        self.assertTrue(self.scan(flow).get('head', {}).get('pending'))

    def test_disable_after_response_prevents_download(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('first')]}})
        flow = OfflineFlow([task('a', 1)], source)
        source.after_browse = lambda: setattr(flow._task_configs['a'], 'enabled', False)
        flow._dispatch_brush()
        self.assertEqual(flow.added, [])

    def test_restart_current_page_skips_processed_candidate(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('first', peers=30), torrent('second')]}})
        flow = OfflineFlow([task('a', 1)], source)
        original = flow._OriechoBrushFlow__download
        def download(item):
            result = original(item)
            flow._stopping = True
            return result
        flow._OriechoBrushFlow__download = download
        flow._dispatch_brush()
        self.assertEqual(flow.added, [('a', 'first')])
        restarted = OfflineFlow([task('a', 1)], source, storage=flow.storage, queue=False)
        restarted.records = copy.deepcopy(flow.records)
        clock.advance()
        restarted._dispatch_brush()
        self.assertEqual(restarted.added, [('a', 'second')])
        self.assertEqual(source.browse_requests, [('https://a.invalid', None), ('https://a.invalid', None)])

    def test_error_retains_tail_page_until_backoff_expires(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('first')], 1: [torrent('later')]}})
        flow = OfflineFlow([task('a', 1)], source)
        flow._dispatch_brush()
        source.outcomes[('https://a.invalid', 1)] = ('error', None)
        source.pages['https://a.invalid'][1] = []
        clock.advance()
        flow._dispatch_brush()
        clock.advance()
        flow._dispatch_brush()
        self.assertEqual(len(source.browse_requests), 2)
        source.outcomes[('https://a.invalid', 1)] = ('success', None)
        source.pages['https://a.invalid'][1] = [torrent('later')]
        clock.advance()
        flow._dispatch_brush()
        self.assertEqual(flow.added, [('a', 'first'), ('a', 'later')])

    def test_rate_limit_backoff_is_at_least_ten_minutes(self):
        source = SourceBoundary({'https://a.invalid': {}})
        source.outcomes[('https://a.invalid', 0)] = ('rate_limited', None)
        flow = OfflineFlow([task('a', 1)], source)
        flow._dispatch_brush()
        clock.advance(599)
        flow._dispatch_brush()
        self.assertEqual(len(source.browse_requests), 1)
        clock.advance(1)
        flow._dispatch_brush()
        self.assertEqual(len(source.browse_requests), 2)

    def test_same_site_tasks_share_budget_and_rotate_fairly(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('one')], 1: [torrent('two')]}})
        flow = OfflineFlow([task('a', 1), task('b', 1)], source)
        flow.sites[1] = SimpleNamespace(id=1, name='a', domain='https://a.invalid')
        flow._dispatch_brush()
        self.assertEqual(len(source.browse_requests), 1)
        clock.advance()
        flow._dispatch_brush()
        self.assertEqual(source.browse_requests[-1], ('https://a.invalid', None))
        self.assertFalse(self.scan(flow, 'b').get('head', {}).get('pending', True))

    def test_independent_sites_drain_candidates_fairly(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('a1', peers=30), torrent('a2')]},
                                 'https://b.invalid': {0: [torrent('b1', site='b', peers=30), torrent('b2', site='b')]}})
        flow = OfflineFlow([task('a', 1), task('b', 2)], source)
        flow._dispatch_brush()
        self.assertEqual(flow.added, [('a', 'a1'), ('b', 'b1'), ('a', 'a2'), ('b', 'b2')])

    def test_slow_page_drain_yields_next_request_to_same_site_waiter(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('one', peers=30), torrent('two', peers=20), torrent('three')], 1: [torrent('tail')]}})
        flow = OfflineFlow([task('a', 1), task('b', 1)], source)
        flow.sites[1] = SimpleNamespace(id=1, name='a', domain='https://a.invalid')
        original = flow._OriechoBrushFlow__download
        def download(item):
            result = original(item)
            clock.advance(20)
            return result
        flow._OriechoBrushFlow__download = download
        flow._dispatch_brush()
        self.assertEqual(source.browse_requests, [('https://a.invalid', None)])
        self.assertEqual(flow.added, [('a', 'one'), ('a', 'two'), ('a', 'three')])
        flow._dispatch_brush()
        self.assertEqual(source.browse_requests[1], ('https://a.invalid', None))
        self.assertFalse(self.scan(flow, 'b')['head']['pending'])

    def test_same_site_waiters_do_not_write_zero_work_runs(self):
        source = SourceBoundary({'https://a.invalid': {page: [torrent(f'page{page}')] for page in range(5)}})
        flow = OfflineFlow([task('a', 1), task('b', 1)], source)
        flow.sites[1] = SimpleNamespace(id=1, name='a', domain='https://a.invalid')
        for _ in range(5):
            flow._dispatch_brush()
            clock.advance()
        self.assertEqual(len(flow.reports), len(source.browse_requests))

    def test_stop_inside_download_keeps_active_cursor_for_restart(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('one')]}})
        flow = OfflineFlow([task('a', 1)], source)
        def download(item):
            flow._stopping = True
            return None
        flow._OriechoBrushFlow__download = download
        flow._dispatch_brush()
        self.assertTrue(self.scan(flow)['active'])
        self.assertEqual(flow.reports[0][1]['result'], 'stopped')
        resumed = OfflineFlow([task('a', 1)], source, storage=flow.storage, queue=False)
        clock.advance()
        resumed._dispatch_brush()
        self.assertEqual(resumed.added, [('a', 'one')])

    def test_request_601_waits_until_oldest_request_expires(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('first')]}})
        flow = OfflineFlow([task('a', 1)], source)
        oldest = clock.now - 18000
        flow.storage['source_budget.1'] = {'requests': [oldest + 30 * i for i in range(600)]}
        flow._dispatch_brush()
        self.assertEqual(source.browse_requests, [])
        clock.now = oldest + 86400
        flow._dispatch_brush()
        self.assertEqual(len(source.browse_requests), 1)
        self.assertEqual(len(flow.storage['source_budget.1']['requests']), 600)

    def test_head_refresh_does_not_reset_tail(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('first')], 1: [torrent('tail1')], 2: [torrent('tail2')]}})
        flow = OfflineFlow([task('a', 1)], source)
        flow._dispatch_brush()
        clock.advance()
        flow._dispatch_brush()
        source.pages['https://a.invalid'][0] = [torrent('new')]
        flow._queue_brush('a')
        clock.advance()
        flow._dispatch_brush()
        clock.advance()
        flow._dispatch_brush()
        self.assertEqual(source.browse_requests, [('https://a.invalid', None), ('https://a.invalid', 1), ('https://a.invalid', None), ('https://a.invalid', 2)])
        self.assertEqual(flow.added, [('a', 'first'), ('a', 'tail1'), ('a', 'new'), ('a', 'tail2')])

    def test_repeated_page_uses_stable_ids_despite_changing_peer_counts(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('one', peers=20)], 1: [torrent('one', peers=99)]}})
        flow = OfflineFlow([task('a', 1)], source)
        flow._dispatch_brush()
        clock.advance()
        flow._dispatch_brush()
        clock.advance()
        flow._dispatch_brush()
        self.assertEqual(len(source.browse_requests), 2)
        self.assertNotIn('a', flow._pending_brush)

    def test_has_more_false_ends_scan_after_all_page_candidates(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('one'), torrent('two')]}})
        source.outcomes[('https://a.invalid', 0)] = ('success', False)
        flow = OfflineFlow([task('a', 1)], source)
        flow._dispatch_brush()
        clock.advance()
        flow._dispatch_brush()
        self.assertEqual(len(source.browse_requests), 1)
        self.assertEqual(len(flow.added), 2)
        self.assertNotIn('a', flow._pending_brush)

    def test_rate_limit_blocks_other_tasks_and_lanes_on_same_site(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('one')], 1: [torrent('two')]}})
        flow = OfflineFlow([task('a', 1), task('b', 1)], source)
        flow.sites[1] = SimpleNamespace(id=1, name='a', domain='https://a.invalid')
        source.outcomes[('https://a.invalid', 0)] = ('rate_limited', None)
        flow._dispatch_brush()
        clock.advance()
        flow._dispatch_brush()
        self.assertEqual(len(source.browse_requests), 1)
        self.assertFalse(flow.reports[0][1]['success'])

    def test_new_period_request_survives_old_run_capacity_pause(self):
        source = SourceBoundary({})
        flow = OfflineFlow([task('a', 1)], source)
        flow._site_completed_count = lambda site_id: 50
        original = flow._execute_brush
        def run_and_queue(task, report):
            result = original(task, report)
            flow._queue_brush(task.id)
            return result
        flow._execute_brush = run_and_queue
        flow._dispatch_brush()
        self.assertIn('a', flow._pending_brush)
        self.assertTrue(self.scan(flow).get('active'))

    def test_changed_source_does_not_restore_obsolete_cursor_or_spam_history(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('one')]}})
        flow = OfflineFlow([task('a', 1)], source)
        flow._dispatch_brush()
        flow._task_configs['a'].rss_support = True
        for _ in range(3):
            clock.advance()
            flow._dispatch_brush()
        self.assertNotIn('a', flow._pending_brush)
        self.assertEqual(len(flow.reports), 1)

    def test_stop_during_torrent_file_fetch_prevents_downloader_submission(self):
        for service_type in ('qbittorrent', 'transmission'):
            with self.subTest(service_type=service_type):
                source = SourceBoundary({})
                flow = OfflineFlow([task('a', 1, site_skip_tips=False, save_path='/download', qb_category='brush', brush_tag='brush')], source)
                submitted = []
                flow.downloader = SimpleNamespace(add_torrent=lambda **kwargs: submitted.append(kwargs) or SimpleNamespace(hashString='test'), get_torrent_id_by_tag=lambda **kwargs: 'test')
                namespace['DownloaderHelper'] = lambda: SimpleNamespace(is_downloader=lambda name, service: name == service_type)
                namespace['StringUtils'] = SimpleNamespace(generate_random_str=lambda length: 'test')
                def fetch(**kwargs):
                    flow._stopping = True
                    return SimpleNamespace(ok=True, content=b'torrent')
                namespace['RequestUtils'] = lambda **kwargs: SimpleNamespace(get_res=fetch)
                with flow._task_scope('a'):
                    OriechoBrushFlow._OriechoBrushFlow__download(flow, torrent('one', enclosure='https://a.invalid/download?id=1', site_proxy=False, site_cookie='secret', site_ua='test'))
                self.assertEqual(submitted, [])

    def test_head_transient_error_allows_tail_to_continue(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('one')], 1: [torrent('tail')]}})
        flow = OfflineFlow([task('a', 1)], source)
        flow._dispatch_brush()
        flow._queue_brush('a')
        source.outcomes[('https://a.invalid', 0)] = ('error', None)
        clock.advance()
        flow._dispatch_brush()
        clock.advance()
        flow._dispatch_brush()
        self.assertEqual(flow.added, [('a', 'one'), ('a', 'tail')])
        self.assertTrue(self.scan(flow)['head']['pending'])

    def test_capacity_pause_keeps_cursor_without_30_second_history_spam(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('one')], 1: [torrent('tail')]}})
        flow = OfflineFlow([task('a', 1)], source)
        flow._dispatch_brush()
        flow._site_completed_count = lambda site_id: 50
        clock.advance()
        flow._dispatch_brush()
        original_count = len(flow.reports)
        for _ in range(5):
            clock.advance()
            flow._dispatch_brush()
        self.assertEqual(len(flow.reports), original_count)
        self.assertEqual(self.scan(flow)['tail']['page'], 1)
        self.assertTrue(self.scan(flow)['tail']['pending'])
        self.assertFalse(self.scan(flow)['active'])

    def test_budget_is_persisted_before_even_failed_network_call(self):
        source = SourceBoundary({'https://a.invalid': {}})
        flow = OfflineFlow([task('a', 1)], source)
        seen = []
        source.after_browse = lambda: seen.append(flow.storage['source_budget.1']['requests'][:])
        source.outcomes[('https://a.invalid', 0)] = ('exception', None)
        flow._dispatch_brush()
        self.assertEqual(seen, [[100000.0]])
        self.assertTrue(self.scan(flow)['head']['pending'])

    def test_page_candidates_are_all_drained_before_next_page_even_when_time_advances(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('one', peers=30), torrent('two')], 1: [torrent('three')]}})
        flow = OfflineFlow([task('a', 1)], source)
        original = flow._OriechoBrushFlow__download
        seen = []
        def download(item):
            result = original(item)
            seen.append((item.title, list(source.browse_requests)))
            clock.advance()
            return result
        flow._OriechoBrushFlow__download = download
        flow._dispatch_brush()
        self.assertEqual([name for name, _ in seen], ['one', 'two', 'three'])
        self.assertEqual(seen[1][1], [('https://a.invalid', None)])

    def test_refresh_during_head_processing_merges_without_clearing_progress(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('one', peers=30), torrent('two')]}})
        flow = OfflineFlow([task('a', 1)], source)
        original = flow._OriechoBrushFlow__download
        def download(item):
            result = original(item)
            if item.title == 'one':
                flow._queue_brush('a')
                flow._stopping = True
            return result
        flow._OriechoBrushFlow__download = download
        flow._dispatch_brush()
        processed = self.scan(flow)['head']['processed_ids']
        self.assertEqual(len(processed), 1)
        self.assertTrue(all(re.fullmatch('[a-f0-9]{64}', identity) for identity in processed))
        resumed = OfflineFlow([task('a', 1)], source, storage=flow.storage, queue=False)
        resumed.records = copy.deepcopy(flow.records)
        clock.advance()
        resumed._dispatch_brush()
        self.assertEqual(resumed.added, [('a', 'two')])

    def test_disabled_tasks_are_not_restored(self):
        source = SourceBoundary({'https://a.invalid': {0: [torrent('one')]}})
        flow = OfflineFlow([task('a', 1)], source)
        flow._dispatch_brush()
        resumed = OfflineFlow([task('a', 1, enabled=False)], source, storage=flow.storage, queue=False)
        clock.advance()
        resumed._dispatch_brush()
        self.assertEqual(len(source.browse_requests), 1)
        self.assertEqual(resumed.added, [])

    def test_selection_capacity_rss_behavior_is_retained(self):
        source = SourceBoundary({}, rss={'https://a.invalid': [torrent('paid', peers=99, downloadvolumefactor=1), torrent('hr', peers=80, hit_and_run=True), torrent('high', peers=50), torrent('low', peers=20), torrent('no-demand', peers=0)]})
        flow = OfflineFlow([task('a', 1, rss_support=True, disksize=1.5)], source)
        flow._dispatch_brush()
        self.assertEqual(flow.added, [('a', 'high')])
        self.assertEqual(source.rss_requests, ['https://a.invalid'])
        self.assertEqual(source.browse_requests, [])
        self.assertNotIn('a', flow._pending_brush)


if __name__ == '__main__':
    unittest.main(verbosity=2)
