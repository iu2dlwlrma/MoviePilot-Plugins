"""Offline behavior tests; only MoviePilot I/O boundaries are replaced."""
import ast
from datetime import datetime, timezone
import hashlib
import importlib.util
import math
from pathlib import Path
from threading import Lock
from types import SimpleNamespace
import unittest
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / 'plugins.v3' / 'sitestatistic'


def load_monthly():
    source = PLUGIN / 'monthly.py'
    if not source.exists():
        return None
    spec = importlib.util.spec_from_file_location('site_monthly_logic', source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def snapshot(day, upload=0, download=0, *, domain='one.test', userid='7', name='同名站', err_msg=None):
    data = SimpleNamespace(updated_day=day, upload=upload, download=download,
                           domain=domain, userid=userid, name=name, err_msg=err_msg,
                           username='test', user_level='user', ratio=1, bonus=0, seeding=2, seeding_size=100)
    data.to_dict = lambda: {'upload': data.upload, 'download': data.download,
                            'seeding': data.seeding, 'seeding_size': data.seeding_size}
    return data


class SiteStore:
    def __init__(self, data=None, active=('one.test',)):
        self.data = data or {}
        self.active = active
        self.calls = []

    def list_active(self):
        return [SimpleNamespace(domain=domain, name='同名站') for domain in self.active]

    def get_userdata_by_date(self, day):
        self.calls.append(day)
        result = self.data.get(day, [])
        if isinstance(result, Exception):
            raise result
        return result


class MonthlyTests(unittest.TestCase):
    def setUp(self):
        self.logic = load_monthly()
        self.assertIsNotNone(self.logic, '独立插件尚未实现自然月统计逻辑')

    def collect(self, store, now=None, tz='Asia/Shanghai'):
        return self.logic.collect_month(store, tz, now or datetime(2026, 10, 5, tzinfo=timezone.utc))

    def test_zero_month_end_baseline_is_a_real_baseline(self):
        store = SiteStore({'2026-09-30': [snapshot('2026-09-30')],
                           '2026-10-05': [snapshot('2026-10-05', 100, 25)]})
        result = self.collect(store)
        self.assertEqual((result['upload'], result['download'], result['covered']), (100, 25, 1))
        self.assertFalse(result['partial'])
        self.assertEqual((result['rows'][0]['start'], result['rows'][0]['end']), ('2026-09-30', '2026-10-05'))
        self.assertEqual(store.calls, ['2026-09-30', '2026-10-01', '2026-10-02', '2026-10-03', '2026-10-04', '2026-10-05'])

    def test_local_month_boundary_and_real_calendar_dates(self):
        cases = [
            (datetime(2026, 12, 31, 16, 30, tzinfo=timezone.utc), '2027-01', '2026-12-31', '2027-01-01', 2),
            (datetime(2024, 2, 29, 12, tzinfo=timezone.utc), '2024-02', '2024-01-31', '2024-02-29', 30),
            (datetime(2026, 10, 31, 12, tzinfo=timezone.utc), '2026-10', '2026-09-30', '2026-10-31', 32),
        ]
        for now, month, first, last, count in cases:
            with self.subTest(month=month):
                store = SiteStore()
                result = self.collect(store, now)
                self.assertEqual(result['month'], month)
                self.assertEqual((store.calls[0], store.calls[-1], len(store.calls)), (first, last, count))

    def test_missing_baseline_uses_only_observed_month_range(self):
        result = self.collect(SiteStore({'2026-10-02': [snapshot('2026-10-02', 100, 20)],
                                        '2026-10-05': [snapshot('2026-10-05', 160, 30)]}))
        row = result['rows'][0]
        self.assertEqual((row['upload'], row['download'], row['start'], row['end']), (60, 10, '2026-10-02', '2026-10-05'))
        self.assertIn('缺月初基线', row['status'])
        self.assertTrue(result['partial'])

    def test_single_reading_does_not_become_fabricated_zero(self):
        result = self.collect(SiteStore({'2026-10-05': [snapshot('2026-10-05', 160, 30)]}))
        self.assertIsNone(result['upload'])
        self.assertIsNone(result['rows'][0]['upload'])
        self.assertIn('不足以计算', result['rows'][0]['status'])
        self.assertEqual(result['covered'], 0)

    def test_same_name_different_domains_are_never_overwritten(self):
        store = SiteStore({
            '2026-09-30': [snapshot('2026-09-30', 10), snapshot('2026-09-30', 500, domain='two.test')],
            '2026-10-05': [snapshot('2026-10-05', 30), snapshot('2026-10-05', 550, domain='two.test')],
        }, active=('one.test', 'two.test'))
        result = self.collect(store)
        self.assertEqual((result['upload'], result['covered'], len(result['rows'])), (70, 2, 2))

    def test_account_change_and_counter_reset_cannot_join_totals(self):
        cases = [
            ([snapshot('2026-09-30', 100)], [snapshot('2026-10-02', 130, userid='8')], [snapshot('2026-10-05', 180, userid='8')], '账号变化'),
            ([snapshot('2026-09-30', 100)], [snapshot('2026-10-02', 20)], [snapshot('2026-10-05', 180)], '计数下降'),
        ]
        for baseline, middle, latest, reason in cases:
            result = self.collect(SiteStore({'2026-09-30': baseline, '2026-10-02': middle, '2026-10-05': latest}))
            self.assertIsNone(result['upload'])
            self.assertIn(reason, result['rows'][0]['status'])
            self.assertTrue(result['partial'])

    def test_invalid_future_and_wrong_day_records_are_discarded(self):
        store = SiteStore({
            '2026-09-30': [snapshot('2026-09-30', 100, 10)],
            '2026-10-01': [snapshot('2026-10-01', 999, 20, err_msg='error')],
            '2026-10-02': [snapshot('2026-10-02', None, 20), snapshot('2026-10-02', float('nan'), 20)],
            '2026-10-03': [snapshot('2026-10-03', float('inf'), 20), snapshot('2026-10-03', -1, 20)],
            '2026-10-04': [snapshot('2026-11-01', 900, 20), snapshot('2026-09-01', 800, 20)],
            '2026-10-05': [snapshot('2026-10-05', 160, 30)],
        })
        result = self.collect(store)
        self.assertEqual((result['upload'], result['download']), (60, 20))

    def test_query_failure_preserves_partial_data_and_names_coverage(self):
        result = self.collect(SiteStore({'2026-09-30': RuntimeError('unavailable'),
                                        '2026-10-02': [snapshot('2026-10-02', 10)],
                                        '2026-10-05': [snapshot('2026-10-05', 20)]}))
        self.assertEqual(result['upload'], 10)
        self.assertTrue(result['partial'])
        self.assertEqual(result['query_errors'], ['2026-09-30'])

    def test_pb_rounding_and_scientific_counters_keep_precision_warning(self):
        base = 1.234 * 1024 ** 5
        result = self.collect(SiteStore({'2026-09-30': [snapshot('2026-09-30', str(base))],
                                        '2026-10-05': [snapshot('2026-10-05', str(base))]}))
        self.assertEqual(result['upload'], 0)
        self.assertIn('upload', result['rows'][0]['approximate'])
        self.assertIn('upload', result['approximate'])

    def test_same_day_duplicates_use_latest_update_instead_of_query_order(self):
        early = snapshot('2026-10-05', 120)
        early.id, early.updated_time = 1, '2026-10-05 02:00:00'
        latest = snapshot('2026-10-05', 150)
        latest.id, latest.updated_time = 2, '2026-10-05 08:00:00'
        result = self.collect(SiteStore({'2026-09-30': [snapshot('2026-09-30', 100)],
                                        '2026-10-05': [latest, early]}))
        self.assertEqual(result['upload'], 50)

    def test_integer_counters_above_float_precision_keep_byte_difference(self):
        result = self.collect(SiteStore({'2026-09-30': [snapshot('2026-09-30', 9007199254740992)],
                                        '2026-10-05': [snapshot('2026-10-05', 9007199254740993)]}))
        self.assertEqual(result['upload'], 1)

    def test_real_host_clock_only_update_time_wins_before_record_id(self):
        early = snapshot('2026-10-05', 120)
        early.id, early.updated_time = 9, '02:00:00'
        latest = snapshot('2026-10-05', 150)
        latest.id, latest.updated_time = 2, '08:00:00'
        result = self.collect(SiteStore({'2026-09-30': [snapshot('2026-09-30', 100)],
                                        '2026-10-05': [early, latest]}))
        self.assertEqual(result['upload'], 50)


class PluginTests(unittest.TestCase):
    def setUp(self):
        source = PLUGIN / '__init__.py'
        self.assertTrue(source.exists(), '独立站点统计插件尚未实现')
        original = ast.parse(source.read_text(encoding='utf-8'))
        definition = next(node for node in original.body if isinstance(node, ast.ClassDef))
        extracted = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0), definition], type_ignores=[])
        self.store = SiteStore({'2026-09-30': [snapshot('2026-09-30', 10, 5)],
                                '2026-10-05': [snapshot('2026-10-05', 40, 15)]})
        self.active_schedulers = []

        class PluginBoundary:
            def __init__(inner):
                inner.messages = []
                inner.saved = {}

            def get_data(inner, key):
                return inner.saved.get(key)

            def save_data(inner, key, value):
                inner.saved[key] = value

            def post_message(inner, **kwargs):
                inner.messages.append(kwargs)

            def update_config(inner, config):
                inner.config = dict(config)

        class SchedulerBoundary:
            def __init__(inner, **kwargs):
                inner.running = False

            def add_job(inner, *args, **kwargs):
                pass

            def print_jobs(inner):
                pass

            def start(inner):
                inner.running = True
                self.active_schedulers.append(inner)

            def remove_all_jobs(inner):
                pass

            def shutdown(inner, wait=True):
                inner.running = False
                self.active_schedulers.remove(inner)

        logic = load_monthly()
        namespace = {
            '_PluginBase': PluginBoundary, 'eventmanager': SimpleNamespace(register=lambda *args: lambda fn: fn),
            'EventType': SimpleNamespace(SiteRefreshed='SiteRefreshed'),
            'NotificationType': SimpleNamespace(SiteMessage='site'),
            'settings': SimpleNamespace(TZ='Asia/Shanghai'), 'SiteOper': lambda: self.store,
            'collect_month': lambda oper, tz: logic.collect_month(oper, tz, datetime(2026, 10, 5, tzinfo=timezone.utc)),
            'identity': logic.identity, 'valid_traffic': logic.valid_traffic,
            'latest_by_domain': logic.latest_by_domain, 'traffic_bytes': logic._bytes,
            'approximate_fields': logic.approximate_fields, 'hashlib': hashlib, 'math': math,
            'datetime': datetime, 'timedelta': __import__('datetime').timedelta,
            'pytz': SimpleNamespace(timezone=ZoneInfo), 'lock': Lock(),
            'BackgroundScheduler': SchedulerBoundary,
            'SiteChain': lambda: SimpleNamespace(refresh_userdatas=lambda: None),
            'logger': SimpleNamespace(info=lambda *args: None, debug=lambda *args: None, error=lambda *args: None),
            'StringUtils': SimpleNamespace(str_filesize=lambda value: f'{value}B'),
        }
        exec(compile(ast.fix_missing_locations(extracted), str(source), 'exec'), namespace)
        self.plugin = namespace['OriechoSiteStatistic']()

    @staticmethod
    def nodes(value, component):
        if isinstance(value, dict):
            if value.get('component') == component:
                yield value
            yield from PluginTests.nodes(value.get('content', []), component)
        elif isinstance(value, (list, tuple)):
            for child in value:
                yield from PluginTests.nodes(child, component)

    def table(self, page):
        tables = list(self.nodes(page, 'VTable'))
        self.assertEqual(len(tables), 1, '所有站点必须合并到唯一的底部表格')
        table = tables[0]
        headers = [node['text'] for node in self.nodes(table, 'th')]
        rows = []
        for body in self.nodes(table, 'tbody'):
            for row in body['content']:
                cells = [node.get('text') for node in row['content']]
                if len(cells) == len(headers):
                    rows.append(dict(zip(headers, cells)))
        return rows

    def summary_value(self, page, label):
        for node in self.nodes(page, 'VCol'):
            children = node.get('content', [])
            if len(children) == 2 and children[0].get('text') == label:
                return children[1]['text']
        self.fail(f'未找到汇总项：{label}')

    def test_current_and_baseline_duplicates_choose_latest_time_for_every_view(self):
        early = snapshot('2026-10-05', 200, 20)
        early.updated_time, early.id = '10:00:00', 90
        latest = snapshot('2026-10-05', 300, 30)
        latest.updated_time, latest.id = '22:00:00', 10
        previous_early = snapshot('2026-10-04', 150, 15)
        previous_early.updated_time, previous_early.id = '10:00:00', 90
        previous_latest = snapshot('2026-10-04', 100, 10)
        previous_latest.updated_time, previous_latest.id = '22:00:00', 10
        self.store.data = {'2026-09-30': [snapshot('2026-09-30', 100, 10)],
                           '2026-10-04': [previous_latest, previous_early],
                           '2026-10-05': [latest, early]}
        self.store.get_userdata_latest = lambda: [latest, early]
        page = self.plugin.get_page()
        self.assertEqual(self.summary_value(page, '总上传量'), '300B')
        self.assertEqual(self.summary_value(page, '今日上传'), '200B')
        self.assertEqual(self.summary_value(page, '本月上传'), '200B')
        rows = self.table(page)
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]['上传量'], rows[0]['今日上传'], rows[0]['本月上传']),
                         ('300B', '200B', '200B'))
        self.plugin.init_plugin({'enabled': True, 'notify_type': 'inc'})
        self.plugin.send_msg(SimpleNamespace(event_data={'site_id': '*'}))
        self.assertIn('总上传：200B', self.plugin.messages[0]['text'])

    def test_same_update_time_uses_record_id_for_current_and_baseline(self):
        old = snapshot('2026-10-05', 200, 20)
        old.updated_time, old.id = '22:00:00', 1
        latest = snapshot('2026-10-05', 300, 30)
        latest.updated_time, latest.id = '22:00:00', 2
        previous_old = snapshot('2026-10-04', 150, 15)
        previous_old.updated_time, previous_old.id = '22:00:00', 1
        previous_latest = snapshot('2026-10-04', 100, 10)
        previous_latest.updated_time, previous_latest.id = '22:00:00', 2
        self.store.data['2026-10-04'] = [previous_latest, previous_old]
        self.store.get_userdata_latest = lambda: [latest, old]
        page = self.plugin.get_page()
        self.assertEqual(self.summary_value(page, '总上传量'), '300B')
        self.assertEqual(self.table(page)[0]['今日上传'], '200B')

    def test_latest_account_on_the_same_day_replaces_older_account_before_matching(self):
        old = snapshot('2026-10-05', 200, 20, userid='old')
        old.updated_time, old.id = '10:00:00', 90
        latest = snapshot('2026-10-05', 300, 30, userid='new')
        latest.updated_time, latest.id = '22:00:00', 10
        self.store.data = {'2026-09-30': [snapshot('2026-09-30', 100, 10, userid='old')],
                           '2026-10-04': [snapshot('2026-10-04', 150, 15, userid='old')],
                           '2026-10-05': [latest, old]}
        self.store.get_userdata_latest = lambda: [latest, old]
        _, current, previous = self.plugin._OriechoSiteStatistic__get_data()
        self.assertEqual([row.userid for row in current], ['new'])
        self.assertEqual(previous, [])
        page = self.plugin.get_page()
        self.assertEqual(self.summary_value(page, '总上传量'), '300B')
        row = self.table(page)[0]
        self.assertEqual((row['今日上传'], row['本月上传']), ('—', '—'))
        self.assertIn('账号变化', row['月覆盖状态'])
        self.plugin.init_plugin({'enabled': True, 'notify_type': 'inc'})
        self.plugin.send_msg(SimpleNamespace(event_data={'site_id': '*'}))
        self.assertEqual(self.plugin.messages, [])

    def test_changed_baseline_account_does_not_fall_back_to_an_older_matching_account(self):
        old = snapshot('2026-10-04', 150, 15, userid='7')
        old.updated_time, old.id = '10:00:00', 90
        replacement = snapshot('2026-10-04', 180, 18, userid='replacement')
        replacement.updated_time, replacement.id = '22:00:00', 10
        self.store.data = {'2026-10-03': [snapshot('2026-10-03', 100, 10)],
                           '2026-10-04': [replacement, old],
                           '2026-10-05': [snapshot('2026-10-05', 300, 30)]}
        self.store.get_userdata_latest = lambda: self.store.data['2026-10-05']
        _, _, previous = self.plugin._OriechoSiteStatistic__get_data()
        self.assertEqual(previous, [], '前日最新账号已变化，不能挑旧账号或再向前回退')
        page = self.plugin.get_page()
        self.assertEqual((self.table(page)[0]['今日上传'], self.table(page)[0]['本月上传']), ('—', '—'))

    def test_baseline_query_failure_keeps_current_totals_and_names_the_failed_read(self):
        self.store.data['2026-10-04'] = RuntimeError('unavailable')
        self.store.get_userdata_latest = lambda: self.store.data['2026-10-05']
        page = self.plugin.get_page()
        self.assertEqual(self.summary_value(page, '总上传量'), '40B')
        row = self.table(page)[0]
        self.assertEqual((row['上传量'], row['今日上传'], row['本月上传']), ('40B', '—', '30B'))
        self.assertIn('日基线读取失败', str(page))
        self.assertNotIn('最新快照读取失败', str(page))
        self.plugin.init_plugin({'enabled': True, 'dashboard_type': 'month'})
        dashboard = self.plugin.get_dashboard('')
        self.assertEqual(self.summary_value(dashboard, '总上传量'), '40B')
        self.assertIn('日基线读取失败', str(dashboard))

    def test_scientific_integer_strings_keep_one_byte_difference_in_new_daily_values(self):
        self.store.data['2026-10-04'] = [snapshot('2026-10-04', '9007199254750992e0', 8)]
        self.store.get_userdata_latest = lambda: [snapshot('2026-10-05', '9007199254750993e0', 15)]
        page = self.plugin.get_page()
        self.assertEqual(self.table(page)[0]['今日上传'], '1B')
        self.assertEqual(self.summary_value(page, '今日上传'), '1B')

    def test_page_keeps_three_summaries_before_one_combined_site_list(self):
        self.store.data['2026-10-04'] = [snapshot('2026-10-04', 25, 8)]
        self.store.get_userdata_latest = lambda: self.store.data['2026-10-05']
        page = self.plugin.get_page()
        titles = [node['text'] for node in self.nodes(page, 'VCardTitle')]
        self.assertEqual(titles[:3], ['总计', '本月', '今日'])
        rows = self.table(page)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['今日上传'], '15B')
        self.assertEqual(rows[0]['今日下载'], '7B')
        self.assertEqual(rows[0]['本月上传'], '30B')
        self.assertEqual(rows[0]['本月下载'], '10B')
        self.assertEqual(rows[0]['本月统计起止'], '2026-09-30 → 2026-10-05')
        self.assertEqual(rows[0]['月覆盖状态'], '已覆盖月初基线')
        for name in ('站点', '数据日期', '用户名', '用户等级', '上传量', '下载量',
                     '分享率', '魔力值', '做种数', '做种体积'):
            self.assertIn(name, rows[0])
        regions = [node for node in self.nodes(page, 'div')
                   if node.get('props', {}).get('role') == 'region']
        self.assertEqual(len(regions), 1)
        self.assertEqual(regions[0]['props']['tabindex'], 0)
        self.assertIn('overflow-x: auto', regions[0]['props']['style'])
        self.assertIn('min-width: 0', page[0]['content'][-1]['props']['style'])

    def test_all_and_month_dashboard_share_summary_order_without_a_site_list(self):
        self.store.get_userdata_latest = lambda: self.store.data['2026-10-05']
        for mode in ('all', 'month'):
            with self.subTest(mode=mode):
                self.plugin.init_plugin({'enabled': True, 'dashboard_type': mode})
                dashboard = self.plugin.get_dashboard('')
                self.assertEqual([node['text'] for node in self.nodes(dashboard, 'VCardTitle')],
                                 ['总计', '本月', '今日'])
                self.assertEqual(list(self.nodes(dashboard, 'VTable')), [])
                self.assertIn('暂无可比较数据', str(dashboard))

    def test_missing_current_snapshots_keep_the_summary_and_site_table_structure(self):
        self.store.get_userdata_latest = lambda: []
        page = self.plugin.get_page()
        self.assertEqual([node['text'] for node in self.nodes(page, 'VCardTitle')][:3],
                         ['总计', '本月', '今日'])
        rows = self.table(page)
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]['上传量'], rows[0]['今日上传']), ('—', '—'))
        self.assertEqual(rows[0]['本月上传'], '30B')
        self.assertIn('暂无最新站点数据', str(page))

    def test_combined_list_never_attaches_another_account_month_or_daily_traffic(self):
        self.store.data['2026-10-04'] = [snapshot('2026-10-04', 25, 8, userid='old')]
        self.store.get_userdata_latest = lambda: [snapshot('2026-10-05', 60, 20, userid='new')]
        row = self.table(self.plugin.get_page())[0]
        for name in ('今日上传', '今日下载', '本月上传', '本月下载'):
            self.assertEqual(row[name], '—')
        self.assertIn('账号变化', row['月覆盖状态'])

    def test_empty_month_partial_and_query_failure_keep_one_combined_list(self):
        self.store.get_userdata_latest = lambda: []
        cases = (
            ({}, ('one.test',), '暂无本月有效快照'),
            ({'2026-10-05': [snapshot('2026-10-05', 40, 15)]}, ('one.test',), '不足以计算'),
            ({'2026-09-30': RuntimeError('unavailable')}, ('one.test',), '读取失败'),
            ({}, (), '暂无启用站点'),
        )
        for data, active, message in cases:
            with self.subTest(message=message):
                self.store.data, self.store.active = data, active
                page = self.plugin.get_page()
                self.assertEqual([node['text'] for node in self.nodes(page, 'VCardTitle')][:3],
                                 ['总计', '本月', '今日'])
                self.table(page)
                self.assertIn(message, str(page))

    def test_daily_table_accepts_a_real_zero_baseline_and_marks_counter_resets_unknown(self):
        self.store.get_userdata_latest = lambda: self.store.data['2026-10-05']
        self.store.data['2026-10-04'] = [snapshot('2026-10-04', 0, 0)]
        row = self.table(self.plugin.get_page())[0]
        self.assertEqual((row['今日上传'], row['今日下载']), ('40B', '15B'))
        self.store.data['2026-10-04'] = [snapshot('2026-10-04', 100, 20)]
        row = self.table(self.plugin.get_page())[0]
        self.assertEqual((row['今日上传'], row['今日下载']), ('—', '—'))

    def test_combined_list_keeps_pb_unknown_increment_warning(self):
        base = 1.234 * 1024 ** 5
        self.store.data = {'2026-09-30': [snapshot('2026-09-30', base, 5)],
                           '2026-10-04': [snapshot('2026-10-04', base, 8)],
                           '2026-10-05': [snapshot('2026-10-05', base, 15)]}
        self.store.get_userdata_latest = lambda: self.store.data['2026-10-05']
        page = self.plugin.get_page()
        row = self.table(page)[0]
        self.assertIn('实际增量未知', row['今日上传'])
        self.assertIn('实际增量未知', row['本月上传'])
        self.assertIn('PB 舍入', str(page))
        precision_notes = [node for node in self.nodes(page, 'VAlert') if 'PB 舍入' in str(node)]
        self.assertEqual(len(precision_notes), 1, '三组汇总和原图共用一份精度提示')

    def test_daily_table_does_not_mislabel_a_week_old_baseline_as_today(self):
        self.store.get_userdata_latest = lambda: self.store.data['2026-10-05']
        # __get_data may reuse a real older reading for the legacy daily chart.
        # The new 今日 fields must keep the missing adjacent-day delta unknown.
        row = self.table(self.plugin.get_page())[0]
        self.assertEqual((row['今日上传'], row['今日下载']), ('—', '—'))
        self.assertEqual(row['本月上传'], '30B')

    def test_daily_table_keeps_integer_byte_differences_above_float_precision(self):
        self.store.data['2026-10-04'] = [snapshot('2026-10-04', '9007199254750992', 8)]
        self.store.get_userdata_latest = lambda: [snapshot('2026-10-05', '9007199254750993', 15)]
        row = self.table(self.plugin.get_page())[0]
        self.assertEqual(row['今日上传'], '1B')

    def test_latest_query_failure_preserves_monthly_values_and_the_combined_list(self):
        def failed_read():
            raise RuntimeError('unavailable')

        self.store.get_userdata_latest = failed_read
        page = self.plugin.get_page()
        self.assertEqual([node['text'] for node in self.nodes(page, 'VCardTitle')][:3],
                         ['总计', '本月', '今日'])
        self.assertEqual(self.table(page)[0]['本月上传'], '30B')
        self.assertIn('读取失败', str(page))

    def test_summary_and_table_reuse_one_month_scan_and_cached_daily_lookup(self):
        self.store.data['2026-10-04'] = [snapshot('2026-10-04', 25, 8)]
        self.store.get_userdata_latest = lambda: self.store.data['2026-10-05']
        page = self.plugin.get_page()
        rows = self.table(page)
        self.assertEqual(rows[0]['今日上传'], '15B')
        # At the I/O boundary, one monthly scan and one cached previous-day
        # lookup suffice; building the three groups and table must not rescan.
        self.assertEqual(self.store.calls.count('2026-09-30'), 1)
        self.assertEqual(self.store.calls.count('2026-10-04'), 2)
        self.assertEqual(len(self.store.calls), 7)

    def test_month_notification_deduplicates_real_text_and_ignores_disabled(self):
        plugin = self.plugin
        event = SimpleNamespace(event_data={'site_id': '*'})
        plugin.init_plugin({'enabled': False, 'notify_type': 'month'})
        plugin.send_msg(event)
        self.assertEqual(plugin.messages, [])
        plugin.init_plugin({'enabled': True, 'notify_type': 'month'})
        plugin.send_msg(event)
        plugin.send_msg(event)
        self.assertEqual(len(plugin.messages), 1)
        text = plugin.messages[0]['text']
        for expected in ('2026-10', '2026-09-30', '2026-10-05', '30B', '10B', '1/1'):
            self.assertIn(expected, text)
        self.store.data['2026-10-05'] = [snapshot('2026-10-05', 50, 15)]
        plugin.send_msg(event)
        self.assertEqual(len(plugin.messages), 2)
        plugin.init_plugin(None)
        plugin.send_msg(event)
        self.assertFalse(plugin.get_state())
        self.assertEqual(len(plugin.messages), 2)

    def test_reloading_and_stopping_reclaims_onlyonce_schedulers(self):
        plugin = self.plugin
        for _ in range(3):
            plugin.init_plugin({'enabled': True, 'onlyonce': True})
            self.assertEqual(len(self.active_schedulers), 1)
        plugin.stop_service()
        self.assertEqual(self.active_schedulers, [])
        self.assertIsNone(plugin._scheduler)

    def test_disabling_during_month_read_does_not_post_a_late_notification(self):
        self.plugin.init_plugin({'enabled': True, 'notify_type': 'month'})
        list_active = self.store.list_active

        def disable_during_read():
            self.plugin.init_plugin({'enabled': False, 'notify_type': 'month'})
            return list_active()

        self.store.list_active = disable_during_read
        self.plugin.send_msg(SimpleNamespace(event_data={'site_id': '*'}))
        self.assertEqual(self.plugin.messages, [])

    def test_month_dashboard_all_dashboard_and_detail_expose_coverage(self):
        plugin = self.plugin
        self.store.get_userdata_latest = lambda: []
        plugin.init_plugin({'enabled': True, 'dashboard_type': 'month'})
        rendered = str(plugin.get_dashboard(''))
        self.assertIn('本月上传', rendered)
        self.assertIn('30B', rendered)
        plugin.init_plugin({'enabled': True, 'dashboard_type': 'all'})
        self.assertIn('本月上传', str(plugin.get_dashboard('')))
        page = str(plugin.get_page())
        for expected in ('本月上传', '2026-09-30', '2026-10-05', '已覆盖月初基线', 'one.test', 'overflow-x'):
            self.assertIn(expected, page)

    def test_large_scientific_values_format_in_pb(self):
        rendered = self.plugin._OriechoSiteStatistic__format_filesize('2.251799813685248e15')
        self.assertEqual(rendered, '2.00PB')

    def test_daily_modes_match_domain_and_account_and_keep_original_charts(self):
        self.store.active = ('one.test', 'two.test')
        self.store.data = {'2026-10-04': [snapshot('2026-10-04', 10, 5),
                                         snapshot('2026-10-04', 500, 100, domain='two.test')]}
        self.store.get_userdata_latest = lambda: [snapshot('2026-10-05', 40, 15),
                                                  snapshot('2026-10-05', 550, 120, domain='two.test')]
        self.plugin.init_plugin({'enabled': True, 'notify_type': 'inc'})
        self.plugin.send_msg(SimpleNamespace(event_data={'site_id': '*'}))
        text = self.plugin.messages[0]['text']
        self.assertIn('总上传：80B', text)
        self.assertIn('总下载：30B', text)
        self.assertIn('one.test', text)
        self.assertIn('two.test', text)
        self.assertIn('VApexChart', str(self.plugin.get_dashboard('')))
        self.plugin.init_plugin({'enabled': True, 'notify_type': 'all'})
        self.plugin.send_msg(SimpleNamespace(event_data={'site_id': '*'}))
        self.assertIn('总上传：590B', self.plugin.messages[-1]['text'])
        self.assertIn('总做种体积', str(self.plugin.get_page()))

    def test_daily_account_switch_does_not_compare_another_account(self):
        self.store.data = {'2026-10-04': [snapshot('2026-10-04', 10, 5, userid='old')]}
        self.store.get_userdata_latest = lambda: [snapshot('2026-10-05', 40, 15, userid='new')]
        self.plugin.init_plugin({'enabled': True, 'notify_type': 'inc'})
        self.plugin.send_msg(SimpleNamespace(event_data={'site_id': '*'}))
        self.assertEqual(self.plugin.messages, [])

    def test_daily_notifications_keep_every_site_with_equal_pb_uploads(self):
        self.store.active = ('one.test', 'two.test')
        self.store.data = {'2026-10-04': [snapshot('2026-10-04', 100, 50),
                                         snapshot('2026-10-04', 100, 50, domain='two.test')]}
        self.store.get_userdata_latest = lambda: [snapshot('2026-10-05', 2 ** 50 + 100, 100),
                                                  snapshot('2026-10-05', 2 ** 50 + 100, 100, domain='two.test')]
        for notify_type in ('inc', 'all'):
            with self.subTest(notify_type=notify_type):
                self.plugin.init_plugin({'enabled': True, 'notify_type': notify_type})
                self.plugin.send_msg(SimpleNamespace(event_data={'site_id': '*'}))
                text = self.plugin.messages[-1]['text']
                self.assertIn('one.test', text)
                self.assertIn('two.test', text)

    def test_daily_counter_reset_is_clamped_before_chart_aggregation(self):
        # Run the unchanged nested arithmetic itself; chart filtering would
        # conceal a negative result and could not catch the broken clamp.
        source = ast.parse((PLUGIN / '__init__.py').read_text(encoding='utf-8'))
        definition = next(node for node in source.body if isinstance(node, ast.ClassDef))
        method = next(node for node in definition.body
                      if isinstance(node, ast.FunctionDef) and node.name == '__get_total_elements')
        helpers = [node for node in method.body if isinstance(node, ast.FunctionDef)
                   and node.name in ('__is_digit', '__to_numeric', '__sub_data')]
        extracted = ast.Module(body=helpers, type_ignores=[])
        namespace = {'logger': SimpleNamespace(error=lambda *args: None)}
        exec(compile(ast.fix_missing_locations(extracted), str(PLUGIN / '__init__.py'), 'exec'), namespace)
        increments = namespace['__sub_data']({'upload': 10, 'download': 2}, {'upload': 100, 'download': 5})
        self.assertEqual(increments, {'upload': 0, 'download': 0})
        self.store.data = {'2026-10-04': [snapshot('2026-10-04', 100, 5)]}
        self.store.get_userdata_latest = lambda: [snapshot('2026-10-05', 10, 2)]
        self.plugin.init_plugin({'enabled': True, 'dashboard_type': 'today'})
        chart = str(self.plugin.get_dashboard(''))
        self.assertIn("'series': []", chart)
        self.assertNotIn('-90', chart)


if __name__ == '__main__':
    unittest.main()
