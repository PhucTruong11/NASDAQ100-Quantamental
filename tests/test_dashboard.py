"""Serving contract: denominators, missingness, filters and actual Streamlit interactions."""
import copy
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from streamlit.testing.v1 import AppTest

from app.data import (SnapshotError, coverage_by_period, coverage_by_sector,
                      csv_download, decode_snapshot, exclusion_summary, filter_sectors, screener)
from research.dashboard_snapshot import publish_snapshot

ROOT = Path(__file__).resolve().parents[1]


def fixture():
    records = []
    for date in ['2026-08-31', '2026-09-30']:
        for ticker, sector, fcf, sbc in [('ZERO', 'TECHNOLOGY', True, True),
                                          ('FCF', 'TECHNOLOGY', False, True),
                                          ('SBC', 'ENERGY', True, False),
                                          ('BOTH', None, False, False)]:
            eligible = fcf and sbc
            records.append(dict(
                rebalance_date=date, ticker=ticker, cik='1', sector=sector,
                membership_quality='monthly_observation', identifier_quality='current_mapping',
                value_score=0.0 if eligible else None, growth_score=0.0,
                momentum_score=0.0, quality_score=0.0,
                available_pillar_count=4 if eligible else 3,
                composite_score=0.0 if eligible else None,
                is_score_eligible=eligible, is_research_signal_eligible=eligible,
                shares_source_tag='dei:EntityCommonStockSharesOutstanding',
                fcf_age_days=200, sbc_age_days=200, growth_age_days=200,
                shares_age_days=100, quarter_age_days=100, signal_price=100,
                fcf_status='available' if fcf else 'missing_instant_shares',
                sbc_status='available' if sbc else 'missing_same_period_reported_sbc',
                growth_status='available', quality_status='available'))
    assessment = dict(generated_at='2026-10-15T00:00:00+00:00',
                      warehouse='PRIVATE_LOCAL_PATH', policy='strict_ppe_us_gaap_research_signal_v1',
                      age_limits_days=dict(annual_flows=550, instant_shares=400, quarterly_income=200),
                      checks=dict(rows=8),
                      latest=dict(rebalance_date='2026-09-30', universe_count=4,
                                  composite_count=1, research_signal_count=1))
    return assessment, records


class DashboardContract(unittest.TestCase):
    def setUp(self):
        (ROOT / '.tmp').mkdir(exist_ok=True)
        self.directory = tempfile.TemporaryDirectory(dir=ROOT / '.tmp', prefix='dashboard-test-')
        self.addCleanup(self.directory.cleanup)
        self.output = Path(self.directory.name)
        assessment, records = fixture()
        publish_snapshot(self.output, assessment, records, 'SELECT audited_inputs')
        self.path = self.output / 'dashboard_snapshot.json'
        self.payload, self.frame = decode_snapshot(self.path.read_bytes())
        self.latest = self.frame[self.frame['rebalance_date'] == self.frame['rebalance_date'].max()]

    def test_snapshot_is_atomic_self_contained_and_archived_without_local_paths(self):
        archived = self.output / 'snapshots' / (self.payload['snapshot_id'] + '.json')
        self.assertEqual(archived.read_bytes(), self.path.read_bytes())
        self.assertNotIn('PRIVATE_LOCAL_PATH', self.path.read_text())
        self.assertFalse(list(self.output.rglob('*.tmp')))

    def test_period_and_sector_counts_keep_missing_inputs_in_denominator(self):
        period = coverage_by_period(self.frame).iloc[-1]
        self.assertEqual((period['universe_count'], period['eligible_count'], period['coverage_pct']), (4, 1, 25))
        groups = coverage_by_sector(self.latest)
        self.assertEqual(groups['universe_count'].sum(), 4)
        self.assertEqual(groups['eligible_count'].sum(), 1)
        self.assertIn('Chưa có sector', groups['sector_label'].to_list())

    def test_value_shortfalls_are_exclusive_and_reconcile(self):
        groups = exclusion_summary(self.latest).set_index('group')['count'].to_dict()
        self.assertEqual(groups, {'Có Value': 1, 'Thiếu FCF': 1, 'Thiếu SBC': 1, 'Thiếu cả FCF và SBC': 1})

    def test_filters_and_csv_preserve_zero_null_and_exact_scope(self):
        filtered = screener(filter_sectors(self.latest, ['TECHNOLOGY']), 'Tất cả')
        exported = pd.read_csv(io.BytesIO(csv_download(filtered)))
        self.assertEqual(set(exported['ticker']), {'ZERO', 'FCF'})
        self.assertEqual(exported.loc[exported['ticker'] == 'ZERO', 'composite_score'].iloc[0], 0)
        self.assertTrue(exported.loc[exported['ticker'] == 'FCF', 'composite_score'].isna().all())
        self.assertEqual(len(filter_sectors(self.latest, [])), 4)
        self.assertEqual(len(screener(self.latest, search='[NO MATCH]')), 0)

    def test_rejects_duplicate_mixed_and_invalid_policy_snapshots(self):
        variants = []
        duplicate = copy.deepcopy(self.payload)
        duplicate['records'].append(duplicate['records'][0])
        variants.append(duplicate)
        mixed = copy.deepcopy(self.payload)
        mixed['assessment']['latest']['universe_count'] = 99
        variants.append(mixed)
        stale = copy.deepcopy(self.payload)
        stale['records'][0]['shares_age_days'] = 401
        variants.append(stale)
        invalid = copy.deepcopy(self.payload)
        invalid['records'][0]['is_research_signal_eligible'] = 'False'
        variants.append(invalid)
        for payload in variants:
            with self.subTest(payload=variants.index(payload)), self.assertRaises(SnapshotError):
                decode_snapshot(json.dumps(payload))

    def app(self):
        return AppTest.from_file(ROOT / 'app/streamlit_app.py', default_timeout=20)

    def test_streamlit_views_filters_and_empty_search(self):
        with patch.dict(os.environ, {'QUANT_DASHBOARD_SNAPSHOT': str(self.path)}):
            app = self.app().run()
            self.assertFalse(app.exception)
            self.assertEqual([metric.value for metric in app.metric], ['4', '1', '25.00%', '3'])
            app.multiselect(key='sectors').set_value(['TECHNOLOGY']).run()
            self.assertEqual([metric.value for metric in app.metric], ['2', '1', '50.00%', '1'])
            app.radio(key='view').set_value('Screener').run()
            self.assertEqual(len(app.dataframe[0].value), 2)
            app.selectbox(key='eligibility').set_value('Đủ điều kiện').run()
            self.assertEqual(app.dataframe[0].value['ticker'].to_list(), ['ZERO'])
            app.text_input(key='ticker_search').set_value('unknown').run()
            self.assertTrue(any('Không có mã khớp' in item.value for item in app.info))
            app.radio(key='view').set_value('Chất lượng dữ liệu').run()
            self.assertEqual(len(app.dataframe[0].value), 5)
            app.radio(key='view').set_value('Backtest').run()
            self.assertTrue(any('Chưa có output backtest' in item.value for item in app.info))
            self.assertFalse(app.exception)

    def test_streamlit_missing_and_corrupt_snapshots_have_useful_states(self):
        for path in [self.output / 'missing.json', self.output / 'corrupt.json']:
            if path.name == 'corrupt.json':
                path.write_text('{broken', encoding='utf-8')
            with self.subTest(path=path.name), patch.dict(os.environ, {'QUANT_DASHBOARD_SNAPSHOT': str(path)}):
                app = self.app().run()
                self.assertFalse(app.exception)
                self.assertTrue(len(app.info) or len(app.error))

    def test_factor_lab_handles_constant_pairs_and_single_member_sector(self):
        with patch.dict(os.environ, {'QUANT_DASHBOARD_SNAPSHOT': str(self.path)}):
            app = self.app().run()
            app.radio(key='view').set_value('Phân tích factor').run()
            self.assertFalse(app.exception)
            self.assertGreaterEqual(len(app.get('plotly_chart')), 5)
            app.selectbox(key='factor_x').set_value('growth_score').run()
            self.assertNotEqual(app.selectbox(key='factor_y').value, 'growth_score')
            app.multiselect(key='sectors').set_value(['ENERGY']).run()
            self.assertFalse(app.exception)

    def test_explorer_keeps_null_scores_and_cuts_history_at_selected_date(self):
        with patch.dict(os.environ, {'QUANT_DASHBOARD_SNAPSHOT': str(self.path)}):
            app = self.app().run()
            app.radio(key='view').set_value('Hồ sơ mã').run()
            app.selectbox(key='explorer_ticker').set_value('FCF').run()
            self.assertFalse(app.exception)
            self.assertEqual(app.metric[0].value, '—')
            self.assertEqual(app.metric[-1].value, '—')
            app.selectbox(key='period').set_value(pd.Timestamp('2026-08-31')).run()
            exact = [item.value for item in app.dataframe if 'rebalance_date' in item.value.columns]
            self.assertTrue(exact)
            self.assertTrue(all(pd.to_datetime(table['rebalance_date']).max() <= pd.Timestamp('2026-08-31')
                                for table in exact))
            app.multiselect(key='sectors').set_value(['ENERGY']).run()
            self.assertFalse(app.exception)
            self.assertTrue(any('ít nhất hai mã' in item.value for item in app.info))


if __name__ == '__main__':
    unittest.main()
