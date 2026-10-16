"""Statistical and comparison contracts for the research dashboard."""
import unittest

import numpy as np
import pandas as pd

from app.analytics import (pillar_coverage_by_period, rank_movement,
                           score_correlations, score_distribution,
                           sector_pillar_summary, ticker_history)
from app.data import MISSING_SECTOR, SCORE_COLUMNS


def snapshot():
    records = []
    rows = [('2026-07-31', 'A', 'TECHNOLOGY', 3),
            ('2026-07-31', 'B', 'TECHNOLOGY', 2),
            ('2026-08-31', 'A', 'TECHNOLOGY', 1),
            ('2026-08-31', 'B', 'TECHNOLOGY', None),
            ('2026-08-31', 'Z', 'ENERGY', 9),
            ('2026-09-30', 'A', 'TECHNOLOGY', 2),
            ('2026-09-30', 'B', 'TECHNOLOGY', 2),
            ('2026-09-30', 'C', 'TECHNOLOGY', 0),
            ('2026-09-30', 'Z', 'ENERGY', 9),
            ('2026-09-30', 'UNKNOWN', None, None)]
    for date, ticker, sector, score in rows:
        records.append(dict(rebalance_date=pd.Timestamp(date), ticker=ticker,
                            sector=sector, sector_label=sector or MISSING_SECTOR,
                            **{column: score for column in SCORE_COLUMNS}))
    return pd.DataFrame(records)


class DashboardAnalytics(unittest.TestCase):
    def setUp(self):
        self.frame = snapshot()
        self.latest = self.frame[self.frame['rebalance_date'].eq(pd.Timestamp('2026-09-30'))]

    def test_pillar_denominators_include_missing_scores_and_zero_is_present(self):
        coverage = pillar_coverage_by_period(self.frame)
        latest = coverage[coverage['rebalance_date'].eq(pd.Timestamp('2026-09-30'))]
        self.assertEqual(len(latest), 5)
        self.assertTrue(latest['universe_count'].eq(5).all())
        self.assertTrue(latest['available_count'].eq(4).all())
        self.assertTrue(latest['coverage_pct'].eq(80).all())

    def test_sector_medians_preserve_missing_sector_and_all_missing_group(self):
        summary = sector_pillar_summary(self.latest)
        missing = summary[summary['sector_label'].eq(MISSING_SECTOR)]
        self.assertEqual(len(missing), 5)
        self.assertTrue(missing['median_score'].isna().all())
        self.assertTrue(missing['universe_count'].eq(1).all())
        self.assertTrue(missing['available_count'].eq(0).all())
        technology = summary[summary['sector_label'].eq('TECHNOLOGY')]
        self.assertTrue(technology['median_score'].eq(2).all())

    def test_pairwise_spearman_masks_infinite_constant_and_small_samples(self):
        frame = pd.DataFrame(dict(a=[1, 2, 3, 4, np.inf],
                                  b=[4, 3, 2, None, 0],
                                  constant=[7, 7, 7, 7, 7],
                                  sparse=[1, None, 2, None, None]))
        correlations, counts = score_correlations(frame, list(frame.columns))
        self.assertAlmostEqual(correlations.loc['a', 'b'], -1)
        self.assertEqual(counts.loc['a', 'b'], 3)
        self.assertEqual(counts.loc['a', 'a'], 4)
        self.assertTrue(np.isnan(correlations.loc['constant', 'constant']))
        self.assertEqual(counts.loc['constant', 'constant'], 5)
        self.assertTrue(np.isnan(correlations.loc['a', 'sparse']))
        self.assertEqual(counts.loc['a', 'sparse'], 2)
        pd.testing.assert_frame_equal(correlations, correlations.T)

    def test_spearman_average_ties_and_pairwise_not_global_complete_cases(self):
        frame = pd.DataFrame(dict(a=[1, 1, 2, 3], b=[4, 3, 2, 1],
                                  unrelated=[None, None, None, None]))
        correlations, counts = score_correlations(frame, list(frame.columns))
        self.assertEqual(counts.loc['a', 'b'], 4)
        expected = np.corrcoef([1.5, 1.5, 3, 4], [4, 3, 2, 1])[0, 1]
        self.assertAlmostEqual(correlations.loc['a', 'b'], expected)

    def test_rank_movement_uses_same_sector_scope_min_ties_and_no_prior_fill(self):
        ranked = rank_movement(self.frame, '2026-09-30', ['TECHNOLOGY']).set_index('ticker')
        self.assertEqual(set(ranked.index), {'A', 'B', 'C'})
        self.assertEqual(ranked['rank'].to_dict(), {'A': 1, 'B': 1, 'C': 3})
        self.assertEqual(ranked.loc['A', 'previous_rank'], 1)
        self.assertEqual(ranked.loc['A', 'rank_change'], 0)
        # B has an older score in July, but the immediately prior August is missing.
        self.assertTrue(np.isnan(ranked.loc['B', 'previous_rank']))
        self.assertTrue(np.isnan(ranked.loc['B', 'rank_change']))
        self.assertTrue(np.isnan(ranked.loc['C', 'previous_rank']))
        self.assertTrue(ranked['previous_period'].eq(pd.Timestamp('2026-08-31')).all())

    def test_prior_date_is_global_reviewed_date_even_when_sector_has_no_rows(self):
        work = self.frame[~(self.frame['rebalance_date'].eq(pd.Timestamp('2026-08-31'))
                           & self.frame['sector_label'].eq('TECHNOLOGY'))]
        ranked = rank_movement(work, '2026-09-30', ['TECHNOLOGY'])
        self.assertTrue(ranked['previous_rank'].isna().all())
        first = rank_movement(self.frame, '2026-07-31')
        self.assertTrue(first['previous_period'].isna().all())
        self.assertTrue(first['rank_change'].isna().all())

    def test_ticker_history_preserves_absent_membership_and_present_missing_scores(self):
        history = ticker_history(self.frame, 'C')
        self.assertEqual(len(history), 3)
        self.assertEqual(history['present_in_universe'].to_list(), [False, False, True])
        self.assertTrue(history['composite_score'].iloc[:2].isna().all())
        self.assertEqual(history['composite_score'].iloc[-1], 0)
        b_history = ticker_history(self.frame, 'B')
        self.assertTrue(b_history['present_in_universe'].all())
        self.assertTrue(np.isnan(b_history['composite_score'].iloc[1]))
        self.assertEqual(len(ticker_history(self.latest, 'C', periods=self.frame['rebalance_date'])), 3)

    def test_empty_populations_have_defined_outputs_and_no_fabricated_statistics(self):
        empty = self.frame.iloc[:0]
        self.assertTrue(pillar_coverage_by_period(empty).empty)
        self.assertTrue(sector_pillar_summary(empty).empty)
        correlations, counts = score_correlations(empty)
        self.assertTrue(correlations.isna().all().all())
        self.assertTrue(counts.eq(0).all().all())
        distribution = score_distribution(empty)
        self.assertEqual(distribution['available_count'], 0)
        self.assertTrue(np.isnan(distribution['median']))
        self.assertTrue(rank_movement(empty, '2026-09-30').empty)
        self.assertTrue(ticker_history(empty, 'A').empty)


if __name__ == '__main__':
    unittest.main()
