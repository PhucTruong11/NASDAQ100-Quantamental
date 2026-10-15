"""Exercise production Gold SQL and singular tests on zero/missing/corrupted cases."""
import unittest
from pathlib import Path

import duckdb
from jinja2 import Environment, StrictUndefined

ROOT = Path(__file__).resolve().parents[1]
GOLD = ['gold_growth_pillar', 'gold_momentum_pillar', 'gold_value_pillar',
        'gold_quality_pillar', 'gold_compose_score']


class GoldSemantics(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect(':memory:')
        self.env = Environment(undefined=StrictUndefined)
        self.env.globals.update(ref=lambda name: name, config=lambda **kw: '')
        self.con.execute('''CREATE TABLE int_rebalance_universe AS
            SELECT d AS rebalance_date, t AS ticker, '1' AS cik, 'Tech' AS sector,
                'monthly_observation' AS membership_quality, 'current_mapping' AS identifier_quality
            FROM (VALUES (DATE '2025-01-31'),(DATE '2025-02-28')) dates(d)
            CROSS JOIN (VALUES ('FULL'),('MISSING'),('ZERO'),('NONE')) tickers(t)''')
        for table, score in [('int_growth_ratio', 'growth_zscore'),
                             ('int_momentum_ratio', 'momentum_zscore'),
                             ('int_fcf_yield', 'fcf_yield_zscore'),
                             ('int_sbc_ratio', 'sbc_zscore'),
                             ('int_earnings_surprise', 'earnings_surprise_zscore')]:
            self.con.execute(f'''CREATE TABLE {table} AS SELECT ticker, rebalance_date,
                CASE WHEN ticker='NONE' THEN NULL WHEN ticker='ZERO' THEN 0.0
                     ELSE 1.0 END::DOUBLE AS {score} FROM int_rebalance_universe''')
        self.con.execute("UPDATE int_fcf_yield SET fcf_yield_zscore=NULL WHERE ticker='MISSING'")
        for model in GOLD:
            self.con.execute(f'CREATE TABLE {model} AS ' + self.render(f'models/gold/{model}.sql'))

    def tearDown(self):
        self.con.close()

    def render(self, relative):
        return self.env.from_string((ROOT / 'dbt' / relative).read_text(encoding='utf-8')).render()

    def violations(self, name):
        return self.con.execute(self.render(f'tests/assert_gold_{name}.sql')).fetchall()

    def test_all_audits_accept_valid_missing_zero_and_multiple_dates(self):
        for path in sorted((ROOT / 'dbt/tests').glob('assert_gold_*.sql')):
            with self.subTest(test=path.name):
                self.assertEqual(self.con.execute(self.render(f'tests/{path.name}')).fetchall(), [])
        rows = self.con.execute('''SELECT ticker, available_pillar_count, composite_score,
            is_score_eligible, membership_quality FROM gold_compose_score
            WHERE rebalance_date='2025-01-31' ORDER BY ticker''').fetchall()
        self.assertEqual(rows, [('FULL', 4, 1.0, True, 'monthly_observation'),
                                ('MISSING', 3, None, False, 'monthly_observation'),
                                ('NONE', 0, None, False, 'monthly_observation'),
                                ('ZERO', 4, 0.0, True, 'monthly_observation')])

    def test_duplicate_key_is_detected(self):
        self.con.execute('INSERT INTO gold_growth_pillar SELECT * FROM gold_growth_pillar LIMIT 1')
        self.assertTrue(self.violations('grain'))

    def test_missing_and_extra_keys_are_detected(self):
        self.con.execute("DELETE FROM gold_growth_pillar WHERE ticker='FULL'")
        self.con.execute("UPDATE gold_momentum_pillar SET ticker='EXTRA' WHERE ticker='ZERO'")
        problems = {r[-1] for r in self.violations('preserves_universe')}
        self.assertEqual(problems, {'missing_gold_key', 'extra_gold_key'})

    def test_wrong_passthrough_is_detected(self):
        self.con.execute("UPDATE gold_quality_pillar SET quality_score=0 WHERE ticker='FULL'")
        self.assertTrue(self.violations('single_factor_scores'))

    def test_missing_value_must_not_be_filled_with_zero(self):
        self.con.execute("UPDATE gold_value_pillar SET value_score=0 WHERE ticker='MISSING'")
        self.assertTrue(self.violations('value_formula'))

    def test_composite_count_eligibility_and_score_are_checked(self):
        self.con.execute("UPDATE gold_compose_score SET available_pillar_count=4, is_score_eligible=true, composite_score=0 WHERE ticker='NONE'")
        self.assertTrue(self.violations('composite_rules'))

    def test_copied_composite_pillar_columns_are_checked(self):
        self.con.execute("UPDATE gold_compose_score SET value_score=2 WHERE ticker='FULL'")
        self.assertTrue(self.violations('composite_rules'))

    def test_sector_and_membership_provenance_are_checked(self):
        self.con.execute("UPDATE gold_growth_pillar SET sector='Wrong' WHERE ticker='FULL'")
        self.con.execute("UPDATE gold_compose_score SET membership_quality='effective' WHERE ticker='FULL'")
        self.assertEqual({r[0] for r in self.violations('provenance')}, {'gold_growth_pillar', 'gold_compose_score'})

    def test_nonfinite_and_out_of_bounds_scores_are_detected(self):
        self.con.execute("UPDATE gold_momentum_pillar SET momentum_score='NaN'::DOUBLE WHERE ticker='FULL'")
        self.con.execute("UPDATE gold_value_pillar SET value_score=4 WHERE ticker='ZERO'")
        self.assertEqual({r[0] for r in self.violations('scores_finite_and_bounded')}, {'gold_momentum_pillar', 'gold_value_pillar'})


if __name__ == '__main__':
    unittest.main()
