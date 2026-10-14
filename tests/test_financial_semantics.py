"""Execute production SQL against edge cases: python -m unittest discover -s tests -v."""
import unittest
from pathlib import Path

import duckdb
from jinja2 import Environment, StrictUndefined

from ingestion.universe_history import write_history

ROOT = Path(__file__).resolve().parents[1]


class FinancialSemantics(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect(':memory:')
        self.env = Environment(undefined=StrictUndefined)
        self.env.globals.update(ref=lambda name: name, config=lambda **kw: '', var=lambda name, default=False: default)
        self.con.execute('''CREATE TABLE stg_fundamentals (
            cik VARCHAR, metric_tag VARCHAR, metric_value DOUBLE, unit_of_measure VARCHAR,
            filing_type VARCHAR, fiscal_year INTEGER, fiscal_period VARCHAR,
            period_start_date DATE, period_end_date DATE, filing_date DATE)''')
        self.con.execute("CREATE TABLE int_rebalance_universe AS SELECT DATE '2025-01-31' rebalance_date, 'AAA' ticker, '1' cik, 'Tech' sector")
        self.con.execute('''CREATE TABLE silver_price_adjusted (
            ticker VARCHAR, trade_date DATE, adjusted_close_price DOUBLE,
            nominal_close_price DOUBLE, split_ratio DOUBLE)''')
        self.con.execute("INSERT INTO silver_price_adjusted VALUES ('AAA','2025-01-31',9,10,0)")

    def tearDown(self):
        self.con.close()

    def sql(self, relative):
        macro = (ROOT / 'dbt/macros/zscore_sector_neutral.sql').read_text(encoding='utf-8')
        body = (ROOT / 'dbt' / relative).read_text(encoding='utf-8')
        return self.env.from_string(macro + body).render()

    def build(self, model):
        layer = 'silver' if model.startswith('silver_') else 'intermediate'
        self.con.execute(f'CREATE OR REPLACE TABLE {model} AS ' + self.sql(f'models/{layer}/{model}.sql'))

    def fact(self, tag, value, start='2024-01-01', end='2024-12-31', filed='2025-01-15', unit='USD', fp='FY'):
        self.con.execute('INSERT INTO stg_fundamentals VALUES (?,?,?,?,?,?,?,?,?,?)',
                         ['1', tag, value, unit, '10-K', 2024, fp, start, end, filed])

    def fundamentals(self):
        self.build('silver_fundamentals_pit')
        self.build('silver_fundamentals_annual')

    def test_fy_quarter_currency_and_53_week_year(self):
        self.fact('Revenues', 400)
        self.fact('Revenues', 9999, start='2024-10-01')  # Quarter in 10-K
        self.fact('Revenues', 1000, unit='AFN')
        self.fact('ShareBasedCompensation', 40, start='2023-12-25', fp='Q1')
        self.fundamentals()
        values = self.con.execute('SELECT metric_value FROM silver_fundamentals_annual ORDER BY 1').fetchall()
        self.assertEqual(values, [(40.0,), (400.0,)])

    def test_growth_matches_period_and_blocks_future_comparative(self):
        self.fact('Revenues', 400)
        self.fact('NetIncomeLoss', 100)
        self.fact('NetIncomeLoss', 9999, start='2023-01-01', end='2023-12-31')
        self.fact('Revenues', 300, start='2023-01-01', end='2023-12-31', filed='2025-02-01')
        self.fundamentals()
        self.build('int_growth_ratio')
        margin, growth = self.con.execute('SELECT net_margin, rev_growth_yoy FROM int_growth_ratio').fetchone()
        self.assertEqual(margin, 0.25)
        self.assertIsNone(growth)

    def test_filing_on_rebalance_day_is_not_available_at_close(self):
        self.fact('Revenues', 400, filed='2025-01-31')
        self.fundamentals()
        self.build('int_growth_ratio')
        self.assertIsNone(self.con.execute('SELECT revenue_available_date FROM int_growth_ratio').fetchone()[0])

    def fcf(self):
        self.fundamentals()
        self.build('int_fcf_yield')
        return self.con.execute('SELECT free_cash_flow, market_cap, shares_outstanding, fcf_yield_raw FROM int_fcf_yield ORDER BY ticker').fetchone()

    def test_missing_capex_remains_missing(self):
        self.fact('NetCashProvidedByUsedInOperatingActivities', 100)
        self.fact('CommonStockSharesOutstanding', 10, start=None, unit='SHARES')
        self.assertEqual(self.fcf(), (None, 100.0, 10.0, None))

    def test_insufficient_cross_section_does_not_invent_clipped_zscore(self):
        self.fact('NetCashProvidedByUsedInOperatingActivities', 100)
        self.fact('PaymentsToAcquirePropertyPlantAndEquipment', 0)
        self.fact('CommonStockSharesOutstanding', 10, start=None, unit='SHARES')
        self.fcf()
        self.assertIsNone(self.con.execute('SELECT fcf_yield_zscore FROM int_fcf_yield').fetchone()[0])

    def test_missing_sector_does_not_form_an_unknown_sector_bucket(self):
        self.con.execute("CREATE TABLE example AS SELECT DATE '2025-01-31' rebalance_date, NULL::VARCHAR sector, 1.0 x UNION ALL SELECT DATE '2025-01-31', 'Tech', 2.0")
        macro = (ROOT / 'dbt/macros/zscore_sector_neutral.sql').read_text(encoding='utf-8')
        query = self.env.from_string(macro + "SELECT {{ zscore_sector_neutral('x') }} FROM example WHERE TRUE ORDER BY x").render()
        self.assertIsNone(self.con.execute(query).fetchall()[0][0])

    def test_capex_must_match_ocf_period(self):
        self.fact('NetCashProvidedByUsedInOperatingActivities', 100)
        self.fact('PaymentsToAcquirePropertyPlantAndEquipment', 20, start='2023-01-01', end='2023-12-31')
        self.fact('CommonStockSharesOutstanding', 10, start=None, unit='SHARES')
        self.assertIsNone(self.fcf()[0])

    def test_split_between_measurement_and_rebalance(self):
        self.fact('NetCashProvidedByUsedInOperatingActivities', 100)
        self.fact('PaymentsToAcquirePropertyPlantAndEquipment', 0)
        self.fact('CommonStockSharesOutstanding', 10, start=None, unit='SHARES')
        self.con.execute("INSERT INTO silver_price_adjusted VALUES ('AAA','2025-01-20',9,10,2)")
        self.assertEqual(self.fcf(), (100.0, 200.0, 20.0, 0.5))

    def test_future_split_restores_nominal_price(self):
        self.con.execute('''CREATE TABLE stg_price_eod (
            ticker VARCHAR, trade_date DATE, close_price DOUBLE, volume BIGINT,
            split_ratio DOUBLE, dividend_amount DOUBLE)''')
        self.con.execute("INSERT INTO stg_price_eod VALUES ('AAA','2025-01-31',10,100,0,0), ('AAA','2025-02-03',10,100,2,0)")
        self.build('silver_price_adjusted')
        self.assertEqual(self.con.execute('SELECT nominal_close_price FROM silver_price_adjusted ORDER BY trade_date').fetchall(), [(20.0,), (10.0,)])

    def test_multiple_share_classes_do_not_get_duplicated_issuer_market_cap(self):
        self.fact('NetCashProvidedByUsedInOperatingActivities', 100)
        self.fact('PaymentsToAcquirePropertyPlantAndEquipment', 20)
        self.fact('CommonStockSharesOutstanding', 10, start=None, unit='SHARES')
        self.con.execute("INSERT INTO int_rebalance_universe VALUES ('2025-01-31','AAB','1','Tech')")
        self.assertIsNone(self.fcf()[1])

    def test_momentum_uses_history_before_membership_entry(self):
        self.con.execute("INSERT INTO silver_price_adjusted VALUES ('AAA','2024-01-31',5,5,0), ('AAA','2024-12-31',10,10,0)")
        self.build('int_momentum_ratio')
        self.assertEqual(self.con.execute('SELECT momentum_12_1_raw FROM int_momentum_ratio').fetchone()[0], 1.0)

    def test_universe_interval_bounds_and_strict_mode(self):
        self.con.execute('DROP TABLE int_rebalance_universe')
        self.con.execute('''CREATE TABLE silver_universe_membership_pit (
            ticker VARCHAR, cik VARCHAR, valid_from TIMESTAMP, valid_to TIMESTAMP,
            membership_quality VARCHAR, identifier_quality VARCHAR)''')
        self.con.execute("CREATE TABLE stg_sector AS SELECT 'AAA' ticker, 'Tech' sector")
        self.con.execute("INSERT INTO silver_universe_membership_pit VALUES ('AAA','1','2025-02-01',NULL,'effective','verified')")
        self.build('int_rebalance_universe')
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM int_rebalance_universe').fetchone()[0], 0)
        self.con.execute("UPDATE silver_universe_membership_pit SET valid_from='2025-01-01',valid_to='2025-01-31'")
        self.build('int_rebalance_universe')
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM int_rebalance_universe').fetchone()[0], 0)
        self.con.execute("UPDATE silver_universe_membership_pit SET valid_to='2025-02-01',membership_quality='monthly_observation'")
        self.build('int_rebalance_universe')
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM int_rebalance_universe').fetchone()[0], 0)
        self.env.globals['var'] = lambda name, default=False: True
        self.build('int_rebalance_universe')
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM int_rebalance_universe').fetchone()[0], 1)

    def test_history_rejects_overlapping_windows(self):
        rows = [dict(ticker='AAA', cik='1', company='A', valid_from='2025-01-01', valid_to='2025-03-01',
                     membership_quality='effective', identifier_quality='verified', source_url='https://example.org/source'),
                dict(ticker='AAA', cik='1', company='A', valid_from='2025-02-01', valid_to='2025-04-01',
                     membership_quality='effective', identifier_quality='verified', source_url='https://example.org/source')]
        # Validation must reject before any filesystem operation.
        with self.assertRaises(ValueError):
            write_history(rows, ROOT / 'must-not-be-written.csv')


if __name__ == '__main__':
    unittest.main()
