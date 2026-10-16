import csv
import tempfile
import unittest
from unittest.mock import patch, Mock
from datetime import date
from pathlib import Path

import polars as pl

from ingestion.adapters.edgar import EdgarAdapter
from ingestion.adapters.sector import SectorAdapter
from ingestion.adapters.yahoo import YahooAdapter
from ingestion.identifiers import enrich_history, load_identifiers
from ingestion.universe import NasdaqUniverse


def local_temp():
    root=Path(__file__).resolve().parents[1]/'.tmp'
    root.mkdir(exist_ok=True)
    return tempfile.TemporaryDirectory(dir=root,prefix='quality-test-')


class QualityIngestion(unittest.TestCase):
    def test_universe_keeps_member_when_directory_has_no_identifier(self):
        response=Mock();response.json.return_value={'0':{'ticker':'KNOWN','cik_str':1,'title':'Known'}}
        members=pl.DataFrame({'Symbol':['KNOWN','UNKNOWN'],'Name':['Known','Unknown']})
        with local_temp() as directory:
            with patch('ingestion.universe.requests.get',return_value=response), \
                 patch('ingestion.universe.pl.read_csv',return_value=members), \
                 patch('ingestion.universe.load_identifiers',return_value=[]):
                result=NasdaqUniverse(output_dir=directory).get_constituents()
        self.assertEqual(result.height,2)
        self.assertEqual(result.filter(pl.col('Ticker')=='UNKNOWN')['CIK'].to_list(),[None])

    def test_dei_and_foreign_annual_forms_preserve_concept_names_and_units(self):
        fact={'val':100,'end':'2024-12-31','filed':'2025-02-15','form':'20-F','fy':2024,'fp':'FY'}
        data={'facts':{'dei':{'EntityCommonStockSharesOutstanding':{'units':{'shares':[fact]}}},
                       'us-gaap':{'NetCashProvidedByUsedInOperatingActivities':{'units':{'USD':[dict(fact,start='2024-01-01')]}}},
                       'ifrs-full':{'CashFlowsFromUsedInOperatingActivities':{'units':{'EUR':[fact]}}}}}
        with local_temp() as directory:
            df=EdgarAdapter('Test research',str(Path(directory)/'edgar')).parse_facts(data,'0000000001')
        self.assertEqual(set(df['tag']),{'NetCashProvidedByUsedInOperatingActivities','dei:EntityCommonStockSharesOutstanding'})
        self.assertEqual(set(df['cik']),{'1'})
        self.assertEqual(set(df['form']),{'20-F'})
        self.assertEqual(set(df['unit']),{'USD','shares'})

    def test_sector_refresh_keeps_removed_constituents(self):
        with local_temp() as directory:
            adapter=SectorAdapter(output_dir=directory)
            adapter.save_to_parquet(pl.DataFrame({'ticker':['OLD','NOW'],'sector':['Tech','Old'],'industry':['A','B']}))
            adapter.save_to_parquet(pl.DataFrame({'ticker':['NOW'],'sector':['New'],'industry':['C']}))
            rows={r['ticker']:r['sector'] for r in pl.read_parquet(Path(directory)/'sector.parquet').to_dicts()}
        self.assertEqual(rows,{'OLD':'Tech','NOW':'New'})

    def test_partial_sector_refresh_distinguishes_new_write_from_old_file(self):
        with local_temp() as directory:
            adapter=SectorAdapter(output_dir=directory,min_success_ratio=0.9)
            fresh=pl.DataFrame({'ticker':[f'T{i}' for i in range(9)],'sector':['Tech']*9,'industry':['A']*9})
            def fetch(tickers):
                adapter.failed_tickers=['T9']
                return fresh
            adapter.fetch_sectors=fetch
            self.assertFalse(adapter.run([f'T{i}' for i in range(10)]))
            self.assertTrue(adapter.last_write_succeeded)
            self.assertEqual(adapter.last_fetched_count,9)
            adapter.fetch_sectors=lambda tickers:fresh.head(1)
            self.assertFalse(adapter.run([f'T{i}' for i in range(10)]))
            self.assertFalse(adapter.last_write_succeeded)
            self.assertEqual(pl.read_parquet(Path(directory)/'sector.parquet').height,9)

    def test_price_refresh_cannot_drop_existing_history(self):
        with local_temp() as directory:
            adapter=YahooAdapter(output_dir=directory)
            old=pl.DataFrame({'Date':[date(2024,1,2),date(2024,1,3)],'Ticker':['AAA','AAA'],'Close':[10.,11.]})
            adapter.save_to_parquet(old,'AAA')
            with self.assertRaises(ValueError):
                adapter.save_to_parquet(old.head(1),'AAA')
            self.assertEqual(pl.read_parquet(Path(directory)/'price_AAA.parquet').height,2)

    def test_identifier_enrichment_preserves_membership_and_detects_conflicts(self):
        row={'ticker':'OLD','cik':'','valid_from':'2023-07-01','valid_to':'2023-08-01',
             'membership_quality':'monthly_observation','identifier_quality':'unmapped'}
        mapping={'ticker':'OLD','cik':'1','valid_from':'2023-01-01','valid_to':'2024-01-01',
                 'identifier_quality':'sec_filing_crosscheck'}
        result=enrich_history([row],[mapping])[0]
        self.assertEqual(result['cik'],'1')
        self.assertEqual(result['membership_quality'],'monthly_observation')
        self.assertEqual(result['valid_from'],row['valid_from'])
        with self.assertRaises(ValueError):
            enrich_history([dict(row,cik='2')],[mapping])
        self.assertEqual(enrich_history([dict(row,valid_to='2025-01-01')],[mapping])[0]['cik'],'')

    def test_reviewed_identifier_windows_cannot_overlap(self):
        with local_temp() as directory:
            path=Path(directory)/'ids.csv'
            row={'ticker':'AAA','cik':'1','valid_from':'2023-01-01','valid_to':'2024-01-01','source_url':'https://www.sec.gov/example'}
            with path.open('w',newline='') as handle:
                writer=csv.DictWriter(handle,fieldnames=list(row));writer.writeheader();writer.writerows([row,row])
            with self.assertRaises(ValueError):load_identifiers(path)


if __name__=='__main__':
    unittest.main()
