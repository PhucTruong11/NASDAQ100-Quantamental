"""Targeted, reproducible refresh after fixing ingestion gaps; no synthetic prices/facts."""
import argparse
import csv
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from ingestion.adapters.edgar import EdgarAdapter
from ingestion.adapters.sector import SectorAdapter
from ingestion.adapters.yahoo import YahooAdapter
from ingestion.universe import NasdaqUniverse


def refresh_quality_inputs(refresh=False, prices=False):
    with open('dbt/seeds/universe_membership_history.csv', encoding='utf-8', newline='') as handle:
        history = list(csv.DictReader(handle))
    ciks = sorted({str(int(r['cik'])) for r in history if r['cik']})
    user_agent = NasdaqUniverse().user_agent
    adapter = EdgarAdapter(user_agent)
    adapter.raw_cache_dir.mkdir(parents=True, exist_ok=True)
    lock = threading.Lock()
    last_request = [0.0]

    def one(cik):
        source = f'https://data.sec.gov/api/xbrl/companyfacts/CIK{int(cik):010d}.json'
        path = adapter.raw_cache_dir / f'CIK{int(cik):010d}.json'
        audit_cache = Path('data/raw/sec_audit') / path.name
        try:
            if not refresh and path.exists():
                raw = json.loads(path.read_text(encoding='utf-8'))
            elif not refresh and audit_cache.exists():
                raw = json.loads(audit_cache.read_text(encoding='utf-8'))
                path.write_text(json.dumps(raw), encoding='utf-8')
            else:
                # At most ~3 requests/sec, with two in-flight requests.
                with lock:
                    time.sleep(max(0, 0.35 - (time.monotonic() - last_request[0])))
                    last_request[0] = time.monotonic()
                raw = adapter.fetch_company_facts(cik)
            if str(int(raw['cik'])) != cik:
                raise ValueError('SEC issuer ID mismatch')
            facts = adapter.parse_facts(raw, cik)
            if not facts.is_empty():
                adapter.save_to_parquet(facts, cik)
            return dict(cik=cik, source_url=source, entity=raw.get('entityName'),
                        namespaces=sorted(raw.get('facts', {})), rows=facts.height,
                        status='refreshed' if facts.height else 'unsupported_taxonomy_or_forms',
                        captured_at=datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat())
        except Exception as exc:
            return dict(cik=cik, source_url=source, status='failed', error=str(exc))

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(one, ciks))
    sectors = SectorAdapter()
    all_tickers = sorted({r['ticker'] for r in history})
    current_sector_path = Path('data/raw/sector/sector.parquet')
    known = set(pl.read_parquet(current_sector_path)['ticker'].to_list()) if current_sector_path.exists() else set()
    missing = [t for t in all_tickers if t not in known]
    if missing:
        fresh = sectors.fetch_sectors(missing)
        if fresh.height:
            sectors.save_to_parquet(fresh)
    price_results = {}
    if prices:
        yahoo = YahooAdapter()
        for ticker in ['EA', 'WBD', 'ANSS', 'WBA', 'SGEN', 'SPLK']:
            price_results[ticker] = yahoo.run(ticker)
    manifest = dict(generated_at=datetime.now(timezone.utc).isoformat(), companies=outcomes,
                    sector_attempted=missing, sector_failed=getattr(sectors, 'failed_tickers', []),
                    prices=price_results)
    out = Path('data/raw/quality_backfill.json')
    out.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(json.dumps({'companies':len(outcomes), 'refreshed':sum(r['status']=='refreshed' for r in outcomes),
                      'failed':sum(r['status']=='failed' for r in outcomes),
                      'unsupported':sum(r['status']=='unsupported_taxonomy_or_forms' for r in outcomes),
                      'sector_failed':manifest['sector_failed'], 'prices':price_results}, indent=2))
    return not any(r['status']=='failed' for r in outcomes)


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--refresh', action='store_true', help='Ignore the full SEC JSON cache')
    parser.add_argument('--prices', action='store_true', help='Also retry unresolved historical Yahoo prices')
    args = parser.parse_args()
    raise SystemExit(0 if refresh_quality_inputs(args.refresh, args.prices) else 1)
