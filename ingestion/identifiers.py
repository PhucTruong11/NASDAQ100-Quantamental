"""Reviewed issuer mappings scoped to observed membership windows, not membership proof."""
import csv
from datetime import date
from pathlib import Path

DEFAULT_REGISTRY = Path(__file__).resolve().parents[1] / 'config/historical_identifiers.csv'


def load_identifiers(path=DEFAULT_REGISTRY):
    if not Path(path).exists():
        return []
    with Path(path).open(encoding='utf-8', newline='') as handle:
        rows = list(csv.DictReader(handle))
    windows = {}
    for row in rows:
        row['cik'] = str(int(row['cik']))
        start, end = date.fromisoformat(row['valid_from']), date.fromisoformat(row['valid_to'])
        if end <= start or not row['source_url'].startswith('https://'):
            raise ValueError('Invalid reviewed identifier window/source')
        windows.setdefault(row['ticker'], []).append((start, end))
    for periods in windows.values():
        periods.sort()
        if any(a[1] > b[0] for a, b in zip(periods, periods[1:])):
            raise ValueError('Overlapping reviewed identifier windows')
    return rows


def lookup_identifier(ticker, as_of, registry=None):
    registry = load_identifiers() if registry is None else registry
    day = str(as_of)
    return next((r for r in registry if r['ticker'] == ticker
                 and r['valid_from'] <= day < r['valid_to']), None)


def enrich_history(rows, registry=None):
    registry = load_identifiers() if registry is None else registry
    enriched = []
    for original in rows:
        row = dict(original)
        match = lookup_identifier(row['ticker'], row['valid_from'], registry)
        if match and row['valid_to'] <= match['valid_to']:
            if row.get('cik') and str(int(row['cik'])) != match['cik']:
                raise ValueError(f"CIK conflict needs review: {row['ticker']}")
            if not row.get('cik'):
                row['cik'] = match['cik']
                row['identifier_quality'] = match['identifier_quality']
        enriched.append(row)
    return enriched
