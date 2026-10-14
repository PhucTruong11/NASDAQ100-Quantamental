"""Import dated index observations, without inventing index effective dates.

Run: python -m ingestion.universe_history --output dbt/seeds/universe_membership_history.csv
For a verified effective-date dataset use --import-csv FILE instead of downloads.
"""
import argparse
import csv
import io
import logging
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

FIELDS = ['ticker', 'cik', 'company', 'valid_from', 'valid_to', 'membership_quality',
          'identifier_quality', 'source_url']
REPO = 'yfiua/index-constituents'
logger = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def get(url, **kwargs):
    if url.startswith('https://api.github.com/') and os.environ.get('GITHUB_TOKEN'):
        kwargs.setdefault('headers', {})['Authorization'] = 'Bearer ' + os.environ['GITHUB_TOKEN']
    response = requests.get(url, timeout=30, **kwargs)
    response.raise_for_status()
    return response


def write_history(rows, output):
    """Validate complete half-open windows before replacing the seed atomically."""
    seen = set()
    by_ticker = {}
    for row in rows:
        start, end = date.fromisoformat(row['valid_from']), date.fromisoformat(row['valid_to'])
        if not row['ticker'] or not row['source_url'].startswith('https://') or end <= start:
            raise ValueError(f'Invalid membership window: {row}')
        if row['membership_quality'] not in ('effective', 'monthly_observation'):
            raise ValueError('membership_quality must be effective or monthly_observation')
        key = row['ticker'], start
        if key in seen:
            raise ValueError(f'Duplicate membership key: {key}')
        seen.add(key)
        by_ticker.setdefault(row['ticker'], []).append((start, end))
    for ticker, windows in by_ticker.items():
        windows.sort()
        if any(a[1] > b[0] for a, b in zip(windows, windows[1:])):
            raise ValueError(f'Overlapping membership windows: {ticker}')
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix('.csv.tmp')
    with temporary.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda row: (row['valid_from'], row['ticker'])))
    temporary.replace(output)


def fetch_archive(path):
    cache = Path('data/raw/universe_archive') / (path.replace('/', '_') + '.json')
    if cache.exists():
        stored = json.loads(cache.read_text(encoding='utf-8'))
        return date.fromisoformat(stored['available']), stored['members'], stored['source_url']
    # Read the earliest published VERSION, not a later revision backdated into
    # the archive month. A day-only date becomes usable on the following day.
    commits = get(f'https://api.github.com/repos/{REPO}/commits',
                  params={'path': path, 'per_page': 100}).json()
    if not commits or len(commits) == 100:
        raise ValueError(f'Cannot establish first publication for {path}')
    original = commits[-1]
    observed = datetime.fromisoformat(original['commit']['committer']['date'].replace('Z', '+00:00')).date()
    available = observed + timedelta(days=1)
    url = f'https://raw.githubusercontent.com/{REPO}/{original["sha"]}/{path}'
    constituents = list(csv.DictReader(io.StringIO(get(url).text)))
    if len(constituents) < 90 or len({x['Symbol'] for x in constituents}) != len(constituents):
        raise ValueError(f'Incomplete or duplicate archive: {path}')
    cache.parent.mkdir(parents=True, exist_ok=True)
    temporary = cache.with_suffix('.json.tmp')
    temporary.write_text(json.dumps({'available': available.isoformat(), 'members': constituents,
                                    'source_url': url}), encoding='utf-8')
    temporary.replace(cache)
    return available, constituents, url


def backfill(output, through=None):
    through = through or date.today()
    if Path(output).exists():
        with Path(output).open(encoding='utf-8', newline='') as handle:
            if any(r['membership_quality'] == 'effective' for r in csv.DictReader(handle)):
                raise ValueError('Output contains effective-date history; choose a separate output before downloading observations')
    tree = get(f'https://api.github.com/repos/{REPO}/git/trees/main', params={'recursive': 1}).json()
    import re
    paths = sorted(x['path'] for x in tree['tree']
                   if re.fullmatch(r'docs/\d{4}/\d{2}/constituents-nasdaq100\.csv', x['path']))
    paths = [p for p in paths if date(int(p.split('/')[1]), int(p.split('/')[2]), 1) <= through]
    # Mapping remains provisional for historical tickers: keep unknown issuers
    # in membership with NULL CIK rather than deleting failed mappings.
    local = Path('data/raw/universe/universe.parquet')
    mapping = {}
    if local.exists():
        import polars as pl
        mapping = {r['Ticker']: r['CIK'] for r in pl.read_parquet(local).to_dicts()}
    with ThreadPoolExecutor(max_workers=4) as pool:
        snapshots = sorted(pool.map(fetch_archive, paths), key=lambda x: x[0])
    snapshots = [s for s in snapshots if s[0] <= through]
    rows = []
    for index, (available, members, url) in enumerate(snapshots):
        # Never fill an arbitrarily long missing-archive gap. Latest observation
        # expires too, so a stale seed cannot silently serve future backtests.
        end = min(available + timedelta(days=40), through + timedelta(days=1))
        if index + 1 < len(snapshots):
            end = min(end, snapshots[index + 1][0])
        if end <= available:
            continue
        for member in members:
            ticker = member['Symbol'].strip().upper()
            rows.append(dict(ticker=ticker, cik=mapping.get(ticker, ''), company=member['Name'],
                valid_from=available.isoformat(), valid_to=end.isoformat(),
                membership_quality='monthly_observation',
                identifier_quality='current_mapping' if ticker in mapping else 'unmapped', source_url=url))
    if not rows:
        raise ValueError('No usable historical observations; existing seed retained')
    write_history(rows, output)
    logger.info('Saved %d membership observations (%s to %s). Exact effective dates remain unavailable.',
                len(rows), min(r['valid_from'] for r in rows), max(r['valid_to'] for r in rows))
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', default='dbt/seeds/universe_membership_history.csv')
    parser.add_argument('--through', type=date.fromisoformat)
    parser.add_argument('--import-csv', type=Path)
    args = parser.parse_args()
    if args.import_csv:
        with args.import_csv.open(encoding='utf-8', newline='') as handle:
            write_history(list(csv.DictReader(handle)), args.output)
    else:
        backfill(args.output, args.through)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
