"""Audit Gold coverage and define an as-of research signal sample, without future-return filtering."""
import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from research.dashboard_snapshot import publish_snapshot

ROOT = Path(__file__).resolve().parents[1]


def score_summary(connection):
    result = connection.execute('''SELECT rebalance_date,COUNT(*) AS universe_count,
        COUNT(value_score) AS value_count,COUNT(growth_score) AS growth_count,
        COUNT(momentum_score) AS momentum_count,COUNT(quality_score) AS quality_count,
        COUNT(composite_score) AS composite_count
        FROM gold_compose_score GROUP BY 1 ORDER BY 1''')
    columns = [c[0] for c in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def write_csv(path, rows, fieldnames=None):
    temporary=path.with_suffix(path.suffix+'.tmp')
    with temporary.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames or list(rows[0]))
        writer.writeheader(); writer.writerows(rows)
    temporary.replace(path)


def audit(warehouse, output, baseline=None, annual_age=None, shares_age=None, quarter_age=None):
    output = Path(output); output.mkdir(parents=True, exist_ok=True)
    manifests=list((ROOT/'data/reports/gold').glob('*-run.json'))
    build_limits={'annual_flows':550,'instant_shares':400,'quarterly_income':200}
    gold_build = {}
    if manifests:
        latest_manifest=max(manifests,key=lambda p:p.stat().st_mtime)
        payload=json.loads(latest_manifest.read_text(encoding='utf-8'))
        if payload:
            gold_build = payload[0]
            build_limits.update(payload[0].get('input_age_limits_days',{}))
    annual_age=build_limits['annual_flows'] if annual_age is None else annual_age
    shares_age=build_limits['instant_shares'] if shares_age is None else shares_age
    quarter_age=build_limits['quarterly_income'] if quarter_age is None else quarter_age
    if min(annual_age,shares_age,quarter_age)<1:
        raise ValueError('Input-age limits must be positive')
    connection = duckdb.connect(str(warehouse), read_only=True)
    sql = (ROOT / 'research/sql/backtest_inputs.sql').read_text(encoding='utf-8')
    result = connection.execute(sql, {'annual_age':annual_age,'shares_age':shares_age,'quarter_age':quarter_age,
        'build_annual_age':build_limits['annual_flows'],'build_shares_age':build_limits['instant_shares'],
        'build_quarter_age':build_limits['quarterly_income']})
    fields = [c[0] for c in result.description]
    records = [dict(zip(fields, row)) for row in result.fetchall()]
    summary = score_summary(connection)
    by_sector = defaultdict(lambda: {'universe_count':0,'composite_count':0,'research_signal_count':0})
    source_path = ROOT / 'data/raw/quality_backfill.json'
    sources = json.loads(source_path.read_text(encoding='utf-8')) if source_path.exists() else {}
    profiles = {r['cik']:r for r in sources.get('companies', [])}
    for row in records:
        source = profiles.get(row['cik'], {})
        row['reporting_scope'] = ('ifrs_not_mapped' if 'ifrs-full' in source.get('namespaces', [])
                                  and 'us-gaap' not in source.get('namespaces', []) else 'us_gaap_or_unknown')
        group = by_sector[(row['rebalance_date'], row['sector'] or '__MISSING_SECTOR__')]
        group['universe_count'] += 1
        group['composite_count'] += row['composite_score'] is not None
        group['research_signal_count'] += row['is_research_signal_eligible']
    latest_date = max(row['rebalance_date'] for row in records)
    latest = [r for r in records if r['rebalance_date']==latest_date]
    reasons = {factor:dict(Counter(r[factor+'_status'] for r in latest))
               for factor in ['fcf','sbc','growth','quality']}
    duplicates = connection.execute('''SELECT COUNT(*) FROM (SELECT ticker,rebalance_date
        FROM gold_compose_score GROUP BY 1,2 HAVING COUNT(*)>1)''').fetchone()[0]
    gaps = connection.execute('''SELECT u.ticker,COUNT(*) missing_rebalances,
        MIN(u.rebalance_date) first_missing,MAX(u.rebalance_date) last_missing
        FROM int_rebalance_universe u LEFT JOIN silver_price_adjusted p
          ON p.ticker=u.ticker AND p.trade_date=u.rebalance_date
        WHERE p.ticker IS NULL GROUP BY 1 ORDER BY 2 DESC,1''').fetchall()
    summary_by_date = {r['rebalance_date']:r for r in summary}
    for date, row in summary_by_date.items():
        row['research_signal_count'] = sum(r['is_research_signal_eligible'] for r in records if r['rebalance_date']==date)
        row['composite_coverage_pct'] = round(100*row['composite_count']/row['universe_count'],2)
    if baseline:
        before = duckdb.connect(str(baseline), read_only=True)
        previous = {r['rebalance_date']:r for r in score_summary(before)}
        for date, row in summary_by_date.items():
            prior = previous.get(date, {})
            row['before_composite_count'] = prior.get('composite_count')
            row['before_value_count'] = prior.get('value_count')
        before.close()
    connection.close()
    checks = {'duplicate_gold_keys':duplicates,'rows':len(records),
              'membership_qualities':dict(Counter(r['membership_quality'] for r in records)),
              'missing_cik_rows':sum(r['cik'] is None for r in records),
              'missing_sector_rows':sum(r['sector'] is None for r in records),
              'missing_signal_price_rows':sum(r['signal_price'] is None for r in records)}
    if duplicates or len(records)!=len({(r['ticker'],r['rebalance_date']) for r in records}):
        raise ValueError('Invalid Gold grain; do not use research sample')
    if not any(r['is_research_signal_eligible'] for r in records):
        raise ValueError('No usable research signals under the input-age policy')
    sector_rows = [dict(rebalance_date=d,sector=s,**v) for (d,s),v in sorted(by_sector.items())]
    write_csv(output/'ticker_diagnostics.csv', records)
    write_csv(output/'coverage_by_period.csv', summary)
    write_csv(output/'coverage_by_sector.csv', sector_rows)
    gap_columns=['ticker','missing_rebalances','first_missing','last_missing']
    write_csv(output/'price_gaps.csv', [dict(zip(gap_columns,r)) for r in gaps],gap_columns)
    meta = dict(generated_at=datetime.now(timezone.utc).isoformat(), warehouse=str(Path(warehouse).resolve()),
                policy='strict_ppe_us_gaap_research_signal_v1',
                age_limits_days={'annual_flows':annual_age,'instant_shares':shares_age,'quarterly_income':quarter_age},
                build_input_age_limits_days=build_limits,
                checks=checks,latest=summary_by_date[latest_date],latest_factor_reasons=reasons,
                full_index_backtest_ready=False,sector_point_in_time=False,execution_returns_validated=False,
                limitations=['Membership is observed, not verified effective history.',
                             'Current sector metadata is not historical point-in-time.',
                             'Unresolved/delisted prices prevent a complete index sample.',
                             'IFRS, broader capital investment, allocated SBC and tax-inclusive revenue are not silently mapped.',
                             'Research signal eligibility uses only as-of inputs; no future-return availability filter.'])
    manifest_tmp=output/'assessment.json.tmp'
    manifest_tmp.write_text(json.dumps(meta, indent=2, default=str),encoding='utf-8')
    manifest_tmp.replace(output/'assessment.json')
    publish_snapshot(output, meta, records, sql, gold_build)
    print(json.dumps({'latest':meta['latest'],'checks':checks,'reasons':reasons},default=str,indent=2))
    return meta


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--warehouse',default='data/warehouse.duckdb')
    parser.add_argument('--output',default='data/reports/backtest_quality')
    parser.add_argument('--baseline')
    parser.add_argument('--annual-age',type=int)
    parser.add_argument('--shares-age',type=int)
    parser.add_argument('--quarter-age',type=int)
    args=parser.parse_args()
    audit(args.warehouse,args.output,args.baseline,args.annual_age,args.shares_age,args.quarter_age)
