"""Publish one atomic, self-contained serving snapshot after a successful input audit."""
import hashlib
import json
from pathlib import Path


def publish_snapshot(output, assessment, records, sql, gold_build=None):
    # Serving needs no local paths, credentials, live database or raw SEC/Yahoo payloads.
    public_assessment = {k: v for k, v in assessment.items() if k != 'warehouse'}
    payload = {
        'schema_version': 1,
        'assessment': public_assessment,
        'records': records,
        'source': {
            'kind': 'local_derived_research_snapshot',
            'grain': ['rebalance_date', 'ticker'],
            'tables': ['gold_compose_score', 'int_fcf_yield', 'int_sbc_ratio',
                       'int_growth_ratio', 'int_earnings_surprise', 'silver_price_adjusted'],
            'sql': sql,
            'gold_build': gold_build or {},
            'refresh_command': 'python -m research.backtest_quality',
        },
    }
    encode = lambda value: json.dumps(value, ensure_ascii=False, sort_keys=True,
                                      default=str, allow_nan=False).encode('utf-8')
    payload['snapshot_id'] = hashlib.sha256(encode(payload)).hexdigest()[:20]
    content = encode(payload)
    output = Path(output)
    archive = output / 'snapshots'
    archive.mkdir(parents=True, exist_ok=True)
    for path in (archive / (payload['snapshot_id'] + '.json'), output / 'dashboard_snapshot.json'):
        temporary = path.with_suffix('.json.tmp')
        temporary.write_bytes(content)
        temporary.replace(path)
    return payload['snapshot_id']
