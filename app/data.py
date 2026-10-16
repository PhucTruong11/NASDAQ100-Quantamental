"""Read-only dashboard measurements. No warehouse connection or job execution."""
import json
from datetime import datetime

import numpy as np
import pandas as pd

PILLARS = {'value_score': 'Value', 'growth_score': 'Growth',
           'momentum_score': 'Momentum', 'quality_score': 'Quality proxy'}
MISSING_SECTOR = 'Chưa có sector'
STATUS_LABELS = {
    'available': 'Đủ dữ liệu',
    'missing_same_period_ppe_capex': 'Thiếu PPE Capex cùng kỳ',
    'broader_productive_assets_only': 'Chỉ có Capex phạm vi rộng',
    'missing_annual_ocf': 'Thiếu OCF năm hợp lệ',
    'stale_instant_shares': 'Shares quá cũ',
    'missing_instant_shares': 'Thiếu shares tại thời điểm',
    'missing_signal_price': 'Thiếu giá tại ngày signal',
    'continuing_operations_ocf_only': 'Chỉ có OCF continuing operations',
    'multi_class_market_cap_unsupported': 'Chưa xử lý market cap đa lớp cổ phiếu',
    'gross_revenue_including_tax_only': 'Chỉ có doanh thu bao gồm thuế',
    'missing_annual_revenue': 'Thiếu doanh thu năm hợp lệ',
    'allocated_sbc_not_equivalent': 'Allocated SBC khác định nghĩa',
    'missing_same_period_reported_sbc': 'Thiếu reported SBC cùng kỳ',
    'missing_same_period_income': 'Thiếu lợi nhuận cùng kỳ',
    'missing_supported_annual_revenue': 'Thiếu doanh thu năm được hỗ trợ',
    'missing_comparable_revenue_pair': 'Thiếu cặp doanh thu so sánh',
    'stale_or_missing_current_quarter_income': 'Lợi nhuận quý thiếu hoặc quá cũ',
    'missing_supported_current_quarter_income': 'Thiếu lợi nhuận quý được hỗ trợ',
    'missing_prior_year_quarter_income': 'Thiếu lợi nhuận quý cùng kỳ năm trước',
    'zero_prior_year_income_denominator': 'Lợi nhuận kỳ so sánh bằng 0',
    'missing_sector': 'Thiếu sector',
    'invalid_ratio_or_normalization': 'Ratio hoặc chuẩn hóa chưa hợp lệ',
    'missing_momentum_input_or_normalization': 'Thiếu đầu vào hoặc chuẩn hóa momentum 12–1',
}
SCORE_COLUMNS = [*PILLARS, 'composite_score']
AGE_COLUMNS = ['fcf_age_days', 'sbc_age_days', 'growth_age_days',
               'shares_age_days', 'quarter_age_days']
REQUIRED = {'rebalance_date', 'ticker', 'cik', 'sector', 'membership_quality',
            'identifier_quality', 'available_pillar_count', 'is_score_eligible',
            'is_research_signal_eligible', 'signal_price', 'shares_source_tag',
            'fcf_status', 'sbc_status', 'growth_status', 'quality_status',
            *SCORE_COLUMNS, *AGE_COLUMNS}


class SnapshotError(ValueError):
    pass


def decode_snapshot(content):
    try:
        payload = json.loads(content)
        if payload['schema_version'] != 1:
            raise SnapshotError('Phiên bản snapshot chưa được hỗ trợ.')
        frame = pd.DataFrame(payload['records'])
        if frame.empty or not REQUIRED.issubset(frame.columns):
            raise SnapshotError('Snapshot rỗng hoặc thiếu cột bắt buộc.')
        frame['rebalance_date'] = pd.to_datetime(frame['rebalance_date'], errors='raise')
        if frame[['ticker', 'rebalance_date']].isna().any().any():
            raise SnapshotError('Snapshot có khóa ticker/kỳ bị thiếu.')
        if frame.duplicated(['ticker', 'rebalance_date']).any():
            raise SnapshotError('Snapshot bị trùng ticker/kỳ.')
        for field in ['is_score_eligible', 'is_research_signal_eligible']:
            if not frame[field].map(lambda value: isinstance(value, bool)).all():
                raise SnapshotError('Eligibility phải là Boolean, không được tự điền giá trị thiếu.')
        for field in SCORE_COLUMNS + AGE_COLUMNS + ['signal_price']:
            frame[field] = pd.to_numeric(frame[field], errors='raise')
            if np.isinf(frame[field].to_numpy(dtype=float)).any():
                raise SnapshotError('Snapshot có giá trị vô hạn.')
        complete = frame[list(PILLARS)].notna().all(axis=1)
        if not (complete == frame['composite_score'].notna()).all():
            raise SnapshotError('Composite không khớp coverage bốn pillar.')
        if not (complete == frame['is_score_eligible']).all():
            raise SnapshotError('Eligibility của Gold không khớp pillar.')
        if (frame['is_research_signal_eligible'] & ~complete).any():
            raise SnapshotError('Có signal được chọn dù chưa đủ pillar.')
        if not np.allclose(frame.loc[complete, list(PILLARS)].mean(axis=1),
                           frame.loc[complete, 'composite_score'], atol=1e-10):
            raise SnapshotError('Công thức composite không khớp trọng số 25%.')
        assessment = payload['assessment']
        if not datetime.fromisoformat(assessment['generated_at']).tzinfo:
            raise SnapshotError('Thời điểm snapshot phải có timezone.')
        if not payload['snapshot_id'] or not isinstance(payload['source']['sql'], str):
            raise SnapshotError('Snapshot thiếu định danh hoặc nguồn SQL.')
        limits = assessment['age_limits_days']
        policy_eligible = complete & frame['cik'].notna() & frame['sector'].notna() & (frame['signal_price'] > 0)
        for column, limit in [('fcf_age_days', limits['annual_flows']),
                              ('sbc_age_days', limits['annual_flows']),
                              ('growth_age_days', limits['annual_flows']),
                              ('shares_age_days', limits['instant_shares']),
                              ('quarter_age_days', limits['quarterly_income'])]:
            policy_eligible &= frame[column].between(0, limit)
        if not (policy_eligible == frame['is_research_signal_eligible']).all():
            raise SnapshotError('Signal không khớp policy tuổi dữ liệu trong manifest.')
        if len(frame) != assessment['checks']['rows']:
            raise SnapshotError('Tổng số dòng khác manifest.')
        last = frame[frame['rebalance_date'] == frame['rebalance_date'].max()]
        latest = assessment['latest']
        if (last['rebalance_date'].iloc[0].date().isoformat() != latest['rebalance_date']
                or len(last) != latest['universe_count']
                or int(last['is_research_signal_eligible'].sum()) != latest['research_signal_count']
                or int(last['composite_score'].notna().sum()) != latest['composite_count']):
            raise SnapshotError('Kỳ mới nhất khác manifest; cần xuất lại snapshot.')
        frame['sector_label'] = frame['sector'].fillna(MISSING_SECTOR)
        frame['momentum_status'] = np.where(frame['momentum_score'].notna(), 'available',
                                            'missing_momentum_input_or_normalization')
        return payload, frame.sort_values(['rebalance_date', 'ticker'])
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, SnapshotError):
            raise
        raise SnapshotError('Snapshot không hợp lệ hoặc không đúng schema.') from exc


def filter_sectors(frame, sectors):
    # An empty selection means all sectors, including rows with missing sector.
    return frame[frame['sector_label'].isin(sectors)].copy() if sectors else frame.copy()


def coverage_by_period(frame):
    result = frame.groupby('rebalance_date', sort=True).agg(
        universe_count=('ticker', 'size'),
        eligible_count=('is_research_signal_eligible', 'sum'),
        composite_count=('composite_score', 'count'),
    ).reset_index()
    result['coverage_pct'] = 100 * result['eligible_count'] / result['universe_count']
    result['excluded_count'] = result['universe_count'] - result['eligible_count']
    return result


def coverage_by_sector(frame):
    result = frame.groupby('sector_label', sort=True).agg(
        universe_count=('ticker', 'size'), eligible_count=('is_research_signal_eligible', 'sum'),
    ).reset_index()
    result['excluded_count'] = result['universe_count'] - result['eligible_count']
    result['coverage_pct'] = 100 * result['eligible_count'] / result['universe_count']
    return result.sort_values(['coverage_pct', 'sector_label'], ascending=[False, True])


def exclusion_summary(frame):
    """Mutually exclusive Value shortfalls, keeping the original universe denominator."""
    fcf = frame['fcf_status'].eq('available')
    sbc = frame['sbc_status'].eq('available')
    groups = pd.Series('Có Value', index=frame.index)
    groups.loc[~fcf & sbc] = 'Thiếu FCF'
    groups.loc[fcf & ~sbc] = 'Thiếu SBC'
    groups.loc[~fcf & ~sbc] = 'Thiếu cả FCF và SBC'
    return groups.value_counts().rename_axis('group').reset_index(name='count')


def screener(frame, eligibility='Tất cả', search=''):
    result = frame.copy()
    if eligibility == 'Đủ điều kiện':
        result = result[result['is_research_signal_eligible']]
    elif eligibility == 'Chưa đủ điều kiện':
        result = result[~result['is_research_signal_eligible']]
    if search.strip():
        result = result[result['ticker'].str.contains(search.strip().upper(), regex=False)]
    return result.sort_values(['composite_score', 'ticker'], ascending=[False, True], na_position='last')


def csv_download(frame):
    # Export exactly the inspected population; NULL remains empty and zero remains zero.
    columns = ['rebalance_date', 'ticker', 'sector', *SCORE_COLUMNS,
               'available_pillar_count', 'is_research_signal_eligible',
               'fcf_status', 'sbc_status', 'growth_status', 'momentum_status', 'quality_status', *AGE_COLUMNS,
               'membership_quality', 'identifier_quality']
    columns += [column for column in ['rank', 'previous_rank', 'rank_change'] if column in frame.columns]
    return frame[columns].to_csv(index=False, date_format='%Y-%m-%d').encode('utf-8-sig')
