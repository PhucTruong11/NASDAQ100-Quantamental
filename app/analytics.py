"""Pure snapshot analytics; scores describe signals, never realized returns.

Inputs use the reviewed ticker × rebalance_date grain from ``app.data``.
Coverage denominators include every supplied membership row, including rows
with no scores. Callers apply a sector filter before coverage/correlation if
desired; rank_movement applies one sector selection consistently to both dates.
"""
import numpy as np
import pandas as pd

from app.data import MISSING_SECTOR, PILLARS, SCORE_COLUMNS, filter_sectors


SCORE_LABELS = {**PILLARS, 'composite_score': 'Composite'}


def _finite(series):
    """Keep zero, mask NULL and non-finite values, never impute scores."""
    values = pd.to_numeric(series, errors='coerce').astype(float)
    return values.where(np.isfinite(values))


def pillar_coverage_by_period(frame):
    """Long coverage table for four pillars plus composite, with denominators."""
    columns = ['rebalance_date', 'pillar', 'pillar_label', 'available_count',
               'universe_count', 'coverage_pct']
    if frame.empty:
        return pd.DataFrame(columns=columns)
    parts = []
    for date, group in frame.groupby('rebalance_date', sort=True):
        denominator = len(group)
        for column, label in SCORE_LABELS.items():
            count = int(_finite(group[column]).notna().sum())
            parts.append(dict(rebalance_date=date, pillar=column,
                              pillar_label=label, available_count=count,
                              universe_count=denominator,
                              coverage_pct=100 * count / denominator))
    return pd.DataFrame(parts, columns=columns)


def sector_pillar_summary(frame):
    """Available-score medians and coverage for every sector/pillar pair.

    Supply a single period for a cross-sectional comparison. With multiple
    periods the denominator is explicitly ticker-period rows, not unique firms.
    A missing score is excluded from the median but retained in its denominator.
    """
    columns = ['sector_label', 'pillar', 'pillar_label', 'median_score',
               'available_count', 'universe_count', 'coverage_pct']
    if frame.empty:
        return pd.DataFrame(columns=columns)
    work = frame.copy()
    if 'sector_label' not in work:
        work['sector_label'] = work['sector'].fillna(MISSING_SECTOR)
    work['sector_label'] = work['sector_label'].fillna(MISSING_SECTOR)
    parts = []
    for sector, group in work.groupby('sector_label', sort=True):
        denominator = len(group)
        for column, label in SCORE_LABELS.items():
            scores = _finite(group[column]).dropna()
            count = len(scores)
            parts.append(dict(sector_label=sector, pillar=column,
                              pillar_label=label,
                              median_score=scores.median() if count else np.nan,
                              available_count=count, universe_count=denominator,
                              coverage_pct=100 * count / denominator))
    return pd.DataFrame(parts, columns=columns)


def score_correlations(frame, score_columns=None):
    """Return (Spearman correlations, pairwise finite sample counts).

    Average ranks handle ties. Each pair uses only its jointly observed rows;
    fewer than three rows or either constant input yields NaN, even on the
    diagonal. Counts remain visible when a correlation cannot be estimated.
    This is descriptive signal association, not predictive performance.
    """
    columns = list(SCORE_COLUMNS if score_columns is None else score_columns)
    correlation = pd.DataFrame(np.nan, index=columns, columns=columns)
    sample_count = pd.DataFrame(0, index=columns, columns=columns, dtype=int)
    numeric = pd.DataFrame({column: _finite(frame[column]) for column in columns})
    for i, left in enumerate(columns):
        for right in columns[i:]:
            paired = numeric[[left]].rename(columns={left: 'left'})
            paired['right'] = numeric[right]
            paired = paired.dropna()
            count = len(paired)
            sample_count.loc[left, right] = sample_count.loc[right, left] = count
            if (count >= 3 and paired['left'].nunique() > 1
                    and paired['right'].nunique() > 1):
                ranked = paired.rank(method='average')
                value = float(ranked['left'].corr(ranked['right']))
                correlation.loc[left, right] = correlation.loc[right, left] = value
    return correlation, sample_count


def rank_movement(frame, period, sectors=None, score_column='composite_score'):
    """Rank the current snapshot and compare with the preceding reviewed date.

    Descending rank uses ``method='min'`` (1, 1, 3 for tied leaders). Positive
    rank_change means an improved rank: previous_rank - rank. Both snapshots
    use the same sector selection and independently include their then-observed
    members. No prior membership/score means a missing previous_rank/change;
    a ticker's last historical score is never carried over a missing period.
    """
    period = pd.Timestamp(period)
    dates = pd.DatetimeIndex(pd.to_datetime(frame['rebalance_date']).unique()).sort_values()
    earlier = dates[dates < period]
    previous_period = earlier[-1] if len(earlier) else pd.NaT
    work = filter_sectors(frame, sectors)
    work['rebalance_date'] = pd.to_datetime(work['rebalance_date'])
    current = work.loc[work['rebalance_date'].eq(period)].copy()
    current['rank'] = _finite(current[score_column]).rank(method='min', ascending=False)
    if pd.isna(previous_period):
        current['previous_rank'] = np.nan
    else:
        previous = work.loc[work['rebalance_date'].eq(previous_period)].copy()
        previous['previous_rank'] = _finite(previous[score_column]).rank(
            method='min', ascending=False)
        current['previous_rank'] = current['ticker'].map(
            previous.set_index('ticker')['previous_rank'])
    current['rank_change'] = current['previous_rank'] - current['rank']
    current['previous_period'] = previous_period
    return current.sort_values(['rank', 'ticker'], na_position='last')


def ticker_history(frame, ticker, periods=None):
    """Reindex one ticker onto reviewed periods without connecting data gaps.

    ``present_in_universe`` distinguishes absent membership rows from members
    with missing factor inputs. Scores and eligibility for absent rows stay
    missing. Explicit periods allow charts to retain the full reviewed axis
    after a sector filter; dates are sorted and deduplicated.
    """
    dates = frame['rebalance_date'].unique() if periods is None else periods
    index = pd.DatetimeIndex(pd.to_datetime(dates)).unique().sort_values()
    index.name = 'rebalance_date'
    work = frame.loc[frame['ticker'].eq(ticker)].copy()
    work['rebalance_date'] = pd.to_datetime(work['rebalance_date'])
    work['present_in_universe'] = True
    result = work.set_index('rebalance_date').reindex(index)
    result['present_in_universe'] = result['present_in_universe'].eq(True)
    result['ticker'] = ticker
    return result.reset_index()


def score_distribution(frame, score_column='composite_score'):
    """One inspectable summary, retaining the supplied population denominator."""
    values = _finite(frame[score_column]).dropna()
    count = len(values)
    return dict(universe_count=len(frame), available_count=count,
                missing_count=len(frame) - count,
                mean=values.mean() if count else np.nan,
                median=values.median() if count else np.nan,
                q25=values.quantile(.25) if count else np.nan,
                q75=values.quantile(.75) if count else np.nan,
                minimum=values.min() if count else np.nan,
                maximum=values.max() if count else np.nan)
