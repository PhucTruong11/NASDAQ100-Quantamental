"""Local research dashboard: streamlit run app/streamlit_app.py."""
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.data import (PILLARS, SCORE_COLUMNS, STATUS_LABELS, SnapshotError,
                      coverage_by_period, coverage_by_sector, csv_download, decode_snapshot,
                      exclusion_summary, filter_sectors, screener)
from app.analytics import rank_movement, ticker_history as aligned_ticker_history
from app.explorer import render_explorer
from app.factor_lab import render_factor_lab
from app.ui import COLORS, PILLAR_COLORS, apply_style, chips, plot, section

TEAL, NAVY, AMBER, MUTED = COLORS['teal'], COLORS['blue'], COLORS['amber'], COLORS['neutral']


@st.cache_data(max_entries=3, show_spinner=False)
def load_snapshot(content):
    # Cache on exact bytes, never only on a mutable "latest" path.
    return decode_snapshot(content)


def chart(figure, key, height=300):
    plot(figure, key, height)


def score_config():
    return {field: st.column_config.NumberColumn(PILLARS.get(field, 'Composite'), format='%.3f')
            for field in SCORE_COLUMNS}


def overview(frame, selected, period, all_periods, previous=None):
    history = frame[frame['rebalance_date'] <= period]
    periods = coverage_by_period(history)
    total = len(selected)
    eligible = int(selected['is_research_signal_eligible'].sum())
    percentage = 100 * eligible / total if total else None
    columns = st.columns(4)
    previous_total = len(previous) if previous is not None else None
    previous_eligible = int(previous['is_research_signal_eligible'].sum()) if previous is not None else None
    previous_pct = 100 * previous_eligible / previous_total if previous_total else None
    columns[0].metric('Mã trong universe', f'{total:,}', border=True,
                      delta=f'{total-previous_total:+d} mã so với kỳ trước' if previous_total is not None else None,
                      delta_color='off')
    columns[1].metric('Đủ điều kiện nghiên cứu', f'{eligible:,}', border=True,
                      delta=f'{eligible-previous_eligible:+d} mã so với kỳ trước' if previous_eligible is not None else None)
    columns[2].metric('Coverage', f'{percentage:.2f}%' if percentage is not None else '—',
                      help='Số mã đủ điều kiện nghiên cứu / toàn bộ mã thuộc phạm vi đang chọn.', border=True,
                      delta=f'{percentage-previous_pct:+.2f} điểm %' if percentage is not None and previous_pct is not None else None)
    columns[3].metric('Chưa đủ điều kiện', f'{total - eligible:,}', border=True,
                      delta=f'{total-eligible-previous_total+previous_eligible:+d} mã so với kỳ trước' if previous_total is not None else None,
                      delta_color='inverse')
    st.caption('Đủ điều kiện = đủ 4 pillar, CIK, sector, giá signal dương và input đạt policy tuổi dữ liệu. '
               'Các mã thiếu đầu vào vẫn nằm trong mẫu số.')
    left, right = st.columns([1.7, 1])
    with left:
        section('Coverage theo kỳ', 'Tỷ lệ universe có đủ signal nghiên cứu tại từng thời điểm.')
        # Missing sector-period observations remain gaps rather than zero or a connected line.
        timeline = periods.set_index('rebalance_date').reindex([d for d in all_periods if d <= period])
        figure = go.Figure(go.Scatter(
            x=timeline.index, y=timeline['coverage_pct'], mode='lines+markers', connectgaps=False,
            line=dict(color=TEAL, width=3), marker=dict(size=5), fill='tozeroy', fillcolor='rgba(93,228,199,.07)',
            customdata=timeline[['eligible_count', 'universe_count']].to_numpy(),
            hovertemplate='%{x|%d/%m/%Y}<br>Coverage: %{y:.2f}%<br>Đủ: %{customdata[0]:.0f} / %{customdata[1]:.0f}<extra></extra>',
        ))
        figure.update_yaxes(range=[0, 100], ticksuffix='%')
        figure.update_xaxes(tickformat='%m/%Y', nticks=6)
        chart(figure, 'coverage_history', 335)
    with right:
        section('Đầu vào theo pillar', 'Số mã có điểm / universe đang chọn.')
        counts = [int(selected[column].notna().sum()) for column in PILLARS]
        figure = go.Figure(go.Bar(
            y=list(PILLARS.values()), x=counts, orientation='h',
            marker_color=list(PILLAR_COLORS.values()),
            text=[f'{count} / {total}' for count in counts], textposition='auto',
            hovertemplate='%{y}<br>Đủ điểm: %{x} mã<extra></extra>',
        ))
        figure.update_xaxes(range=[0, max(total, 1)], title='Số mã có điểm')
        figure.update_yaxes(autorange='reversed', showgrid=False)
        chart(figure, 'pillar_coverage', 335)
    if selected.empty:
        st.info('Không có mã thuộc sector đã chọn tại kỳ này. Chọn sector khác để xem chi tiết.')
        return
    left, right = st.columns([1.45, 1])
    with left:
        section('Coverage giữa các ngành', 'Giữ cả mã có điểm và mã thiếu đầu vào trong từng ngành.')
        groups = coverage_by_sector(selected)
        figure = go.Figure()
        for field, label, color in [('eligible_count', 'Đủ điều kiện', TEAL),
                                     ('excluded_count', 'Chưa đủ điều kiện', MUTED)]:
            figure.add_bar(y=groups['sector_label'].str.title(), x=groups[field], name=label,
                           orientation='h', marker_color=color,
                           customdata=groups[['universe_count', 'coverage_pct']].to_numpy(),
                           hovertemplate='%{y}<br>%{x} mã<br>Universe: %{customdata[0]:.0f}<br>Coverage: %{customdata[1]:.2f}%<extra>%{fullData.name}</extra>')
        figure.update_layout(barmode='stack')
        figure.update_xaxes(rangemode='tozero', title='Số mã')
        figure.update_yaxes(autorange='reversed', showgrid=False)
        chart(figure, 'sector_coverage', height=390)
    with right:
        section('Phân bố composite', 'Điểm hiện tại, từ −3 đến +3.')
        scores = selected['composite_score'].dropna()
        if scores.empty:
            st.info('Phạm vi này chưa có composite hợp lệ.')
        else:
            figure = go.Figure(go.Histogram(x=scores, xbins=dict(start=-3, end=3, size=0.3),
                                           marker_color=TEAL,
                                           hovertemplate='Composite: %{x}<br>%{y} mã<extra></extra>'))
            figure.update_xaxes(range=[-3, 3], title='Composite score')
            figure.update_yaxes(rangemode='tozero', title='Số mã')
            chart(figure, 'composite_distribution', height=390)
        st.caption('Điểm cao hơn nghĩa là xếp hạng factor cao hơn trong mô hình hiện tại. '
                   'Phân bố chỉ gồm mã có composite; chưa đo lợi nhuận dự báo.')
    leaders = selected[selected['is_research_signal_eligible']].sort_values(
        ['composite_score', 'ticker'], ascending=[False, True]).head(8)
    if leaders['composite_score'].nunique() > 1:
        section('Các signal có composite cao nhất', 'Tối đa 8 mã đủ điều kiện trong kỳ và ngành đang chọn; đây là thứ hạng điểm factor.')
        figure = go.Figure(go.Bar(x=leaders['ticker'], y=leaders['composite_score'],
                                 marker_color=[TEAL if value >= 0 else COLORS['rose'] for value in leaders['composite_score']],
                                 text=[f'{value:+.3f}' for value in leaders['composite_score']], textposition='outside',
                                 customdata=leaders[['sector_label']].to_numpy(),
                                 hovertemplate='%{x}<br>Composite: %{y:+.3f}<br>%{customdata[0]}<extra></extra>'))
        figure.update_yaxes(range=[-3.2, 3.2], title='Composite', zeroline=True)
        chart(figure, 'leading_signals', 300)


def open_ticker(ticker):
    st.session_state['view'] = 'Hồ sơ mã'
    st.session_state['explorer_ticker'] = ticker


def show_screener(full, period, sectors):
    selected = rank_movement(full, period, sectors)
    section('Screener điểm số', 'Xếp hạng, biến động hạng và bốn pillar của từng mã trong universe đang chọn.')
    left, right = st.columns([1, 1])
    with left:
        eligibility = st.selectbox('Điều kiện', ['Tất cả', 'Đủ điều kiện', 'Chưa đủ điều kiện'], key='eligibility')
    with right:
        query = st.text_input('Tìm ticker', placeholder='Ví dụ: AAPL', key='ticker_search')
    rows = screener(selected, eligibility, query)
    st.caption(f'{len(rows):,} / {len(selected):,} mã sau bộ lọc. Điểm NULL được giữ là ô trống; '
               'lọc sector không tính lại z-score hoặc trọng số.')
    if rows.empty:
        st.info('Không có mã khớp bộ lọc hiện tại.')
    else:
        columns = ['rank', 'ticker', 'sector_label', 'composite_score', 'rank_change', *PILLARS,
                   'available_pillar_count', 'is_research_signal_eligible']
        event = st.dataframe(rows[columns], hide_index=True, width='stretch', height=530,
                     key='screener_table', on_select='rerun', selection_mode='single-row',
                     column_config={**score_config(), 'ticker': 'Ticker', 'sector_label': 'Sector',
                                    'rank': st.column_config.NumberColumn('Hạng', format='%d'),
                                    'rank_change': st.column_config.NumberColumn('Δ hạng', format='%+d',
                                        help='Hạng kỳ trước − hạng hiện tại; dương là tăng hạng. Trống khi không có hạng ở kỳ liền trước.'),
                                    'available_pillar_count': 'Số pillar',
                                    'is_research_signal_eligible': 'Đủ điều kiện'})
        if event.selection.rows and event.selection.rows[0] < len(rows):
            ticker = rows.iloc[event.selection.rows[0]]['ticker']
            st.button(f'Mở hồ sơ {ticker} →', on_click=open_ticker, args=(ticker,), key='open_screener_ticker')
        st.caption('Chọn một dòng để mở hồ sơ. Hạng tính trên toàn bộ mã có composite thuộc kỳ/ngành đã chọn, '
                   'trước bộ lọc tìm kiếm và điều kiện. Thay đổi thành phần mẫu có thể làm đổi hạng.')
    st.download_button('Tải CSV đang lọc', csv_download(rows), 'gold_screener.csv', 'text/csv', key='screener_csv')


def quality_view(frame, selected, assessment, periods=None):
    st.subheader('Vì sao chưa đủ dữ liệu?')
    st.caption('Mỗi mã có một trạng thái ưu tiên cho từng input. Các lý do giữa FCF, SBC, Growth '
               'và Quality có thể chồng lấp; không cộng chúng thành số mã bị loại.')
    if selected.empty:
        st.info('Không có dữ liệu tại kỳ và sector đã chọn.')
        return
    group = st.selectbox('Nhóm đầu vào', ['FCF', 'SBC', 'Growth', 'Momentum', 'Quality proxy'], key='diagnostic_factor')
    field = {'FCF': 'fcf_status', 'SBC': 'sbc_status', 'Growth': 'growth_status',
             'Momentum': 'momentum_status', 'Quality proxy': 'quality_status'}[group]
    counts = selected.loc[selected[field] != 'available', field].value_counts().sort_values()
    if counts.empty:
        st.success(f'{group}: tất cả mã đang chọn có đầu vào hợp lệ.')
    else:
        figure = go.Figure(go.Bar(y=[STATUS_LABELS.get(code, code) for code in counts.index],
                                 x=counts.values, orientation='h', marker_color=AMBER,
                                 text=counts.values, textposition='auto',
                                 hovertemplate='%{y}<br>%{x} mã<extra></extra>'))
        figure.update_xaxes(rangemode='tozero', dtick=1, title='Số mã')
        figure.update_yaxes(showgrid=False)
        chart(figure, 'input_reasons', height=max(300, 44 * len(counts)))
    st.markdown('**Phần giao giữa FCF và SBC**')
    groups = exclusion_summary(selected).set_index('group')['count']
    figure = go.Figure()
    for label, color in [('Có Value', TEAL), ('Thiếu FCF', NAVY),
                         ('Thiếu SBC', AMBER), ('Thiếu cả FCF và SBC', MUTED)]:
        count = int(groups.get(label, 0))
        figure.add_bar(y=['Universe'], x=[count], name=label, orientation='h', marker_color=color,
                       text=[count if count else ''], textposition='auto',
                       hovertemplate='%{x} mã<extra>%{fullData.name}</extra>')
    figure.update_layout(barmode='stack')
    figure.update_xaxes(range=[0, len(selected)])
    figure.update_yaxes(showgrid=False, showticklabels=False)
    chart(figure, 'value_intersection', height=240)
    st.caption('Bốn nhóm loại trừ nhau và cộng lại bằng universe đang chọn.')
    st.divider()
    ticker = st.selectbox('Kiểm tra một mã', sorted(selected['ticker'].unique()), key='detail_ticker')
    row = selected[selected['ticker'] == ticker].iloc[0]
    st.markdown(f'**{ticker} · {row["sector_label"].title()}**')
    statuses = pd.DataFrame([
        {'Input': label, 'Trạng thái': STATUS_LABELS.get(row[field], row[field]), 'Mã chẩn đoán': row[field]}
        for label, field in [('FCF', 'fcf_status'), ('SBC', 'sbc_status'),
                             ('Growth', 'growth_status'), ('Momentum', 'momentum_status'),
                             ('Quality proxy', 'quality_status')]])
    st.dataframe(statuses, hide_index=True, width='stretch')
    limits = assessment['age_limits_days']
    ages = pd.DataFrame([
        {'Input': label, 'Tuổi dữ liệu (ngày)': row[column], 'Giới hạn (ngày)': limit}
        for label, column, limit in [
            ('FCF năm', 'fcf_age_days', limits['annual_flows']),
            ('SBC năm', 'sbc_age_days', limits['annual_flows']),
            ('Growth năm', 'growth_age_days', limits['annual_flows']),
            ('Shares instant', 'shares_age_days', limits['instant_shares']),
            ('Lợi nhuận quý hiện tại', 'quarter_age_days', limits['quarterly_income'])]])
    st.dataframe(ages, hide_index=True, width='stretch')
    st.caption('Tuổi tính từ ngày kết thúc kỳ đo lường tới ngày rebalance; ô trống nghĩa là '
               'không chọn được input hợp lệ. Kỳ so sánh năm trước được phép cũ hơn.')
    ticker_history = aligned_ticker_history(frame, ticker, periods)
    figure = go.Figure()
    for column, label, color in [('composite_score', 'Composite', COLORS['text']),
                                 ('value_score', 'Value', PILLAR_COLORS['value_score'])]:
        figure.add_scatter(x=ticker_history['rebalance_date'], y=ticker_history[column],
                           name=label, mode='lines+markers', connectgaps=False, line=dict(color=color),
                           hovertemplate='%{x|%d/%m/%Y}<br>%{y:.3f}<extra>%{fullData.name}</extra>')
    figure.update_yaxes(range=[-3, 3], title='Điểm')
    figure.update_xaxes(tickformat='%m/%Y', nticks=7)
    chart(figure, 'ticker_history')
    st.caption(f'CIK: {row["cik"] or "—"} · Identifier: {row["identifier_quality"]} · '
               f'Membership: {row["membership_quality"]} · Shares tag: {row["shares_source_tag"] or "—"}')
    if row['cik']:
        st.link_button('Mở hồ sơ SEC', f'https://www.sec.gov/edgar/browse/?CIK={row["cik"]}&owner=exclude')


def source_details(payload):
    assessment = payload['assessment']
    with st.expander('Định nghĩa và nguồn dữ liệu'):
        st.markdown('''- **Value:** 50% FCF yield z-score + 50% z-score SBC đã đảo dấu; cần đủ cả hai.
- **Growth:** z-score của tăng trưởng doanh thu năm YoY + biên lợi nhuận ròng.
- **Momentum:** z-score của giá adjusted cuối tháng t−1 / giá adjusted cuối tháng t−12 − 1; bỏ tháng gần nhất.
- **Quality proxy:** z-score tăng trưởng Net Income quý YoY, chia cho trị tuyệt đối lợi nhuận quý cùng kỳ năm trước.
- **Composite:** 25% mỗi pillar; chỉ có điểm khi đủ cả bốn.
- Z-score tính theo ngành khi đủ mẫu, fallback toàn universe khi cần; kẹp trong [-3, 3].''')
        st.caption('Snapshot giữ nguyên điểm đã tính trong dbt. Bộ lọc chỉ thay đổi phạm vi hiển thị.')
        st.json({'snapshot_id': payload['snapshot_id'], 'generated_at': assessment['generated_at'],
                 'policy': assessment['policy'], 'age_limits_days': assessment['age_limits_days'],
                 'gold_build': payload['source']['gold_build'], 'checks': assessment['checks']}, expanded=False)
        st.code(payload['source']['sql'], language='sql')


def main():
    st.set_page_config(page_title='NASDAQ-100 · Research desk', page_icon='📊', layout='wide')
    apply_style()
    st.title('NASDAQ-100 · Research desk')
    snapshot = Path(os.environ.get('QUANT_DASHBOARD_SNAPSHOT',
                                   ROOT / 'data/reports/backtest_quality/dashboard_snapshot.json'))
    try:
        payload, full = load_snapshot(snapshot.read_bytes())
    except FileNotFoundError:
        st.info('Chưa có snapshot đã audit. Chạy pipeline tới bước audit_backtest_inputs để tạo dữ liệu.')
        st.code('python -m research.backtest_quality', language='bash')
        st.stop()
    except (OSError, SnapshotError) as exc:
        st.error(f'Không đọc được snapshot: {exc}')
        st.stop()
    assessment = payload['assessment']
    generated = datetime.fromisoformat(assessment['generated_at'])
    chips([(f'Dữ liệu đến {full["rebalance_date"].max():%d/%m/%Y}', 'accent'),
           (f'{full["rebalance_date"].nunique()} kỳ quan sát', ''),
           ('Research mode · chưa có backtest', 'amber')])
    if (datetime.now(timezone.utc) - generated).total_seconds() > 36 * 3600:
        st.warning('Snapshot đã quá 36 giờ. Đây là bản audit gần nhất app đang đọc; chưa xác minh trạng thái lần chạy pipeline mới hơn.')
    with st.sidebar:
        st.html('<div class="desk-brand"><b>NASDAQ<span>100</span></b><small>QUANTAMENTAL RESEARCH</small></div>')
        view = st.radio('Màn hình', ['Tổng quan', 'Screener', 'Phân tích factor', 'Hồ sơ mã',
                                    'Chất lượng dữ liệu', 'Backtest'], key='view')
        st.divider()
        st.header('Phạm vi dữ liệu')
        dates = sorted(full['rebalance_date'].unique(), reverse=True)
        period = st.selectbox('Kỳ rebalance', dates, format_func=lambda value: pd.Timestamp(value).strftime('%d/%m/%Y'), key='period')
        sectors = st.multiselect('Sector', sorted(full['sector_label'].unique()),
                                 placeholder='Tất cả ngành', help='Để trống để chọn toàn bộ ngành, bao gồm mã thiếu sector.', key='sectors')
        window = st.selectbox('Lịch sử hiển thị', ['12 kỳ', '24 kỳ', 'Toàn bộ'], index=2, key='history_window',
                              help='Chỉ giới hạn các biểu đồ lịch sử. Không đổi điểm hoặc kỳ so sánh liền trước.')
        if st.button('Đọc snapshot mới', width='stretch'):
            st.rerun()
        st.caption(f'Snapshot {payload["snapshot_id"][:10]}\n\n'
                   f'Audit {generated.astimezone(timezone(timedelta(hours=7))).strftime("%d/%m/%Y %H:%M")} UTC+7')
        st.caption('Membership quan sát theo tháng. Sector dùng metadata hiện tại. '
                   'Bộ lọc giữ nguyên điểm từ Gold.')
    frame = filter_sectors(full, sectors)
    selected = frame[frame['rebalance_date'] == period]
    past_dates = sorted(date for date in dates if date <= period)
    earlier_dates = sorted(date for date in dates if date < period)
    previous = frame[frame['rebalance_date'].eq(earlier_dates[-1])] if earlier_dates else None
    chart_dates = past_dates if window == 'Toàn bộ' else past_dates[-int(window.split()[0]):]
    history = frame[frame['rebalance_date'].isin(chart_dates)]
    chips([(view, 'accent'), (f'Kỳ {pd.Timestamp(period):%d/%m/%Y}', ''),
           ('Tất cả ngành' if not sectors else ', '.join(sectors), '')])
    if view == 'Tổng quan':
        overview(history, selected, period, chart_dates, previous)
    elif view == 'Screener':
        show_screener(full, period, sectors)
    elif view == 'Phân tích factor':
        render_factor_lab(history, selected, chart_dates)
    elif view == 'Hồ sơ mã':
        render_explorer(history, selected, period, chart_dates)
    elif view == 'Chất lượng dữ liệu':
        quality_view(history, selected, assessment, chart_dates)
    else:
        section('Backtest workspace', 'Khu vực nối kết quả của engine sau khi kiểm chứng methodology và execution data.')
        st.info('Chưa có output backtest đã kiểm chứng. Trang này chưa hiển thị return, Sharpe, drawdown hoặc equity curve.')
        steps = st.columns(3)
        for target, number, title, description in [
            (steps[0], '01 · DATASET', 'Cố định đầu vào', 'Chụp dữ liệu và config; giữ coverage, lý do loại mã và universe mode.'),
            (steps[1], '02 · EXECUTION', 'Mô phỏng giao dịch', 'Chốt signal → execution, giá exit/delisting, holding period và chi phí.'),
            (steps[2], '03 · VALIDATION', 'Đối chiếu kết quả', 'Kiểm chứng NAV, turnover, drawdown và benchmark trước khi nối cùng run vào app.')]:
            with target:
                st.html(f'<div class="desk-step"><small>{number}</small><h3>{title}</h3><p>{description}</p></div>')
        st.caption('Chọn sector tại dashboard chỉ lọc dữ liệu hiển thị. Mỗi chiến lược hoặc phạm vi backtest khác cần một run riêng.')
    st.divider()
    source_details(payload)
    st.html('<div class="desk-foot">NASDAQ-100 Quantamental · Gold → audited snapshot → research desk · '
            'Điểm factor là tín hiệu nghiên cứu; khả năng dự báo lợi nhuận chưa được kiểm chứng.</div>')


if __name__ == '__main__':
    main()
