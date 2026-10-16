"""Ticker dossiers and comparisons built only from the audited snapshot."""
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.data import AGE_COLUMNS, PILLARS, SCORE_COLUMNS, STATUS_LABELS
from app.analytics import ticker_history
from app.ui import COLORS, PILLAR_COLORS, plot, section


SCORE_LABELS = {**PILLARS, 'composite_score': 'Composite'}
COMPARISON_COLORS = ['#5DE4C7', '#77A8FF', '#F1BE68', '#EA88B1']


def _number(value):
    return f'{value:+.3f}' if pd.notna(value) else '—'


def _text(value):
    return str(value) if pd.notna(value) and str(value) else '—'


def _download(frame):
    # Export exactly the table values, preserving zero and leaving NULL blank.
    return frame.to_csv(index=False, date_format='%Y-%m-%d').encode('utf-8-sig')


def _score_config():
    return {column: st.column_config.NumberColumn(label, format='%.3f')
            for column, label in SCORE_LABELS.items()}


def _aligned_history(history, ticker, dates):
    """An absent ticker-period must break a line just like a missing score."""
    return ticker_history(history, ticker, dates).set_index('rebalance_date')


def _history_figure(history, ticker, dates):
    rows = _aligned_history(history, ticker, dates)
    figure = go.Figure()
    for column, label in SCORE_LABELS.items():
        composite = column == 'composite_score'
        figure.add_scatter(
            x=rows.index, y=rows[column], name=label,
            mode='lines+markers', connectgaps=False,
            line=dict(color=COLORS['text'] if composite else PILLAR_COLORS[column],
                      width=3.5 if composite else 1.8,
                      dash='solid' if composite else 'dot'),
            marker=dict(size=5 if composite else 3),
            hovertemplate='%{x|%d/%m/%Y}<br>Điểm: %{y:+.3f}<extra>%{fullData.name}</extra>',
        )
    figure.update_xaxes(title=None, tickformat='%m/%Y', nticks=8)
    figure.update_yaxes(title='Điểm factor', range=[-3.15, 3.15], zeroline=True)
    figure.update_layout(hovermode='x unified')
    return figure


def _pillar_figure(row):
    figure = go.Figure(go.Bar(
        x=[row[column] for column in PILLARS], y=list(PILLARS.values()),
        orientation='h', marker_color=[PILLAR_COLORS[column] for column in PILLARS],
        text=[_number(row[column]) for column in PILLARS], textposition='outside',
        cliponaxis=False,
        hovertemplate='%{y}<br>Điểm: %{x:+.3f}<extra></extra>',
    ))
    figure.update_xaxes(range=[-3.5, 3.5], title='Điểm chuẩn hóa', zeroline=True)
    figure.update_yaxes(autorange='reversed', showgrid=False)
    figure.update_layout(showlegend=False)
    return figure


def _provenance(row):
    with st.expander('Nguồn và điều kiện đầu vào', expanded=False):
        left, right = st.columns([1.1, 1])
        with left:
            statuses = pd.DataFrame([
                {'Đầu vào': label, 'Trạng thái': STATUS_LABELS.get(row[field], row[field]),
                 'Tuổi (ngày)': row[age] if age else None}
                for label, field, age in [
                    ('FCF năm', 'fcf_status', 'fcf_age_days'),
                    ('SBC năm', 'sbc_status', 'sbc_age_days'),
                    ('Growth năm', 'growth_status', 'growth_age_days'),
                    ('Momentum 12–1', 'momentum_status', None),
                    ('Quality proxy', 'quality_status', 'quarter_age_days'),
                ]
            ])
            st.dataframe(statuses, width='stretch', hide_index=True,
                         column_config={'Tuổi (ngày)': st.column_config.NumberColumn(format='%d')})
            st.caption('Tuổi tính từ ngày kết thúc kỳ đo lường. Ô trống giữ nguyên dữ liệu thiếu; '
                       'Momentum không dùng policy tuổi báo cáo tài chính.')
        with right:
            provenance = pd.DataFrame([
                {'Thuộc tính': 'CIK', 'Giá trị': _text(row['cik'])},
                {'Thuộc tính': 'Chất lượng định danh', 'Giá trị': _text(row['identifier_quality'])},
                {'Thuộc tính': 'Chất lượng membership', 'Giá trị': _text(row['membership_quality'])},
                {'Thuộc tính': 'Concept shares', 'Giá trị': _text(row['shares_source_tag'])},
                {'Thuộc tính': 'Tuổi shares (ngày)',
                 'Giá trị': f"{row['shares_age_days']:.0f}" if pd.notna(row['shares_age_days']) else '—'},
                {'Thuộc tính': 'Phạm vi báo cáo', 'Giá trị': _text(row.get('reporting_scope'))},
            ])
            st.dataframe(provenance, width='stretch', hide_index=True)
            cik = _text(row['cik']).removesuffix('.0')
            if cik.isdigit():
                st.link_button('Đối chiếu hồ sơ SEC ↗',
                               f'https://www.sec.gov/edgar/browse/?CIK={int(cik)}&owner=exclude',
                               width='stretch')
            st.caption('Membership được quan sát theo tháng. Sector là metadata hiện tại; '
                       'đây chưa phải lịch sử phân ngành theo từng thời điểm.')


def _comparison(history, selected, dates, focused, period):
    section('Đặt các mã cạnh nhau',
            'So sánh cấu trúc điểm trên cùng một thang đo; bộ lọc giữ nguyên điểm đã tính trong Gold.')
    options = sorted(selected['ticker'].unique())
    candidates = selected.sort_values(['composite_score', 'ticker'], ascending=[False, True])['ticker'].tolist()
    default = list(dict.fromkeys([focused, *candidates]))[:min(3, len(options))]
    tickers = st.multiselect('Chọn 2–4 mã để so sánh', options, default=default,
                            max_selections=4, key='explorer_compare')
    if len(tickers) < 2:
        st.info('Chọn ít nhất hai mã trong kỳ đang xem để mở bảng so sánh.')
        return
    comparison = selected.set_index('ticker').loc[tickers].reset_index()
    left, right = st.columns([1, 1.2])
    with left:
        st.markdown('**Bốn pillar · cùng kỳ**')
        figure = go.Figure()
        for ticker, color in zip(tickers, COMPARISON_COLORS):
            row = comparison.loc[comparison['ticker'].eq(ticker)].iloc[0]
            figure.add_bar(
                x=list(PILLARS.values()), y=[row[column] for column in PILLARS],
                name=ticker, marker_color=color,
                hovertemplate='%{x}<br>Điểm: %{y:+.3f}<extra>%{fullData.name}</extra>',
            )
        figure.update_layout(barmode='group', bargap=0.3)
        figure.update_yaxes(title='Điểm factor', range=[-3.15, 3.15], zeroline=True)
        figure.update_xaxes(title=None)
        plot(figure, 'explorer_compare_pillars', height=360)
    with right:
        st.markdown('**Composite · qua các kỳ**')
        figure = go.Figure()
        for ticker, color in zip(tickers, COMPARISON_COLORS):
            rows = _aligned_history(history, ticker, dates)
            figure.add_scatter(
                x=rows.index, y=rows['composite_score'], name=ticker,
                mode='lines+markers', connectgaps=False,
                line=dict(color=color, width=2.5), marker=dict(size=4),
                hovertemplate='%{x|%d/%m/%Y}<br>Composite: %{y:+.3f}<extra>%{fullData.name}</extra>',
            )
        figure.update_xaxes(tickformat='%m/%Y', nticks=7)
        figure.update_yaxes(title='Composite score', range=[-3.15, 3.15], zeroline=True)
        figure.update_layout(hovermode='x unified')
        plot(figure, 'explorer_compare_history', height=360)
    st.caption('Cột trống = chưa có điểm. Đường bị ngắt khi thiếu điểm hoặc mã không có trong universe '
               'của kỳ đó. Đường composite mô tả tín hiệu factor, chưa phải đường lợi nhuận.')

    columns = ['ticker', 'sector_label', *SCORE_COLUMNS,
               'available_pillar_count', 'is_research_signal_eligible']
    table = comparison[columns]
    st.dataframe(table, width='stretch', hide_index=True,
                 column_config={**_score_config(), 'ticker': 'Ticker', 'sector_label': 'Sector',
                                'available_pillar_count': 'Pillar có điểm',
                                'is_research_signal_eligible': 'Đủ signal nghiên cứu'})
    downloads = st.columns(2)
    with downloads[0]:
        st.download_button('↓ Bảng so sánh kỳ đang xem', _download(table),
                           f'comparison_{pd.Timestamp(period):%Y-%m-%d}.csv', 'text/csv',
                           key='explorer_comparison_csv', width='stretch')
    with downloads[1]:
        rows = history.loc[history['ticker'].isin(tickers)].sort_values(['rebalance_date', 'ticker'])
        series_columns = ['rebalance_date', 'ticker', 'sector_label', *SCORE_COLUMNS,
                          'available_pillar_count', 'is_research_signal_eligible']
        st.download_button('↓ Lịch sử điểm các mã đang so sánh', _download(rows[series_columns]),
                           f'comparison_history_to_{pd.Timestamp(period):%Y-%m-%d}.csv', 'text/csv',
                           key='explorer_comparison_history_csv', width='stretch')


def render_explorer(history, selected, period, periods=None):
    """Render a dossier using the same period and sector filters as the desk."""
    section('Hồ sơ tín hiệu', 'Đi từ điểm tổng hợp đến từng pillar, lịch sử và nguồn đầu vào của một mã.')
    if selected.empty:
        st.info('Chưa có mã ở kỳ và ngành đang chọn. Đổi phạm vi để mở hồ sơ tín hiệu.')
        return
    # Defensive cutoff: a future observation must never appear in a historical view.
    history = history.loc[history['rebalance_date'] <= pd.Timestamp(period)].copy()
    dates = sorted(history['rebalance_date'].unique() if periods is None else periods)
    options = sorted(selected['ticker'].unique())
    ticker = st.selectbox('Mã đang phân tích', options,
                          index=options.index('AAPL') if 'AAPL' in options else 0,
                          key='explorer_ticker')
    row = selected.loc[selected['ticker'].eq(ticker)].iloc[0]
    heading, status = st.columns([1, 1.3], vertical_alignment='center')
    with heading:
        st.subheader(ticker)
        st.caption(f"{row['sector_label'].title()} · Kỳ {pd.Timestamp(period):%d/%m/%Y}")
    with status:
        if row['is_research_signal_eligible']:
            st.success('Đủ điều kiện signal nghiên cứu · 4/4 pillar')
        else:
            st.warning(f"Chưa đủ điều kiện signal nghiên cứu · {int(row['available_pillar_count'])}/4 pillar có điểm")

    metric_columns = st.columns(5)
    for column, (field, label) in zip(metric_columns, SCORE_LABELS.items()):
        column.metric(label, _number(row[field]), border=True,
                      help='Điểm giữ nguyên từ Gold. Dấu — biểu thị thiếu dữ liệu, không phải điểm 0.')
    scored = selected.loc[selected['composite_score'].notna()]
    if pd.notna(row['composite_score']):
        rank = int(scored['composite_score'].rank(method='min', ascending=False).loc[row.name])
        st.caption(f'Hạng composite {rank}/{len(scored)} trong các mã có composite của kỳ và ngành đang lọc. '
                   'Điểm bằng nhau nhận cùng hạng. Đây là thứ hạng điểm factor.')
    else:
        st.caption('Composite cần đủ cả bốn pillar, mỗi pillar có trọng số 25%. Không chia lại trọng số khi thiếu điểm.')

    left, right = st.columns([1, 1.7])
    with left:
        st.markdown('**Cấu trúc điểm hiện tại**')
        plot(_pillar_figure(row), 'explorer_pillar_profile', height=340)
    with right:
        st.markdown('**Hành trình tín hiệu**')
        plot(_history_figure(history, ticker, dates), 'explorer_factor_history', height=340)
    st.caption('Thang điểm có cả âm và dương; điểm 0 vẫn hiển thị. Khoảng thiếu được giữ nguyên. '
               'Quality proxy là tăng trưởng lợi nhuận quý YoY chuẩn hóa; chưa đo chất lượng lợi nhuận tổng quát.')
    _provenance(row)
    with st.expander(f'Dữ liệu chính xác · lịch sử {ticker}'):
        rows = history.loc[history['ticker'].eq(ticker)].sort_values('rebalance_date')
        columns = ['rebalance_date', 'ticker', 'sector_label', *SCORE_COLUMNS,
                   'available_pillar_count', 'is_research_signal_eligible',
                   'fcf_status', 'sbc_status', 'growth_status', 'momentum_status', 'quality_status',
                   *AGE_COLUMNS, 'cik', 'membership_quality', 'identifier_quality', 'shares_source_tag']
        exact = rows[columns]
        st.dataframe(exact, width='stretch', hide_index=True, height=360,
                     column_config={**_score_config(),
                                    'rebalance_date': st.column_config.DateColumn('Kỳ', format='DD/MM/YYYY')})
        st.download_button(f'↓ Tải dữ liệu {ticker}', _download(exact),
                           f'{ticker}_signals_to_{pd.Timestamp(period):%Y-%m-%d}.csv', 'text/csv',
                           key='explorer_ticker_csv', width='stretch')
    st.divider()
    _comparison(history, selected, dates, ticker, period)
