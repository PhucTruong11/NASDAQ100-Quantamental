"""Cross-sectional factor diagnostics; no return or prediction claims."""
import numpy as np
import plotly.graph_objects as go
import streamlit as st

from app.analytics import pillar_coverage_by_period, score_correlations, sector_pillar_summary
from app.data import PILLARS
from app.ui import COLORS, PILLAR_COLORS, plot, section

SECTOR_COLORS = dict(zip(
    ['TECHNOLOGY', 'HEALTHCARE', 'FINANCIAL SERVICES', 'CONSUMER CYCLICAL',
     'CONSUMER DEFENSIVE', 'COMMUNICATION SERVICES', 'INDUSTRIALS', 'ENERGY',
     'UTILITIES', 'REAL ESTATE', 'BASIC MATERIALS'],
    ['#5DE4C7', '#77A8FF', '#F1BE68', '#EA88B1', '#A89DF0', '#76C6E7',
     '#B4CA7B', '#E8947A', '#AAC5D7', '#B58CA6', '#C4B29D']))
SIGNED_SCALE = [[0, '#B87091'], [.5, '#20334B'], [1, '#5DE4C7']]


def render_factor_lab(history, selected, all_periods):
    section('Phân tích factor', 'Khám phá phân bố, mối liên hệ và coverage của các điểm đã tính trong Gold.')
    if selected.empty:
        st.info('Không có mã tại kỳ và ngành đang chọn.')
        return
    left, right = st.columns([1.4, 1])
    with left:
        controls = st.columns(2)
        x = controls[0].selectbox('Trục X', list(PILLARS), format_func=PILLARS.get, key='factor_x')
        y_options = [column for column in PILLARS if column != x]
        y = controls[1].selectbox('Trục Y', y_options, format_func=PILLARS.get, key='factor_y')
        paired = selected.dropna(subset=[x, y])
        figure = go.Figure()
        sectors = sorted(selected['sector_label'].unique())
        for sector in sectors:
            color = SECTOR_COLORS.get(sector, '#74869B')
            rows = paired[paired['sector_label'].eq(sector)]
            figure.add_scatter(x=rows[x], y=rows[y], name=sector.title(), mode='markers',
                               text=rows['ticker'], marker=dict(color=color, size=10, opacity=.85,
                                                                 line=dict(color=COLORS['panel'], width=1)),
                               hovertemplate='%{text}<br>' + PILLARS[x] + ': %{x:+.3f}<br>' + PILLARS[y] + ': %{y:+.3f}<extra>%{fullData.name}</extra>')
        figure.update_xaxes(title=PILLARS[x], range=[-3.2, 3.2], zeroline=True)
        figure.update_yaxes(title=PILLARS[y], range=[-3.2, 3.2], zeroline=True)
        # Sector is in each point's tooltip; a long legend would crowd the comparison.
        figure.update_layout(showlegend=False)
        plot(figure, 'factor_scatter', 410)
        st.caption(f'{len(paired)}/{len(selected)} mã có đồng thời hai điểm. Màu thể hiện sector; rê chuột để xem mã.')
    with right:
        section('Tương quan điểm Spearman', 'Tính từng cặp trên các mã có đủ hai điểm; tooltip giữ số mẫu n.')
        correlation, counts = score_correlations(selected, list(PILLARS))
        labels = list(PILLARS.values())
        values = correlation.to_numpy()
        annotations = [[f'{v:.2f}' if np.isfinite(v) else '—' for v in row] for row in values]
        figure = go.Figure(go.Heatmap(
            z=values, x=labels, y=labels, zmin=-1, zmax=1, colorscale=SIGNED_SCALE,
            text=annotations, texttemplate='%{text}', textfont=dict(size=14, color=COLORS['text']),
            customdata=counts.to_numpy(), xgap=4, ygap=4,
            hovertemplate='%{y} × %{x}<br>Spearman: %{z:.3f}<br>n = %{customdata}<extra></extra>',
            colorbar=dict(thickness=9, len=.8, tickvals=[-1, 0, 1]), hoverongaps=False,
        ))
        figure.update_yaxes(autorange='reversed', showgrid=False)
        figure.update_xaxes(side='bottom')
        plot(figure, 'factor_correlation', 410)
        st.caption('Ô trống: n < 3 hoặc điểm không biến thiên. Đây là liên hệ giữa các signal, chưa phải IC với lợi nhuận tương lai.')
    section('Phân bố điểm trên cùng thang đo', 'Mỗi pillar dùng các mã có điểm riêng; các tập mẫu có thể khác nhau.')
    figure = go.Figure()
    for column, label in PILLARS.items():
        values = selected[column].dropna()
        figure.add_trace(go.Box(y=values, name=f'{label} · n={len(values)}',
                                marker_color=PILLAR_COLORS[column], boxpoints='all', jitter=.35,
                                pointpos=0, marker=dict(size=4, opacity=.45),
                                hovertemplate='%{y:+.3f}<extra>%{fullData.name}</extra>'))
    figure.update_yaxes(range=[-3.25, 3.25], title='Điểm chuẩn hóa', zeroline=True)
    figure.update_layout(showlegend=False)
    plot(figure, 'factor_distributions', 330)
    section('Coverage của từng factor qua thời gian', 'Mỗi ô = số mã có điểm / toàn bộ universe thuộc ngành đang lọc ở kỳ đó.')
    coverage = pillar_coverage_by_period(history)
    order = [*PILLARS.values(), 'Composite']
    dates = sorted(all_periods)
    wide = coverage.pivot(index='pillar_label', columns='rebalance_date', values='coverage_pct').reindex(index=order, columns=dates)
    numerator = coverage.pivot(index='pillar_label', columns='rebalance_date', values='available_count').reindex(index=order, columns=dates)
    denominator = coverage.pivot(index='pillar_label', columns='rebalance_date', values='universe_count').reindex(index=order, columns=dates)
    figure = go.Figure(go.Heatmap(z=wide.to_numpy(), x=dates, y=order, zmin=0, zmax=100,
                                colorscale=[[0, '#16263D'], [1, '#5DE4C7']], xgap=2, ygap=4,
                                customdata=np.stack([numerator.to_numpy(), denominator.to_numpy()], axis=-1),
                                hovertemplate='%{y} · %{x|%d/%m/%Y}<br>%{z:.2f}%<br>%{customdata[0]:.0f} / %{customdata[1]:.0f} mã<extra></extra>',
                                colorbar=dict(thickness=9, ticksuffix='%'), hoverongaps=False))
    figure.update_xaxes(tickformat='%m/%Y', nticks=8)
    figure.update_yaxes(autorange='reversed', showgrid=False)
    plot(figure, 'factor_coverage_history', 300)
    with st.expander('Điểm trung vị theo ngành · kỳ đang chọn'):
        sectors = sector_pillar_summary(selected)
        wide = sectors.pivot(index='sector_label', columns='pillar_label', values='median_score').reindex(columns=order)
        available = sectors.pivot(index='sector_label', columns='pillar_label', values='available_count').reindex(index=wide.index, columns=order)
        total = sectors.pivot(index='sector_label', columns='pillar_label', values='universe_count').reindex(index=wide.index, columns=order)
        figure = go.Figure(go.Heatmap(z=wide.to_numpy(), x=order, y=wide.index.str.title(),
                                    zmin=-3, zmax=3, colorscale=SIGNED_SCALE, xgap=3, ygap=3,
                                    customdata=np.stack([available.to_numpy(), total.to_numpy()], axis=-1),
                                    hovertemplate='%{y} · %{x}<br>Trung vị: %{z:+.3f}<br>n = %{customdata[0]:.0f} / %{customdata[1]:.0f}<extra></extra>',
                                    colorbar=dict(thickness=9), hoverongaps=False))
        figure.update_yaxes(autorange='reversed', showgrid=False)
        plot(figure, 'sector_factor_medians', max(300, len(wide) * 34))
        st.caption('Trung vị chỉ dùng mã có điểm. Các điểm đã chuẩn hóa theo ngành hoặc fallback toàn universe; đây không phải so sánh lợi nhuận ngành.')
        st.dataframe(sectors, hide_index=True, width='stretch')
