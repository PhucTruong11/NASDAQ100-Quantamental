"""Shared visual system for the local Streamlit research desk."""
from html import escape

import streamlit as st

COLORS = dict(background='#081220', panel='#101E31', text='#E8EEF7',
              muted='#91A4BE', grid='#22344C', teal='#5DE4C7', blue='#77A8FF',
              amber='#F1BE68', rose='#EA88B1', neutral='#425773')
PILLAR_COLORS = {'value_score': COLORS['teal'], 'growth_score': COLORS['blue'],
                 'momentum_score': COLORS['amber'], 'quality_score': COLORS['rose']}


def apply_style():
    st.html('''<style>
    :root { color-scheme: dark; }
    .stApp { background:#081220; }
    [data-testid="stHeader"] { background:rgba(8,18,32,.95); }
    [data-testid="stAppDeployButton"] { display:none; }
    .block-container { max-width:1680px; padding:2.5rem 2.4rem 4rem; }
    [data-testid="stSidebar"] { background:#0C1829; border-right:1px solid #21334B; }
    [data-testid="stSidebarContent"] { padding-top:1.1rem; }
    [data-testid="stSidebar"] h2 { font-size:.83rem; letter-spacing:.07em; color:#91A4BE; }
    [data-testid="stSidebar"] [role="radiogroup"] { gap:.35rem; }
    [data-testid="stSidebar"] [role="radiogroup"] label {
      padding:.55rem .7rem; border:1px solid transparent; border-radius:8px;
    }
    [data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {
      background:#152F3D; border-color:#2A615E;
    }
    h1 { font-size:2.05rem!important; letter-spacing:-.045em; font-weight:650!important; }
    h2 { font-size:1.35rem!important; letter-spacing:-.025em; }
    h3 { font-size:1.08rem!important; letter-spacing:-.015em; }
    [data-testid="stCaptionContainer"] { color:#91A4BE; line-height:1.65; }
    [data-testid="stMetric"] {
      background:linear-gradient(130deg,#12243A,#0F1C2D);
      border:1px solid #253952!important; border-radius:12px; padding:1.1rem 1.15rem!important;
      min-height:142px; box-shadow:0 8px 22px #00000018;
    }
    [data-testid="stMetricLabel"] { color:#A3B3C8; font-size:.78rem; }
    [data-testid="stMetricValue"] { color:#F0F6FF; font-variant-numeric:tabular-nums; letter-spacing:-.045em; }
    [data-testid="stMetricDelta"] { font-size:.75rem; }
    [data-testid="stPlotlyChart"] { background:#101E31; border:1px solid #20334B; border-radius:12px; padding:.35rem; }
    [data-testid="stDataFrame"] { border:1px solid #263B55; border-radius:10px; overflow:hidden; }
    [data-testid="stExpander"] { border-color:#253952; background:#0C192A; border-radius:10px; }
    [data-testid="stAlert"] { border-radius:9px; }
    [data-testid="stButton"] button, [data-testid="stDownloadButton"] button {
      border-radius:8px; border-color:#314761; font-weight:500;
    }
    button:focus-visible, input:focus-visible { outline:2px solid #5DE4C7!important; outline-offset:3px; }
    .desk-brand { padding:.25rem 0 1.5rem; border-bottom:1px solid #263B55; margin-bottom:1rem; }
    .desk-brand b { color:#E8EEF7; font-size:1.2rem; letter-spacing:-.04em; }
    .desk-brand span { color:#5DE4C7; }
    .desk-brand small { display:block; color:#91A4BE; font-size:.72rem; letter-spacing:.13em; margin-top:.35rem; }
    .desk-meta { display:flex; align-items:center; gap:.5rem; flex-wrap:wrap; margin:.1rem 0 1.1rem; }
    .desk-chip { border:1px solid #2B415E; border-radius:6px; padding:.3rem .65rem; font-size:.72rem; color:#AFC0D5; }
    .desk-chip.accent { border-color:#286458; color:#5DE4C7; background:#102F30; }
    .desk-chip.amber { border-color:#6A5432; color:#F1BE68; background:#29251F; }
    .desk-section { margin:1.25rem 0 .7rem; }
    .desk-section h3 { margin:0 0 .3rem; padding:0; color:#E8EEF7; }
    .desk-section p { color:#91A4BE; font-size:.78rem; line-height:1.6; margin:0; }
    .desk-foot { color:#7289A4; font-size:.72rem; border-top:1px solid #20334B; padding-top:1rem; margin-top:2rem; }
    .desk-step { background:#101E31; border:1px solid #263B55; border-radius:12px; padding:1.2rem; min-height:165px; }
    .desk-step small { color:#5DE4C7; font-size:.72rem; }
    .desk-step h3 { color:#E8EEF7; margin:.6rem 0; }
    .desk-step p { color:#91A4BE; font-size:.82rem; line-height:1.6; }
    @media (max-width:1100px) {
      .block-container { padding-left:1.4rem; padding-right:1.4rem; }
      [data-testid="stHorizontalBlock"] { flex-wrap:wrap; }
      [data-testid="stHorizontalBlock"] > [data-testid="stColumn"] { flex:1 1 100%!important; width:100%!important; }
      [data-testid="stHorizontalBlock"]:has(> [data-testid="stColumn"] [data-testid="stMetric"]) > [data-testid="stColumn"] { flex:1 1 42%!important; width:42%!important; }
    }
    @media (max-width:600px) {
      h1 { font-size:1.6rem!important; }
      .block-container { padding:2rem .9rem; }
      [data-testid="stMetric"] { padding:.8rem!important; min-height:122px; }
      [data-testid="stMetricValue"] { font-size:1.8rem; }
    }
    </style>''')


def section(title, description=None):
    body = f'<p>{escape(description)}</p>' if description else ''
    st.html(f'<div class="desk-section"><h3>{escape(title)}</h3>{body}</div>')


def chips(items):
    st.html('<div class="desk-meta">' + ''.join(
        f'<span class="desk-chip {escape(tone)}">{escape(text)}</span>' for text, tone in items
    ) + '</div>')


def plot(figure, key, height=330):
    multi = len(figure.data) > 1 and figure.layout.showlegend is not False
    figure.update_layout(
        template='plotly_dark', height=height,
        margin=dict(l=18, r=22, t=65 if multi else 25, b=48),
        font=dict(family='Inter, Segoe UI, sans-serif', size=12, color=COLORS['muted']),
        paper_bgcolor=COLORS['panel'], plot_bgcolor=COLORS['panel'],
        colorway=list(PILLAR_COLORS.values()),
        legend=dict(orientation='h', y=1.18, x=0, traceorder='normal', font=dict(size=11)),
        hoverlabel=dict(bgcolor='#1C304B', font_color=COLORS['text'], bordercolor='#49617F'),
    )
    figure.update_xaxes(showgrid=False, zerolinecolor='#47617F', automargin=True)
    figure.update_yaxes(gridcolor=COLORS['grid'], zerolinecolor='#47617F', automargin=True)
    st.plotly_chart(figure, width='stretch', theme=None, key=key,
                    config={'displaylogo': False, 'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
                            'toImageButtonOptions': {'format': 'png', 'filename': key, 'scale': 2}})
