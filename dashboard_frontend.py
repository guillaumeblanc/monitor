import streamlit as st
from streamlit_extras.metric_cards import style_metric_cards

import pandas as pd
import plotly.express as px
import pydeck as pdk
import json
import datetime

import dashboard_plot as dp
import dashboard_backend as db

from src import std_utils

kDefaultGap = 'large'

st.set_page_config(page_title='SuperCV', layout='wide',
                   menu_items={'about': 'Eau & Soleil du Lac supervision dashboard'})

style_metric_cards(background_color='#262730', border_size_px=0, border_color='#fafafa',
                   border_radius_px=3, border_left_color='#6AB43E', box_shadow=True)
# dp.style_container()

data = db.prepare_data()

# Title bar
top = st.container()
with top:
    logo, title, trend = st.columns([1, 1, 10])
    with logo:
        st.image('logo.png', use_column_width=True, output_format="auto")
    with title:
        # st.header("Supervision Eau & Soleil du Lac")
        pass
    with trend:
        dp.plot_power_trend(data)

st.markdown('----')

# Main content
middle = st.container()
with middle:

    left, right = st.columns([2, 4], gap=kDefaultGap)
    with left:
        trend, graph = st.columns([1, 3])
        with trend:
            st.subheader('Alarms')
            dp.plot_alarms_trend(data)
        with graph:
            st.subheader('Plants health')
            dp.plot_qos(data)

    with right:
        dp.plot_map(data)

st.markdown('----')

maps = st.container()
with maps:
    left, right = st.columns([2, 2], gap=kDefaultGap)
    with left:
        st.subheader('Yield Statistics')
        cumulated, hourly, daily, monthly, yearly = st.tabs(
            ['Lifetime', 'Hourly', 'Daily', 'Monthly', 'Yearly'])
        with cumulated:
            dp.plot_power_realtime(data)
        with hourly:
            dp.plot_power_hourly(data)
        with daily:
            dp.plot_power_daily(data)
        with monthly:
            dp.plot_power_monthly(data)
        with yearly:
            dp.plot_power_yearly(data)
    with right:
        st.subheader('Power ratio')
        daily, monthly, yearly = st.tabs(['Daily', 'Monthly', 'Yearly'])
        with daily:
            dp.plot_power_daily_ppr(data)
        with monthly:
            dp.plot_power_monthly_ppr(data)
        with yearly:
            dp.plot_power_yearly_ppr(data)

st.markdown('----')

# Bottom
bottom = st.container()
with bottom:
    pass
