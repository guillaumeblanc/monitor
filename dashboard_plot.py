import streamlit.components.v1 as components
import streamlit as st

import pandas as pd
import plotly.express as px
import pydeck as pdk
import json
import datetime

from typing import cast
from typing import Tuple

import dashboard_backend as db
from src import std_utils


# @extra
def style_container(
    background_color: str = "#FFF",
    border_size_px: int = 1,
    border_color: str = "#CCC",
    border_radius_px: int = 5,
    border_left_color: str = "#9AD8E1",
    box_shadow: bool = True,
):
    st.markdown(
        f"""
        <style>
            div[data-testid="column"] {{
                border: {border_size_px}px solid {border_color};
                border-radius: {border_radius_px}px;
                border-left: 0.5rem solid {border_left_color} !important;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def date_range_picker(
        title: str,
        default_start,
        default_end,
        min_date,
        max_date,
        error_message: str = "Please select start and end date",
        label_visibility='visible',
        key=None) -> Tuple[datetime.date, datetime.date]:

    val = st.date_input(
        title,
        value=[default_start, default_end],
        min_value=min_date,
        max_value=max_date,
        key=key,
        label_visibility=label_visibility
    )
    try:
        start_date, end_date = cast(Tuple[datetime.date, datetime.date], val)
    except ValueError:
        st.error(error_message)
        st.stop()

    return start_date, end_date


def plot_power_trend(data):
    ptd = db.power_trend_data(data)

    cols = st.columns(6)
    cols[0].metric(label='Capacity', value='%.1f kWc' % ptd['capacity'])
    cols[1].metric(label='Last 24h', value='%.1f kWh' % ptd['yesterday'])
    cols[2].metric(label='Last month', value='%.1f MWh' %
                   (ptd['monthly'] / 1000))
    cols[3].metric(label='Last year', value='%.1f MWh' %
                   (ptd['yearly'] / 1000))
    cols[4].metric(label='Total', value=' % .1f MWh' %
                   (ptd['total'] / 1000))
    cols[5].metric(label='Data freshness', value='%.0f h' %
                   (ptd['freshness'].total_seconds() / 3600))


def plot_alarms_trend(data):
    atd = db.alarms_trend_data(data)

    delta = {}
    for sev in atd.keys():
        delta[sev] = int(atd[sev]) if atd[sev] else None

    st.metric(label=':violet[Critical]',
              value=atd['critical'], delta=delta['critical'], delta_color='inverse')
    st.metric(label=':red[Major]', value=atd['major'],
              delta=delta['major'], delta_color='inverse')
    st.metric(label=':orange[Minor]', value=atd['minor'],
              delta=delta['minor'], delta_color='inverse')
    st.metric(label=':green[Warning]',
              value=atd['warning'], delta=delta['warning'], delta_color='inverse')


def plot_qos(data):

    qos = db.qos_data(data)
    fig = px.bar(qos, x='plant_name', y='Quality of Service', color='health_state', hover_data=["Quality of Service", "Days"], orientation='v',
                 color_discrete_map=std_utils.health_state_colormap(), labels=std_utils.descriptions())
    fig.update_layout(xaxis_title=None)
    # ,config={'displayModeBar': False})
    st.plotly_chart(fig, use_container_width=True)


@ st.cache_data(max_entries=20)
def plot_power_ctrl(data, step, past):
    # h = data['collect_time'].max().to_pydatetime()
    h = datetime.datetime.now()
    l = data['collect_time'].min()
    # l = min(data['collect_time'].min().to_pydatetime(), h - step)
    # l = h-step

    return {'min': l,
            'max': h,
            'rmin': max(l, h - past) if past else l,
            'rmax': h,
            'step': step}


@st.cache_data(max_entries=db.kMaxEntries)
def plot_power_realtime_data(data):
    return data['realtime'][['collect_time', 'total_power', 'day_power', 'month_power', 'plant_name']]


def plot_power_realtime(data):
    realtime = plot_power_realtime_data(data)
    fig = px.area(realtime, x='collect_time', y='total_power',
                  color='plant_name',
                  hover_data=['total_power', 'day_power', 'month_power'],
                  color_discrete_sequence=px.colors.qualitative.D3,
                  labels=std_utils.descriptions())
    fig.update_traces(mode="markers+lines")
    fig.update_layout(hovermode="x")
    fig.update_layout(xaxis_title=None)  # , config={'displayModeBar': False})
    st.plotly_chart(fig, use_container_width=True)


@st.cache_data(max_entries=20)
def plot_power_data(data, range):
    # in_range = data.loc[(data['collect_time'] >= pd.to_datetime(range[0], utc=True).date())
    #                     & (data['collect_time'] <= pd.to_datetime(range[1], utc=True).date())]

    collect_date = data['collect_time'].dt.date
    in_range = data.loc[(collect_date >= range[0]) &
                        (collect_date <= range[1])]
    return in_range[['collect_time', 'plant_name', 'inverter_power']]


def _plot_power(data, step, past, slider_format, tickformat, dtick):
    ctrl = plot_power_ctrl(data, step, past)

    range = date_range_picker(
        'Select date range', min_date=ctrl['min'], max_date=ctrl['max'], default_start=ctrl['rmin'], default_end=ctrl['rmax'], label_visibility='collapsed')

    # range = st.slider('Select date range', label_visibility='collapsed',
    #                   min_value=ctrl['min'], max_value=ctrl['max'],
    #                   value=(ctrl['rmin'], ctrl['rmax']),
    #                   step=ctrl['step'], format=slider_format)

    data_range = plot_power_data(data, range)

    fig = px.bar(data_range, x='collect_time', y='inverter_power', color="plant_name",
                 barmode='stack', labels=std_utils.descriptions())
    fig.update_layout(xaxis=dict(tickformat=tickformat, dtick=dtick))

    # ,config={'displayModeBar': False})
    st.plotly_chart(fig, use_container_width=True)


def plot_power_hourly(data):
    _plot_power(data['hourly'], datetime.timedelta(
        hours=1), datetime.timedelta(days=5), 'DD/MM/YYYY hh', '%Hh\n%d %b', '')


def plot_power_daily(data):
    _plot_power(data['daily'], datetime.timedelta(days=1),
                datetime.timedelta(weeks=2*4), 'DD/MM/YYYY', '%d\n%b', '24*60*60*1000')


def plot_power_monthly(data):
    _plot_power(data['monthly'], datetime.timedelta(
        weeks=4), datetime.timedelta(weeks=2*52), 'MM/YYYY', '%b\n%Y', 'M1')


def plot_power_yearly(data):
    _plot_power(data['yearly'], datetime.timedelta(
        weeks=52), None, 'YYYY', '%Y', 'M12')


def _plot_power_ppr(data, step, past, slider_format, plot_format):
    ctrl = plot_power_ctrl(data, step, past)

    # range = st.slider('Select date range', label_visibility='collapsed',
    #                   min_value=ctrl['min'], max_value=ctrl['max'],
    #                   value=(ctrl['rmin'], ctrl['rmax']),
    #                   step=ctrl['step'], format=slider_format)

    data_range = data  # plot_power_data(data, range)

    fig = px.line(data_range, x='collect_time', y='perpower_ratio', color="plant_name",
                  labels=std_utils.descriptions() | {'perpower_ratio': 'Performance ratio (kWh/kWp)'})
    fig.update_layout(xaxis=dict(tickformat=plot_format))

    # ,config={'displayModeBar': False})
    st.plotly_chart(fig, use_container_width=True)


def ppr(data, plants):
    df = pd.merge(data, plants[['plant_code', 'capacity']], on=['plant_code'])
    df['perpower_ratio'] = df['inverter_power'] / df['capacity']
    return df


def plot_power_daily_ppr(data):
    df = ppr(data['daily'], data['plants'])
    _plot_power_ppr(df, datetime.timedelta(
        hours=1), datetime.timedelta(weeks=2), 'DD/MM/YYYY hh', '%d-%b %Hh')


def plot_power_monthly_ppr(data):
    df = ppr(data['monthly'], data['plants'])
    _plot_power_ppr(df, datetime.timedelta(
        weeks=4), datetime.timedelta(weeks=2*52), 'MM/YYYY', '%b')


def plot_power_yearly_ppr(data):
    df = ppr(data['yearly'], data['plants'])
    _plot_power_ppr(df, datetime.timedelta(
        weeks=52), None, 'YYYY', '%Y')


def plot_map(data):

    map_data = db.plant_map_data(data)

    boundaries = {}
    with open('resources/geo/territoire.geojson') as f:
        boundaries = json.load(f)

    geojson = pdk.Layer(
        'GeoJsonLayer',
        boundaries,
        opacity=0.8,
        stroked=True,
        filled=False,
        extruded=False,
        get_line_color=[0, 0, 255],
        line_width_min_pixels=2,
    )

    pv = pdk.Layer(
        'ColumnLayer',
        data=map_data,
        get_position='[longitude,latitude]',
        radius=80,
        auto_highlight=True,
        elevation_scale=50,
        pickable=True,
        opacity=0.5,
        get_fill_color='(health_state == "Healthy") ? [0, 255, 0, 255] : ((health_state == "Faulty") ? [255, 0, 0, 255]: [128, 128, 128, 255])',
        get_elevation='capacity',
        extruded=True)

    tooltip = {
        "html": "<b>{plant_name}</b><br>{plant_addr}<br>Capacity: {capacity} kWc<br>Status: {health_state}<br>Last day: {day_power} kWh<br>Last month: {month_power} MWh<br> Total: {total_power} MWh",
        "style": {
            "background": "grey",
            "color": "white",
            "z-index": "10000"}
    }

    view_state = pdk.ViewState(
        latitude=45.71,
        longitude=5.87,
        zoom=11,
        min_zoom=9,
        pitch=50,
        bearing=0
    )

    st.pydeck_chart(pdk.Deck(
        map_style='road',
        layers=[geojson, pv],
        initial_view_state=view_state,
        tooltip=tooltip,
    ))
