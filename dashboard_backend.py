import pandas as pd
import base64
import json
from pathlib import Path
import streamlit as st
import datetime

import dashboard_backend as db
import src.gdrive as gdrive
import src.std_utils as std_utils

kDataPath = Path('data/agg')
kMaxEntries = 1


@st.cache_resource(show_spinner='Fetching data...', ttl=6*60*60, max_entries=kMaxEntries)
def download_data():
    kDataPath.mkdir(parents=True, exist_ok=True)
    key = json.loads(base64.b64decode(st.secrets.gdrive.credentials))
    auth = gdrive.authenticate(key)
    with gdrive.GoogleDriveClient(auth) as drive:
        drive.download(kDataPath, st.secrets.gdrive.folder,
                       Path('agg'), '**/*')


@st.cache_data(show_spinner='Loading data...', max_entries=kMaxEntries)
def load_data():

    # Loads plants
    plants = std_utils.from_csv(kDataPath / 'plants.csv')
    plants_name = plants[['plant_code', 'plant_name']]

    # Loads data, merges with plants information (to get plants name)
    realtime = pd.merge(std_utils.from_csvs(
        kDataPath, '**/realtime.csv'), plants_name, on=['plant_code'])
    hourly = pd.merge(std_utils.from_csvs(
        kDataPath, '**/hourly.csv'), plants_name, on=['plant_code'])
    daily = pd.merge(std_utils.from_csvs(
        kDataPath, '**/daily.csv'), plants_name, on=['plant_code'])
    monthly = pd.merge(std_utils.from_csvs(
        kDataPath, '**/monthly.csv'), plants_name, on=['plant_code'])
    yearly = pd.merge(std_utils.from_csvs(
        kDataPath, '**/yearly.csv'), plants_name, on=['plant_code'])
    alarms = pd.merge(std_utils.from_csvs(
        kDataPath, '**/alarms.csv'), plants_name, on=['plant_code'])

    return {'plants': plants,
            'realtime': realtime,
            'hourly': hourly,
            'daily': daily,
            'monthly': monthly,
            'yearly': yearly,
            'alarms': alarms}


def prepare_data():
    download_data()
    l = load_data()
    return l


def data_in_range(data, begin, end):
    return data.loc[(data['collect_time'] >= begin)
                    & (data['collect_time'] <= end)]


@st.cache_data(max_entries=kMaxEntries)
def power_trend_data(data):

    realtime = data['realtime']
    lastest_collect = realtime['collect_time'].max().to_pydatetime()
    freshness = datetime.datetime.now() - lastest_collect

    lastest_realtime = realtime[realtime.groupby(['plant_code'])['collect_time'].transform(
        max) == realtime['collect_time']]

    def power_per_plant(data, days):
        in_range = data_in_range(
            data, lastest_collect - datetime.timedelta(days), lastest_collect)
        dl = in_range.groupby('plant_name')['inverter_power'].sum().to_frame(
            name='total').reset_index()
        return dl

    return {'capacity': data['plants']['capacity'].sum(),
            'yesterday': power_per_plant(data['hourly'], 1)['total'].sum(),
            'monthly': power_per_plant(data['daily'], 30)['total'].sum(),
            'yearly': power_per_plant(data['daily'], 365)['total'].sum(),
            'total': lastest_realtime['total_power'].sum(),
            'freshness': freshness}


@st.cache_data(max_entries=kMaxEntries)
def plant_map_data(data):

    realtime = data['realtime']
    lastest = realtime[realtime.groupby(['plant_code'])['collect_time'].transform(
        max) == realtime['collect_time']]

    lastest['month_power'] = lastest['month_power'].apply(
        lambda d: f'{round(d /1000, 1)}')
    lastest['total_power'] = lastest['total_power'].apply(
        lambda d: f'{round(d /1000, 1)}')

    return pd.merge(lastest, data['plants'], on=['plant_code', 'plant_name'])


@st.cache_data(max_entries=kMaxEntries)
def alarms_trend_data(data):
    alarms = data['alarms']
    severities = alarms['alarm_severity'].value_counts()

    # Fixup missing severities
    for sev in ['Critical', 'Major', 'Minor', 'Warning']:
        if sev not in severities:
            severities[sev] = 0

    return {'critical': severities['Critical'],
            'major': severities['Major'],
            'minor': severities['Minor'],
            'warning': severities['Warning']}


@st.cache_data(max_entries=kMaxEntries)
def qos_data(data):
    realtime = data['realtime']
    by_plant = realtime.groupby(['plant_name'])
    normalized = by_plant['health_state'].value_counts(normalize=True)
    count = by_plant['health_state'].value_counts()

    df = pd.DataFrame({'Quality of Service': normalized,
                      'Days': count}).reset_index()
    return df
