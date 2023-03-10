from pathlib import Path
import csv
import glob
import datetime
import pandas as pd


def to_csv(data: pd.DataFrame, path: Path):
    '''
    Dumps list of entries to csv.
    '''
    path.parent.mkdir(parents=True, exist_ok=True)

    data.fillna('', inplace=True)
    data.to_csv(path, index=False)


def from_csv(path: Path):

    df = pd.read_csv(str(path))
    return df


def from_csvs(path: Path, pattern: str):
    '''
    Returns a list (generator) of dataframe loaded from all csv files that matches the pattern.
    '''
    aggregated = pd.DataFrame()
    for filename in path.glob(pattern):
        aggregated = pd.concat([aggregated, from_csv(filename)])

    aggregated.drop_duplicates(inplace=True)

    return aggregated

def categories(df: pd.DataFrame):
    cats = ['plant_code', 'plant_name', 'build_state', 'health_state']
    for col in df.columns:
        if col in cats:
            df[col] = pd.Categorical(df[col])
    return df

def file_patterns():
    return ['plants', 'realtime', 'hourly', 'daily', 'monthly', 'yearly']


def format_filename(key, time: datetime.datetime):
    year = time.strftime('%Y') + '/'
    formats = {
        'plants': year + 'plants_' + time.strftime('%Y-%m') + '.csv',
        'realtime': year + 'realtime_' + time.strftime('%Y-%m-%d') + '.csv',
        'hourly': year + 'hourly_' + time.strftime('%Y-%m-%d') + '.csv',
        'daily': year + 'daily_' + time.strftime('%Y-%m') + '.csv',
        'monthly': year + 'monthly_' + time.strftime('%Y') + '.csv',
        'yearly': year + 'yearly_' + time.strftime('%Y') + '.csv'
    }
    return Path(formats[key])


def descriptions():
    '''
    Gets a dict mapping column name to its description.
    '''
    return {
        'plant_code': 'Plant unique code',
        'plant_name': 'Plant name',
        'plant_addr': 'Detailed address of the plant',
        'collect_time': 'Collection time',
        'capacity': 'Installed capacity (MW)',
        'health_state': 'Plant health status',
        'day_power': 'Yield today (kWh)',
        'month_power': 'Yield this month (kWh)',
        'total_power': 'Total yield (kWh)',
        'day_income': 'Revenue today',
        'total_income': 'Total revenue',
        'radiation_intensity': 'Global irradiation (kWh/m??)',
        'theory_power': 'Theoretical yield (kWh)',
        'inverter_power': 'Inverter yield (kWh)',
        'ongrid_power': 'Feed-in energy',
        'power_profit': 'Revenue',
        'installed_capacity': 'Installed capacity kW',
        'performance_ratio': 'Performance ratio kWh',
        'use_power': 'Consumption (kWh)',
        'perpower_ratio': 'Specific energy (kWh/kWp : h)'
    }


def description(parameter: str):
    '''
    Gets description from column name.
    '''
    return descriptions().get(parameter, 'Unknown')
