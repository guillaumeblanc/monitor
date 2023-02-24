from pathlib import Path
import csv
import glob
import datetime
import pandas as pd


def to_csv(data: list, path: Path):
    '''
    Dumps list of entries to csv.
    '''
    new = pd.DataFrame(data)
    new.fillna("", inplace=True)
    new.to_csv(path, index=False)


def from_csv(path: Path):
    return pd.read_csv(str(path))


def from_csvs(path: Path, pattern: str):
    '''
    Returns a list (generator) of dataframe loaded from all csv files that matches the pattern.
    '''
    for filename in path.glob(pattern):
        yield from_csv(filename)


def format_filename(key, time: datetime.datetime):
    formats = {
        'plants': 'plants.csv',
        'realtime': 'realtime_' + time.strftime('%Y') + '.csv',
        'hourly': 'hourly_' + time.strftime('%Y-%m') + '.csv',
        'daily': 'daily_' + time.strftime('%Y-%m') + '.csv',
        'monthly': 'monthly_' + time.strftime('%Y') + '.csv',
        'yearly': 'yearly.csv'
    }
    return formats[key]


def description(parameter: str):
    '''
    Gets description from column name.
    '''
    descriptions = {
        "plant_code": "Plant unique code",
        "plant_name": 'Plant name',
        "plant_addr": 'Detailed address of the plant',
        "collect_time": 'Collection time',
        "capacity": 'Installed capacity (MW)',
        "build_state": 'Plant status: 0: not constructed, 1: under construction, 2: grid-connected',
        "health_state": 'Plant health status: 1: disconnected, 2: faulty, 3: healthy',
        "day_power": 'Yield today (kWh)',
        "month_power": 'Yield this month (kWh)',
        "total_power": 'Total yield (kWh)',
        "day_income": 'Revenue today',
        "total_income": 'Total revenue',
        "radiation_intensity": 'Global irradiation (kWh/m²)',
        "theory_power": 'Theoretical yield (kWh)',
        "inverter_power": 'Inverter yield (kWh)',
        "ongrid_power": 'Feed-in energy',
        "power_profit": 'Revenue',
        "installed_capacity": 'Installed capacity kW',
        'performance_ratio': 'Performance ratio kWh',
        'use_power': 'Consumption (kWh)',
        'perpower_ratio': 'Specific energy (kWh/kWp : h)'
    }
    return descriptions.get(parameter, 'Unknown')
