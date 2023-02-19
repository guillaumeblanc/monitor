from pathlib import Path
import csv
import datetime
import glob
import pandas as pd


def flatten(data):
    '''
    Convert list of dataItemMap to a flat list of data.
    '''
    for entry in data:
        line = entry['dataItemMap']
        line['stationCode'] = entry['stationCode']
        if entry.get('collectTime'):
            line['collectTime'] = entry['collectTime']
        yield line

def fixup_time(df):
    def fixup_time_col(df:pd.DataFrame, col:str):
        if col in df:
            df[col] = df[col].map(lambda t: datetime.datetime.fromtimestamp(t / 1000.))

    fixup_time_col(df, 'collectTime')
    return df

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


def description(parameter: str):
    '''
    Gets description from column name.
    '''
    descriptions = {
        'stationName': 'Plant name',
        'stationAddr': 'Detailed address of the plant',
        'capacity': 'Installed capacity (MW)',
        'buildState': 'Plant status: 0: not constructed, 1: under construction, 2: grid-connected',
        'combineType': 'Grid connection type: 1: utility, 2: commercial & industrial, 3: residential',
        'aidType': 'Poverty alleviation plant ID: 0: poverty alleviation plant, 1: non-poverty alleviation plant',
        'stationLinkman': 'Plant contact',
        'linkmanPho': 'Contact phone number',
        'day_power': 'Yield today (kWh)',
        'month_power': 'Yield this month (kWh)',
        'total_power': 'Total yield (kWh)',
        'day_income': 'Revenue today',
        'total_income': 'Total revenue',
        'real_health_state': 'Plant health status: 1: disconnected, 2: faulty, 3: healthy',
        'radiation_intensity': 'Global irradiation (kWh/m²)',
        'theory_power': 'Theoretical yield (kWh)',
        'inverter_power': 'Inverter yield (kWh)',
        'ongrid_power': 'Feed-in energy',
        'power_profit': 'Revenue',
        'installed_capacity': 'Installed capacity kW',
        'performance_ratio': 'Performance ratio kWh',
        'use_power': 'Consumption (kWh)',
        'perpower_ratio': 'Specific energy (kWh/kWp : h)',
        'reduction_total_co2': 'CO2 emission reduction (Ton)',
        'reduction_total_coal': 'Standard coal saved (Ton)',
        'reduction_total_tree': 'Equivalent trees planted',
    }
    return descriptions.get(parameter, 'Unknown')
