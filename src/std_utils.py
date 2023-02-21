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
