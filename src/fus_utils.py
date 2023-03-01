from pathlib import Path
import csv
import datetime
import pandas as pd
from extern.fusnic import fusnic


def flatten(data):
    '''
    Convert list of dataItemMap to a flat list of data.
    Fixup also non standard units, like time
    '''
    def iterate(data):
        for entry in data:
            line = entry['dataItemMap']
            line['stationCode'] = entry['stationCode']

            # Fix up time
            if entry.get('collectTime'):
                line['collectTime'] = fusnic.Client.from_timestamp(
                    entry['collectTime'])

            yield line

    return pd.DataFrame(iterate(data))


def descriptions():
    '''
    Gets a dict mapping column name to its description.
    '''
    return {
        'plant_code': 'Plant unique code',
        'stationName': 'Plant name',
        'stationAddr': 'Detailed address of the plant',
        'capacity': 'Installed capacity (MW)',
        'buildState': 'Plant status: 0: not constructed, 1: under construction, 2: grid-connected',
        'combineType': 'Grid connection type: 1: utility, 2: commercial & industrial, 3: residential',
        'aidType': 'Poverty alleviation plant ID: 0: poverty alleviation plant, 1: non-poverty alleviation plant',
        'stationLinkman': 'Plant contact',
        'linkmanPho': 'Contact phone number',
        'collectTime': 'Collection time',
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


def description(parameter: str):
    '''
    Gets description from column name.
    '''
    return descriptions().get(parameter, 'Unknown')
