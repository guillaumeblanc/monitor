from pathlib import Path
import csv
import datetime
import pytz
import pandas as pd
import pyhfs


def flatten(data, timezone):
    '''
    Convert list of dataItemMap to a flat list of data.
    Fixup also non standard units, like time
    '''
    def iterate(data):
        for entry in data:
            line = entry.get('dataItemMap')
            if line:
                line['stationCode'] = entry['stationCode']
            else:
                line = entry

            # Copy collect time
            if 'collectTime' in entry.keys():
                line['collectTime'] = entry['collectTime']

            # Fix up time
            dates = ['collectTime', 'raiseTime']
            for date in dates:
                if date in line.keys():
                    utc_date = pyhfs.Client.from_timestamp(line[date])
                    line[date] = utc_date.astimezone(timezone)
            yield line

    return pd.DataFrame(iterate(data))


def descriptions():
    '''
    Gets a dict mapping column name to its description.
    '''
    return {
        'plantCode': 'Plant unique code',
        'stationCode': 'Plant unique code',
        'plantName': 'Plant name',
        'plantAddress': 'Detailed address of the plant',
        'longitude': 'Plant longitude',
        'latitude': 'Plant latitude',
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
        'perpower_ratio': 'Specific energy (kWh/kWp)',
        'reduction_total_co2': 'CO2 emission reduction (Ton)',
        'reduction_total_coal': 'Standard coal saved (Ton)',
        'reduction_total_tree': 'Equivalent trees planted',
        'alarmName': 'Alarm name',
        'devName': 'Device name',
        'repairSuggestion': 'Solution',
        'esnCode': 'Device SN',
        'devTypeId': 'Device type ID',
        'causeId': 'Cause ID',
        'alarmCause': 'Alarm cause',
        'alarmType': 'Alarm type',
        'raiseTime': 'Alarm generation time in milliseconds',
        'alarmId': 'Alarm ID',
        'stationName': 'Plant name',
        'lev': 'Alarm severity',
        "status": 'Alarm status: 1: not processed (active)'
    }


def description(parameter: str):
    '''
    Gets description from column name.
    '''
    return descriptions().get(parameter, 'Unknown')
