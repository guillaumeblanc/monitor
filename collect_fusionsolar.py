import os
import sys
import logging
import argparse
from datetime import datetime, timezone
import pandas
import pathlib

# Relies on fusnic package for FusionSolar accesses
from extern.fusnic import fusnic

# from extern.fusnic.fusnic.tests.mock_session import MockSession


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


def to_csv(data: list, path: pathlib.Path):
    '''
    Dumps list of entries to csv.
    '''
    df = pandas.DataFrame(data)
    df.to_csv(path, index=False)


def collect(output: pathlib.Path, username: str, password: str):

    now = datetime.now(tz=timezone.utc)

    # Setup output directory
    logging.info('Output directory: ' + str(output))
    output.mkdir(parents=True, exist_ok=True)

    try:
        with fusnic.ClientSession(user=username, password=password) as client:
            # with fusnic.Client(session=MockSession()) as client:

            logging.info('Querying plants list.')
            plants = client.get_plant_list()
            logging.info('- Found ' + str(len(plants)) + ' plants:')
            [logging.info(' - ' + plant['stationName'] +
                          ' (' + plant['stationCode'] + ')') for plant in plants]
            to_csv(plants, output / ('plants_' +
                                     now.strftime('%Y-%m') + '.csv'))

            # Extract the list of plants code
            plants_code = [plant['stationCode'] for plant in plants]

            # Hourly data
            logging.info('Querying hourly data.')
            hourly = client.get_plant_hourly_data(plants_code, now)
            logging.info('- Found ' + str(len(hourly)) + ' hourly data')
            to_csv(flatten(hourly), output /
                   ('hourly_' + now.strftime('%Y-%m-%d') + '.csv'))

            # Daily data
            logging.info('Querying daily data.')
            daily = client.get_plant_daily_data(plants_code, now)
            logging.info('- Found ' + str(len(daily)) + ' daily data')
            to_csv(flatten(daily), output /
                   ('daily_' + now.strftime('%Y-%m') + '.csv'))

            # Monthly data
            logging.info('Querying monthly data.')
            monthly = client.get_plant_monthly_data(plants_code, now)
            logging.info('- Found ' + str(len(monthly)) + ' monthly data')
            to_csv(flatten(monthly), output /
                   ('monthly_' + now.strftime('%Y') + '.csv'))

            # Yearly data
            logging.info('Querying yearly data.')
            yearly = client.get_plant_yearly_data(plants_code, now)
            logging.info('- Found ' + str(len(yearly)) + ' yearly data')
            to_csv(flatten(yearly), output / 'yearly.csv')

    except fusnic.LoginFailed:
        sys.exit(
            'Login failed. Verify user and password for FusionSolar Northbound interface account.')
    except fusnic.FrequencyLimit:
        sys.exit('FusionSolar Northbound interface access frequency is too high.')
    except fusnic.Permission:
        sys.exit('Missing permission to access FusionSolar Northbound interface.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--username', required=True,
                        help='FusionSolar Northbound interface user name')
    parser.add_argument('-p', '--password', required=True,
                        help='FusionSolar Northbound interface password')
    parser.add_argument('-l', '--loglevel', default='info',
                        help='Logging level. Example --loglevel debug, default=info')
    parser.add_argument('-o', '--output', default='out',
                        help='Output directory. Example --output your_path, default=out')
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel.upper())

    collect(output=pathlib.Path(args.output),
            username=args.username, password=args.password)
