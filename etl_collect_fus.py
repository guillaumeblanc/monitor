import logging
import sys
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import pytz

import pyhfs
# from pyhfs.pyhfs.tests.mock_session import MockSession

import src.std_utils as std_utils
import src.hfs_utils as hfs_utils


def collect(output: Path, username: str, password: str, date: datetime, tz: pytz.timezone, mock: bool):
    '''
    Query all plants data from Fusion Solar Northbound interface.
    '''

    try:
        '''session=MockSession() if mock else'''
        with pyhfs.Client(pyhfs.Session(user=username, password=password)) as client:

            logging.info('Querying plants list.')
            plants = client.get_plant_list()
            logging.info('- Found ' + str(len(plants)) + ' plants:')
            [logging.info(' - ' + plant['plantName'] +
                          ' (' + plant['plantCode'] + ')') for plant in plants]
            std_utils.to_csv(pd.DataFrame(plants), output /
                             std_utils.format_filename('plants', date))

            # Extract the list of plants code
            plants_code = [plant['plantCode'] for plant in plants]

            # Realtime data
            logging.info('Querying realtime data.')
            realtime = client.get_plant_realtime_data(plants_code)
            for entry in realtime:  # Adds time information
                entry.update({'collectTime': client.to_timestamp(date)})
            logging.info('- Found ' + str(len(realtime)) + ' realtime data')
            std_utils.to_csv(hfs_utils.flatten(realtime, tz), output /
                             std_utils.format_filename('realtime', date))

            # Hourly data
            logging.info('Querying hourly data.')
            hourly = client.get_plant_hourly_data(plants_code, date)
            logging.info('- Found ' + str(len(hourly)) + ' hourly data')
            std_utils.to_csv(hfs_utils.flatten(hourly, tz), output /
                             std_utils.format_filename('hourly', date))

            # Daily data
            logging.info('Querying daily data.')
            daily = client.get_plant_daily_data(plants_code, date)
            logging.info('- Found ' + str(len(daily)) + ' daily data')
            std_utils.to_csv(hfs_utils.flatten(daily, tz), output /
                             std_utils.format_filename('daily', date))

            # Monthly data
            logging.info('Querying monthly data.')
            monthly = client.get_plant_monthly_data(plants_code, date)
            logging.info('- Found ' + str(len(monthly)) + ' monthly data')
            std_utils.to_csv(hfs_utils.flatten(monthly, tz), output /
                             std_utils.format_filename('monthly', date))

            # Yearly data
            logging.info('Querying yearly data.')
            yearly = client.get_plant_yearly_data(plants_code, date)
            logging.info('- Found ' + str(len(yearly)) + ' yearly data')
            std_utils.to_csv(hfs_utils.flatten(yearly, tz), output /
                             std_utils.format_filename('yearly', date))

            # Alarms data
            logging.info('Querying alarms data.')
            alarms = client.get_alarms_list(
                plants_code, datetime(2000, 1, 1), date)
            logging.info('- Found ' + str(len(alarms)) + ' alarms')
            std_utils.to_csv(hfs_utils.flatten(alarms, tz), output /
                             std_utils.format_filename('alarms', date))

    except pyhfs.LoginFailed:
        sys.exit(
            'Login failed. Verify user and password for FusionSolar Northbound interface account.')
    except pyhfs.FrequencyLimit:
        sys.exit('FusionSolar Northbound interface access frequency is too high.')
    except pyhfs.Permission:
        sys.exit('Missing permission to access FusionSolar Northbound interface.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--username', required=True,
                        help='FusionSolar Northbound interface user name')
    parser.add_argument('-p', '--password', required=True,
                        help='FusionSolar Northbound interface password')
    parser.add_argument('-o', '--output', default='out',
                        help='Output directory. Example --output your_path, default=out')
    parser.add_argument('-d', '--date', default='',
                        help='Collection date with format d/m/Y. Example --date 07/07/2023, default to now')
    parser.add_argument('-m', '--mock', default=False, action=argparse.BooleanOptionalAction,
                        help='Mock fusion solar data')
    parser.add_argument('-t', '--timezone',
                        default='Europe/Paris', help='Timezone')
    parser.add_argument('-ll', '--loglevel', default='info',
                        help='Logging level. Example --loglevel debug, default=info')
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel.upper())

    date = datetime.strptime(
        args.date, '%d/%m/%Y') if args.date else datetime.now(tz=timezone.utc)

    collect(output=Path(args.output), username=args.username,
            password=args.password, date=date, tz=pytz.timezone(args.timezone), mock=args.mock)
