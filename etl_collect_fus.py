import logging
import sys
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import pytz

from extern.fusnic import fusnic
from extern.fusnic.fusnic.tests.mock_session import MockSession

import src.std_utils as std_utils
import src.fus_utils as fus_utils


def collect(output: Path, username: str, password: str, tz: pytz.timezone, mock: bool):
    '''
    Query all plants data from Fusion Solar Northbound interface.
    '''
    now = datetime.now(tz=timezone.utc)

    try:
        with fusnic.Client(session=MockSession() if mock else fusnic.Session(user=username, password=password)) as client:

            logging.info('Querying plants list.')
            plants = client.get_plant_list()
            logging.info('- Found ' + str(len(plants)) + ' plants:')
            [logging.info(' - ' + plant['plantName'] +
                          ' (' + plant['plantCode'] + ')') for plant in plants]
            std_utils.to_csv(pd.DataFrame(plants), output /
                             std_utils.format_filename('plants', now))

            # Extract the list of plants code
            plants_code = [plant['plantCode'] for plant in plants]

            # Realtime data
            logging.info('Querying realtime data.')
            realtime = client.get_plant_realtime_data(plants_code)
            for entry in realtime:  # Adds time information
                entry.update({'collectTime': client.to_timestamp(now)})
            logging.info('- Found ' + str(len(realtime)) + ' realtime data')
            std_utils.to_csv(fus_utils.flatten(realtime, tz), output /
                             std_utils.format_filename('realtime', now))

            # Hourly data
            logging.info('Querying hourly data.')
            hourly = client.get_plant_hourly_data(plants_code, now)
            logging.info('- Found ' + str(len(hourly)) + ' hourly data')
            std_utils.to_csv(fus_utils.flatten(hourly, tz), output /
                             std_utils.format_filename('hourly', now))

            # Daily data
            logging.info('Querying daily data.')
            daily = client.get_plant_daily_data(plants_code, now)
            logging.info('- Found ' + str(len(daily)) + ' daily data')
            std_utils.to_csv(fus_utils.flatten(daily, tz), output /
                             std_utils.format_filename('daily', now))

            # Monthly data
            logging.info('Querying monthly data.')
            monthly = client.get_plant_monthly_data(plants_code, now)
            logging.info('- Found ' + str(len(monthly)) + ' monthly data')
            std_utils.to_csv(fus_utils.flatten(monthly, tz), output /
                             std_utils.format_filename('monthly', now))

            # Yearly data
            logging.info('Querying yearly data.')
            yearly = client.get_plant_yearly_data(plants_code, now)
            logging.info('- Found ' + str(len(yearly)) + ' yearly data')
            std_utils.to_csv(fus_utils.flatten(yearly, tz), output /
                             std_utils.format_filename('yearly', now))

            # Alarms data
            logging.info('Querying alarms data.')
            alarms = client.get_alarms_list(
                plants_code, datetime(2000, 1, 1), now)
            logging.info('- Found ' + str(len(alarms)) + ' alarms')
            std_utils.to_csv(fus_utils.flatten(alarms, tz), output /
                             std_utils.format_filename('alarms', now))

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
    parser.add_argument('-o', '--output', default='out',
                        help='Output directory. Example --output your_path, default=out')
    parser.add_argument('-m', '--mock', default=False, action=argparse.BooleanOptionalAction,
                        help='Mock fusion solar data')
    parser.add_argument('-t', '--timezone',
                        default='Europe/Paris', help='Timezone')
    parser.add_argument('-ll', '--loglevel', default='info',
                        help='Logging level. Example --loglevel debug, default=info')
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel.upper())

    collect(output=Path(args.output), username=args.username,
            password=args.password, tz=pytz.timezone(args.timezone), mock=args.mock)
