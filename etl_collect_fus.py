import logging
import sys
import argparse
from pathlib import Path
from datetime import datetime, timezone

from extern.fusnic import fusnic
# from extern.fusnic.fusnic.tests.mock_session import MockSession

import src.std_utils as std_utils
import src.fus_utils as fus_utils


def collect(output: Path, username: str, password: str):
    '''
    Query all plants data from Fusion Solar Northbound interface.
    '''
    now = datetime.now(tz=timezone.utc)

    try:
        with fusnic.ClientSession(user=username, password=password) as client:
            # with fusnic.Client(session=MockSession()) as client:

            logging.info('Querying plants list.')
            plants = client.get_plant_list()
            logging.info('- Found ' + str(len(plants)) + ' plants:')
            [logging.info(' - ' + plant['stationName'] +
                          ' (' + plant['stationCode'] + ')') for plant in plants]
            std_utils.to_csv(plants, output /
                             std_utils.format_filename('plants', now))

            # Extract the list of plants code
            plants_code = [plant['stationCode'] for plant in plants]

            # Hourly data
            logging.info('Querying realtime data.')
            realtime = client.get_plant_realtime_data(plants_code)
            logging.info('- Found ' + str(len(realtime)) + ' realtime data')
            std_utils.to_csv(fus_utils.flatten(realtime), output /
                             std_utils.format_filename('realtime', now))

            # Hourly data
            logging.info('Querying hourly data.')
            hourly = client.get_plant_hourly_data(plants_code, now)
            logging.info('- Found ' + str(len(hourly)) + ' hourly data')
            std_utils.to_csv(fus_utils.flatten(hourly), output /
                             std_utils.format_filename('hourly', now))

            # Daily data
            logging.info('Querying daily data.')
            daily = client.get_plant_daily_data(plants_code, now)
            logging.info('- Found ' + str(len(daily)) + ' daily data')
            std_utils.to_csv(fus_utils.flatten(daily), output /
                             std_utils.format_filename('daily', now))

            # Monthly data
            logging.info('Querying monthly data.')
            monthly = client.get_plant_monthly_data(plants_code, now)
            logging.info('- Found ' + str(len(monthly)) + ' monthly data')
            std_utils.to_csv(fus_utils.flatten(monthly), output /
                             std_utils.format_filename('monthly', now))

            # Yearly data
            logging.info('Querying yearly data.')
            yearly = client.get_plant_yearly_data(plants_code, now)
            logging.info('- Found ' + str(len(yearly)) + ' yearly data')
            std_utils.to_csv(fus_utils.flatten(yearly), output /
                             std_utils.format_filename('yearly', now))

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
    parser.add_argument('-ll', '--loglevel', default='info',
                        help='Logging level. Example --loglevel debug, default=info')
    parser.add_argument('-o', '--output', default='out',
                        help='Output directory. Example --output your_path, default=out')
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel.upper())

    collect(output=Path(args.output),
            username=args.username, password=args.password)
