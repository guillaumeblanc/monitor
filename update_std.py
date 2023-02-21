import os
import sys
import logging
import argparse
import glob
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from extern.fusnic import fusnic
import src.std_utils as std_utils


def update(previous: Path, latest: Path, updated: Path):
    '''
    Update (ie add new rows) all files based on their name.
    '''
    for lfile in latest.glob('*.csv'):
        try:
            udata = ldata = std_utils.from_csv(lfile)
        except pd.errors.EmptyDataError:
            logging.info(
                'Empty data for latest file: %s. Skipping update.' % lfile)
            pass
        else:
            pfile = previous / lfile.name
            try:
                pdata = std_utils.from_csv(pfile)
            except (pd.errors.EmptyDataError, FileNotFoundError):
                logging.info(
                    'No/empty data for previous file: %s. Using latest.' % pfile)
                pass
            else:
                # Updates with new lines (only)
                udata = pd.concat([ldata, pdata]).drop_duplicates()

            ufile = updated / lfile.name
            logging.info('Updating file: %s' % ufile)
            std_utils.to_csv(udata, ufile)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--previous', required=True,
                        help='Previous data')
    parser.add_argument('-l', '--latest', required=True,
                        help='Latest data')
    parser.add_argument('-u', '--updated', required=True,
                        help='Updated data')
    parser.add_argument('-ll', '--loglevel', default='info',
                        help='Logging level. Example --loglevel debug, default=info')
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel.upper())

    update(Path(args.previous), Path(args.latest), Path(args.updated))
