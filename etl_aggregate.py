import os
import sys
import logging
import argparse
import glob
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from extern.fusnic import fusnic
import src.std_utils as std_utils


def aggregate(source: Path, destination: Path):
    for pattern in std_utils.file_patterns():
        search_pattern = '**/' + pattern + '_*.csv'

        logging.info('Aggregating files with pattern: %s.' % search_pattern)
        aggregated = std_utils.from_csvs(source, search_pattern)
        if aggregated.empty:
            logging.info('No data for pattern: %s.' % search_pattern)
            continue
        aggregated.sort_values(by=['collect_time', 'plant_code'], inplace=True)

        dfile = destination / (pattern + '.csv')
        logging.info('Outputting file: %s.' % dfile)
        std_utils.to_csv(aggregated, dfile)


if __name__ == '__main__':
    '''
    Aggregate all data, based on their name.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--source', required=True,
                        help='Source directory')
    parser.add_argument('-d', '--destination', required=True,
                        help='Destination directory')
    parser.add_argument('-ll', '--loglevel', default='info',
                        help='Logging level. Example --loglevel debug, default=info')
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel.upper())

    aggregate(Path(args.source), Path(args.destination))
