import std_utils
import sys
import logging
import argparse
import glob
import json
import pandas as pd
from pathlib import Path

logging.basicConfig(level='INFO')

root = Path('data/')
to_drop = ['perpower_ratio', 'installed_capacity']

for pattern in std_utils.file_patterns():
    search_pattern = '**/' + pattern + '*.csv'

    logging.info('Processing files with pattern: %s.' % search_pattern)

    for filename in root.glob(search_pattern):
        logging.info('Processing file: %s.' % filename)

        try:
            df = std_utils.from_csv(filename)
        except pd.errors.EmptyDataError:
            pass
        else:
            df.drop(to_drop, axis=1, errors='ignore', inplace=True)

            logging.info('Outputting file: %s.' % filename)
            std_utils.to_csv(df, filename)
