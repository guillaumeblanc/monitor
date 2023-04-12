import os
import sys
import logging
import argparse
import glob
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

import src.std_utils as std_utils


def standardize(source: Path, destination: Path, config: Path):
    try:
        with open(config) as f:
            rdict = json.load(f)
    except:
        logging.fatal(
            'Failed to decode json remaping configuration: %' % config)
        raise
    else:
        for sfile in source.glob('**/*.csv'):
            logging.info('Processing latest file: %s' % sfile)
            try:
                sdata = std_utils.from_csv(sfile)
            except pd.errors.EmptyDataError:
                logging.info(
                    'Empty data for latest file: %s. Skipping update.' % sfile)
                pass
            else:
                # Select columns
                ddata = sdata[[
                    c for c in sdata.columns if c in rdict['columns'].keys()]]
                ddata = ddata.rename(columns=rdict['columns'])

                # Remap values
                for col, dict in rdict['values'].items():
                    if col in ddata:
                        dict = {int(k) if k.isdigit() else k: v for k,
                                v in dict.items()}
                        ddata = ddata.replace({col: dict})

                dfile = destination / sfile.relative_to(source)
                logging.info('Outputting file: %s' % dfile)
                std_utils.to_csv(ddata, dfile)


if __name__ == '__main__':
    '''
    Convert proprietary data to standard ones
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--source', required=True,
                        help='Source directory')
    parser.add_argument('-d', '--destination', required=True,
                        help='Destination directory')
    parser.add_argument('-c', '--config', required=True,
                        help='Column renaming configuration json file')
    parser.add_argument('-ll', '--loglevel', default='info',
                        help='Logging level. Example --loglevel debug, default=info')
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel.upper())

    standardize(Path(args.source), Path(args.destination), Path(args.config))
