#!/usr/bin/env python

import argparse
import pandas as pd
import geopandas as gpd
from pathlib import Path

metadata_formats = ['.csv', '.geojson']
metadata_columns = {
    'csv': {
        'time': 'Acquisition Date',
        'path': 'Path Number',
        'frame': 'Frame Number',
        'granule': 'Granule Name',
    },
    'geojson': {
        'time': 'stopTime',
        'path': 'pathNumber',
        'frame': 'frameNumber',
        'granule': 'sceneName',
    }
}


def get_granule_dict(metadata):
    """
    Return a granule dictionary
    """
    if Path(metadata).suffix not in metadata_formats:
        raise Exception(f'Metadata file format ({Path(metadata).suffix}) not supported')

    if Path(metadata).suffix == '.csv':
        df = pd.read_csv(metadata)
        cols = metadata_columns['csv']
    elif Path(metadata).suffix == '.geojson':
        df = gpd.read_file(metadata)
        cols = metadata_columns['geojson']

    df[cols['time']] = pd.DatetimeIndex(df[cols['time']])
    df[cols['path']] = df[cols['path']].astype(int)
    df[cols['frame']] = df[cols['frame']].astype(int)
    df['year'] = pd.DatetimeIndex(df[cols['time']]).year

    df = df.sort_values(by=[cols['path'], cols['frame'], cols['time']])

    gb = df.groupby(by=['year', cols['path'], cols['frame']])

    # Create a dictionary for granule groups
    # Key: (year, path, frame)
    # Value: list of granule names
    granule_dict = {
        key : gb.get_group(key)[cols['granule']].to_list()
        for key in gb.groups.keys()
    }

    return granule_dict


def print_granule_summary(metadata):
    granule_dict = get_granule_dict(metadata)
    print(f"\n{metadata} contains the following granules:\n")
    for key, granule_list in granule_dict.items():
        year, path, frame = key
        num_granules = len(granule_list)
        print(f"Year {year}, Path {path}, Frame {frame} ({num_granules} granules):")
        for granule in granule_list:
            print(f"  - {granule}")


def main():
    parser = argparse.ArgumentParser(
        description='Display summary of ASF Vertex data search results'
    )
    parser.add_argument('metadata', metavar='asf_metadata', type=str,
                        help=('Metadata (csv/geojson) of data search results'))
    args = parser.parse_args()

    print_granule_summary(args.metadata)


if __name__ == "__main__":
    main()