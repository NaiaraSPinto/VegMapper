#!/usr/bin/env python

import argparse
import pandas as pd
import geopandas as gpd
from pathlib import Path

supported_metadata_formats = ['.csv', '.geojson']

def generate_granules_group_dict(metadata):
    """
    Return the granules dictionary
    """
    if metadata.suffix == '.csv':
        granules_df = pd.read_csv(metadata)
        col_date = 'Acquisition Date'
        col_path = 'Path Number'
        col_frame = 'Frame Number'
        col_granule = 'Granule Name'
    elif metadata.suffix == '.geojson':
        granules_df = gpd.read_file(metadata)
        col_date = 'stopTime'
        col_path = 'pathNumber'
        col_frame = 'frameNumber'
        col_granule = 'sceneName'
    else:
        raise Exception(f'Metadata file format ({metadata.suffix}) not supported')

    granules_df['Year'] = granules_df[col_date].apply(lambda x: x.split('-')[0])
    granules_df = granules_df.filter([col_granule, 'Year', col_path, col_frame])
    granules_df['year_path_frame'] = granules_df.apply(lambda row: f"{row['Year']}_{row[col_path]}_{row[col_frame]}", axis=1)

    granules_groups = granules_df.groupby(by=['year_path_frame'])[col_granule]

    # Create a dictionary for granule groups
    # Key: year_path_frame
    # Value: list of granule names
    granules_group_dict = {
        key : granules_groups.get_group(x).to_list()
        for key, x in zip(granules_groups.indices, granules_groups.groups)
    }

    return granules_group_dict

def main():
    # Setup argument parsing
    parser = argparse.ArgumentParser(
        description='display summary information about metadata from ASF vertex')
    parser.add_argument('metadata', metavar='csv/geojson',
                        type=Path,
                        help='metadata file downloaded from ASF Vertex after data search')

    args = parser.parse_args()

        # Check metadata
    if not args.metadata.exists():
        raise Exception(f'Metadata file {args.metadata} does not exist')
    if args.metadata.suffix not in supported_metadata_formats:
        raise Exception(f'Metadata file format ({args.metadata.suffix}) not supported')

    granules_dict = generate_granules_group_dict(args.metadata)
    print(f"\n{args.metadata} contains the following granules:\n")
    for year_path_frame in granules_dict:
        year, path, frame = year_path_frame.split("_")
        print(f"Year {year}, Path {path}, Frame {frame}:")
        for granule in granules_dict[year_path_frame]:
            print(f"  - {granule}")


if __name__ == "__main__":
    main()