#!/usr/bin/env python

import argparse
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

import vegmapper
from vegmapper import pathurl
from vegmapper.pathurl import ProjDir


pf_pattern = re.compile(r'^\d+_\d+$', re.ASCII)

# RTC product filename pattern
rtc_zip_pattern = re.compile(r'^\w{3}_\w{2}_\d{8}T\d{6}_\w{3}_RTC\d{2}_\w{1}_\w{6}_\w{4}.zip$')

# GeoTIFF suffix of data layer in the RTC product
layer_suffix = {
    'VV': 'VV',
    'VH': 'VH',
    'INC': 'inc_map',
    'LS': 'ls_map',
}


def build_vrt(proj_dir: ProjDir, start_date, end_date, layers=['VV', 'VH', 'INC', 'LS'], path_frame=None):
    s1_dir = proj_dir / 'Sentinel-1'

    # Read rtc_products.csv
    rtc_products_file = s1_dir / 'rtc_products.csv'
    pathurl.copy(rtc_products_file, '.', overwrite=True)
    df_products = pd.read_csv('rtc_products.csv')
    Path('rtc_products.csv').unlink()

    # Filter out files outside of date range
    dt_start = datetime.strptime(start_date, '%Y-%m-%d')
    dt_end = datetime.strptime(end_date, '%Y-%m-%d')
    df_products['startTime']= pd.to_datetime(df_products['startTime'])
    df_products['stopTime']= pd.to_datetime(df_products['stopTime'])
    df_products = df_products[(dt_start < df_products.startTime) & (df_products.startTime < dt_end)]

    # Get list of path_frame
    if s1_dir.is_cloud:
        vsi_prefix = f'/vsizip/vsi{s1_dir.storage}/{s1_dir.bucket}/{s1_dir.prefix}'
    else:
        vsi_prefix = f'/vsizip/{s1_dir}'
    gb = df_products.groupby(['pathNumber', 'frameNumber'])
    file_paths = {
        f'{p}_{f}': [f'{vsi_prefix}/{p}_{f}/{filename}'for filename in gb.get_group((p, f)).filename.to_list()] for p, f in gb.groups.keys()
    }

    if path_frame is not None:
        file_paths = {path_frame: file_paths[path_frame]}

    for path_frame, vsi_paths in file_paths.items():
        for layer in layers:
            tif_list = []
            for vsi_path in vsi_paths:
                zip_stem = Path(vsi_path).stem
                tif_list.append(f'{vsi_path}/{zip_stem}/{zip_stem}_{layer}.tif')

            # Build VRT
            if s1_dir.is_cloud:
                vrt = f'/vsi{s1_dir.storage}/{s1_dir.bucket}/{s1_dir.prefix}/{path_frame}/{start_date}_{end_date}/{layer}.vrt'
            else:
                vrt = Path(f'{s1_dir}/{path_frame}/{start_date}_{end_date}/{layer}.vrt')
                if not vrt.parent.exists():
                    vrt.parent.mkdir(parents=True)
            print(f'\nBuilding {layer} VRT for {path_frame} to {vrt}')
            cmd = f'gdalbuildvrt -overwrite {vrt} {" ".join(tif_list)}'
            subprocess.check_call(cmd, shell=True)


def s1_proc(proj_dir, year, m1, m2, path_frame=None):
    # Get list of path_frame
    path_frame_list = []
    if path_frame is not None:
        if pf_pattern.fullmatch(path_frame):
            path_frame_list.append(path_frame)
        else:
            raise Exception(f'{path_frame} is not of correct path_frame format.')
    else:
        p = ProjDir(proj_dir)
        if p.is_cloud:
            ls_cmd = f'gsutil ls {proj_dir}/sentinel_1/{year}'
            obj_list = subprocess.check_output(ls_cmd, shell=True).decode(sys.stdout.encoding).splitlines()
            for obj_path in obj_list:
                obj_name = Path(obj_path).name
                if obj_path[-1] == '/' and pf_pattern.fullmatch(obj_name):
                    path_frame_list.append(obj_name)
        else:
            for p in (proj_dir / f'sentinel_1/{year}').iterdir():
                if pf_pattern.match(p.name):
                    path_frame_list.append(p.name)

    for path_frame in path_frame_list:
        print(f'\nProcessing {year}_{path_frame} ...')

        for layer in ['VV', 'VH', 'INC', 'LS']:
            # Build VRT
            subprocess.check_call(f's1_build_vrt.py {proj_dir}/sentinel_1 {year}_{path_frame} {layer} --m1 {m1} --m2 {m2}', shell=True)
            # Calculate temporal mean
            vrtpath = f'{proj_dir}/sentinel_1/{year}/{path_frame}/{year}_{path_frame}_{layer}.vrt'
            subprocess.check_call(f'calc_vrt_stats.py {vrtpath} mean', shell=True)

        # Remove left and right edge pixels
        subprocess.check_call(f's1_remove_edges.py {proj_dir}/sentinel_1/{year}/{path_frame}', shell=True)

        print(f'\nDone processing {year}_{path_frame}.')
