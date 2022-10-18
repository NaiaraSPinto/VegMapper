#!/usr/bin/env python

import json
import subprocess
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio

from vegmapper import pathurl


# GeoTIFF suffix of data layer in the RTC product
layer_suffix = {
    'VV': 'VV',
    'VH': 'VH',
    'INC': 'inc_map',
    'LS': 'ls_map',
}

# Expect this from user before processing
# s1_proc = {
#     's1_dir': proj_dir / 'Sentinel-1',
#     'start_date': '2021-01-01',
#     'end_date': '2021-12-31',
#     'platform': 'S1B'
#     'frames': None,
# }


def get_rtc_products(s1_proc):
    s1_dir = s1_proc['s1_dir']
    start_date = s1_proc['start_date']
    end_date = s1_proc['end_date']

    # Read rtc_products.csv
    rtc_products_file = s1_dir / 'rtc_products.csv'
    pathurl.copy(rtc_products_file, '.', overwrite=True)
    df_products = pd.read_csv('rtc_products.csv')
    Path('rtc_products.csv').unlink()

    # Filter out files outside of date range
    dt_start = datetime.strptime(f'{start_date}T00:00:00+00:00', '%Y-%m-%dT%H:%M:%S%z')
    dt_end = datetime.strptime(f'{end_date}T00:00:00+00:00', '%Y-%m-%dT%H:%M:%S%z')
    df_products['startTime']= pd.to_datetime(df_products['startTime'], utc=True)
    df_products['stopTime']= pd.to_datetime(df_products['stopTime'], utc=True)
    df_products = df_products[(dt_start < df_products.startTime) & (df_products.startTime < dt_end)]

    # Filter out files based on platform
    if 'platform' in s1_proc.keys():
        df_products = df_products[df_products.filename.str[0:3] == s1_proc['platform']]

    # Get dictionary of vsi_paths of all RTC products
    # {frame: {'paths': [vsi_paths]}
    if s1_dir.is_cloud:
        vsi_prefix = f'/vsizip/vsi{s1_dir.storage}/{s1_dir.bucket}/{s1_dir.prefix}'
    else:
        vsi_prefix = f'/vsizip/{s1_dir}'
    gb = df_products.groupby(['pathNumber', 'frameNumber'])
    product_paths = {
        f'{p}_{f}': {'rtc_products': [f'{vsi_prefix}/{p}_{f}/{filename}'for filename in gb.get_group((p, f)).filename.to_list()]} for p, f in gb.groups.keys()
    }

    # Update s1_proc
    if s1_proc['frames'] is not None:
        frames = s1_proc['frames']
        s1_proc['frames'] = {}
        if isinstance(frames, str):
            frames = [frames]
        s1_proc['frames'] = {frame_id: product_paths[frame_id] for frame_id in frames}
    else:
        s1_proc['frames'] = product_paths
    # with open(f'{s1_dir}/s1_proc.json', 'w') as f:
    #     json.dump(s1_proc, f)


def build_vrt(s1_proc, layers=['VV', 'VH', 'INC', 'LS'], quiet=True):
    s1_dir = s1_proc['s1_dir']
    start_date = s1_proc['start_date']
    end_date = s1_proc['end_date']

    for frame_id, frame_dict in s1_proc['frames'].items():
        print(f'Building VRTs for {frame_id}')
        for layer in layers:
            # Get vsi paths of RTC products
            tif_list = []
            for vsi_path in frame_dict['rtc_products']:
                zip_stem = Path(vsi_path).stem
                tif_list.append(f'{vsi_path}/{zip_stem}/{zip_stem}_{layer_suffix[layer]}.tif')
            # Build VRT
            if s1_dir.is_cloud:
                vrt = f'/vsi{s1_dir.storage}/{s1_dir.bucket}/{s1_dir.prefix}/{frame_id}/{start_date}_{end_date}/{layer}.vrt'
            else:
                vrt = Path(f'{s1_dir}/{frame_id}/{start_date}_{end_date}/{layer}.vrt')
                if not vrt.parent.exists():
                    vrt.parent.mkdir(parents=True)
            cmd = f'gdalbuildvrt -overwrite {vrt} {" ".join(tif_list)}'
            if quiet:
                cmd = cmd + ' -q'
            subprocess.check_call(cmd, shell=True)
            # Update s1_proc
            s1_proc['frames'][frame_id][layer] = {'vrt': vrt}

    # with open(f'{s1_dir}/s1_proc.json', 'w') as f:
    #     json.dump(s1_proc, f)


def calc_temporal_mean(s1_proc, layers=['VV', 'VH', 'INC', 'LS'], quiet=True):
    s1_dir = s1_proc['s1_dir']
    start_date = s1_proc['start_date']
    end_date = s1_proc['end_date']

    for frame_id, frame_dict in s1_proc['frames'].items():
        print(f'Calculating temporal means for {frame_id}')
        for layer in layers:
            # Calculate temporal mean
            vrt = frame_dict[layer]['vrt']
            subprocess.check_call(f'calc_vrt_stats.py {vrt} mean', shell=True)
            # Update s1_proc
            frame_dict[layer]['mean'] = s1_dir / frame_id / f'{start_date}_{end_date}' / f'{layer}_mean.tif'

    # with open(f'{s1_dir}/s1_proc.json', 'w') as f:
    #     json.dump(s1_proc, f)


def remove_edges(s1_proc, edge_depth=200):
    for frame_id, frame_dict in s1_proc['frames'].items():
        print(f'Removing {edge_depth} edge pixels on the left and right for {frame_id}')
        vv = frame_dict['VV']['mean'].path
        vv_edge_removed = Path('VV_mean.tif')
        ls = frame_dict['LS']['mean'].path
        subprocess.check_call((f'remove_edges.py {vv} {vv_edge_removed} '
                               f'--maskfile {ls} '
                               f'--lr_only --edge_depth {edge_depth}'),
                               shell=True)
        with rasterio.open(vv_edge_removed) as dset:
            mask = dset.read_masks(1)
        for layer in ['VV', 'VH', 'INC']:
            tif = frame_dict[layer]['mean'].path
            tif_edge_removed = Path(f'{layer}_mean.tif')
            if layer != 'VV':
                with rasterio.open(tif) as dset:
                    data = dset.read(1)
                    if layer == 'INC':
                        data = np.rad2deg(data)
                    data[mask == 0] = dset.nodata
                    profile = dset.profile
                with rasterio.open(tif_edge_removed, 'w', **profile) as dset:
                    dset.write(data, 1)
            pathurl.copy(tif_edge_removed, tif, overwrite=True)
            tif_edge_removed.unlink()


def warp_to_tiles(s1_proc, tiles):
    s1_dir = s1_proc['s1_dir']
    start_date = s1_proc['start_date']
    end_date = s1_proc['end_date']
    vrt_dir = s1_dir / 'vrt' / f'{start_date}_{end_date}'
    if not vrt_dir.exists() and vrt_dir.is_local:
        vrt_dir.path.mkdir(parents=True)

    gdf_tiles = gpd.read_file(tiles)
    t_epsg = gdf_tiles.crs.to_epsg()

    # Group frames by EPSG codes (UTM zones)
    frames_by_epsg = {}
    for frame_id, frame_dict in s1_proc['frames'].items():
        vv = frame_dict['VV']['mean'].path
        with rasterio.open(vv) as dset:
            epsg = dset.crs.to_epsg()
        if epsg in frames_by_epsg.keys():
            frames_by_epsg[epsg].append(frame_id)
        else:
            frames_by_epsg[epsg] = [frame_id]

    for layer in ['VV', 'VH', 'INC']:
        vrt_list = []
        for epsg, frames in frames_by_epsg.items():
            # Make VRT for each EPSG (UTM zone)
            vrt = vrt_dir / f'C-{layer}-{epsg}.vrt'
            tif_list = []
            for frame_id in frames:
                tif = f"{s1_proc['frames'][frame_id][layer]['mean'].path}"
                tif_list.append(tif)
            cmd = f'gdalbuildvrt -overwrite {vrt} {" ".join(tif_list)}'
            subprocess.check_call(cmd, shell=True)

            # Virtually warp all VRT into target UTM projection (t_epsg)
            if epsg == t_epsg:
                vrt_list.append(str(vrt))
            else:
                vrt_t_epsg = vrt_dir / f'C-{layer}-{epsg}-to-{t_epsg}.vrt'
                cmd = (f'gdalwarp -overwrite '
                       f'-t_srs EPSG:{t_epsg} -et 0 '
                       f'-tr 30 30 -tap '
                       f'-dstnodata nan '
                       f'-r near '
                       f'-co COMPRESS=LZW '
                       f'{vrt} {vrt_t_epsg}')
                subprocess.check_call(cmd, shell=True)
                vrt_list.append(str(vrt_t_epsg))

        vrt = vrt_dir / f'C-{layer}.vrt'
        cmd = (f'gdalbuildvrt -overwrite '
               f'{vrt} {" ".join(vrt_list)}')
        subprocess.check_call(cmd, shell=True)

        for i in gdf_tiles.index:
            h = gdf_tiles['h'][i]
            v = gdf_tiles['v'][i]
            m = gdf_tiles['mask'][i]
            g = gdf_tiles['geometry'][i]

            if m == 0:
                continue

            # Warp to reference tiles
            src_vrt = vrt_dir / f'C-{layer}.vrt'
            dst_vrt = vrt_dir / f'C-{layer}-h{h}v{v}.vrt'
            cmd = (f'gdalwarp -overwrite '
                f'-t_srs EPSG:{t_epsg} -et 0 '
                f'-te {g.bounds[0]} {g.bounds[1]} {g.bounds[2]} {g.bounds[3]} '
                f'-tr 30 30 '
                f'-dstnodata nan '
                f'-r near '
                f'-co COMPRESS=LZW '
                f'{src_vrt} {dst_vrt}')
            subprocess.check_call(cmd, shell=True)
