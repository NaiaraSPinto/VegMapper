#!/usr/bin/env python

import subprocess
from pathlib import Path

import geopandas as gpd
import rasterio


def build_stack(stack_name, stack_dir, bands, tiles):
    # Directory for VRTs of stacks
    stack_dir = Path(stack_dir)
    vrt_dir = stack_dir / 'vrt'
    if not vrt_dir.exists():
        vrt_dir.mkdir(parents=True)

    gdf_tiles = gpd.read_file(tiles)
    for _, row in gdf_tiles.iterrows():
        h = row['h']
        v = row['v']
        m = row['mask']

        if m == 0:
            # Skip unused tiles
            continue

        band_data_list = []
        band_name_list = []
        band_num = len(bands.keys())
        for i in range(band_num):
            band = bands[f'{i+1}']
            band_name_list.append(band['name'])
            band_data_list.append(f"{band['dir']}/{band['prefix']}h{h}v{v}{band['suffix']}")

        vrt = vrt_dir / f'{stack_name}_h{h}v{v}.vrt'
        cmd = (f'gdalbuildvrt -overwrite -separate -q '
               f'{vrt} {" ".join(band_data_list)}')
        subprocess.check_call(cmd, shell=True)

        with rasterio.open(vrt, 'r+') as dset:
            dset.descriptions = band_name_list

        print(f'Making stack tif for h{h}v{v} ...')
        stack_tif = stack_dir / f'{stack_name}_h{h}v{v}.tif'
        cmd = (f'gdalwarp '
               f'-overwrite '
               f'-dstnodata -9999 '
               f'-ot Float32 '
               f'-of COG '
               f'-co COMPRESS=LZW '
               f'-co RESAMPLING=NEAREST '
               f'-multi -wo NUM_THREADS=ALL_CPUS '
               f'{vrt} {stack_tif}')
        subprocess.check_call(cmd, shell=True)
