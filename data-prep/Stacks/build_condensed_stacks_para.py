#!/usr/bin/env python

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from urllib.parse import urlparse


pixfun_name = 'rvi'
pixfun = f"""
import numpy as np
def {pixfun_name}(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    copol = in_ar[0]
    cxpol = in_ar[1]
    rvi = 4 * cxpol / (copol + cxpol)
    rvi[copol + cxpol == 0] = 0
    out_ar[:] = rvi
"""
pixfun_contents = f"""    <PixelFunctionType>{pixfun_name}</PixelFunctionType>
    <PixelFunctionLanguage>Python</PixelFunctionLanguage>
    <PixelFunctionCode><![CDATA[{pixfun}]]>
    </PixelFunctionCode>
"""


def build_rvi_vrt(stack_tif, out_vrt, bands=(1, 2)):
    # Build VRT with bands of co-pol and cross-pol
    cmd = (f'gdalbuildvrt -overwrite -b {bands[0]} -b {bands[1]} '
           f'{out_vrt} {stack_tif}')
    subprocess.check_call(cmd, shell=True)

    # Modify VRT and insert RVI pixel function
    with open(out_vrt) as f:
        lines = f.readlines()
    lines[3] = lines[3].replace('band="1"',
                                'band="1" subClass="VRTDerivedRasterBand"')
    idx = lines.index('  </VRTRasterBand>\n')
    del lines[idx:idx+3]
    lines.insert(4, pixfun_contents)
    with open(out_vrt, 'w') as f:
        f.writelines(lines)


def build_condensed_stacks(storage, proj_dir, vsi_path, tiles, year, sitename=None):
    # If sitename not specified, use proj_dir basename as sitename
    if sitename is None:
        sitename = f'{proj_dir}'.split('/')[-1]

    # Make temporary directories to store VRT files
    vrt_dir = Path('tmp_condensed_vrt')
    if not vrt_dir.exists():
        vrt_dir.mkdir()

    gdf_tiles = gpd.read_file(tiles)
    for i in gdf_tiles.index:
        h = gdf_tiles['h'][i]
        v = gdf_tiles['v'][i]
        m = gdf_tiles['mask'][i]
        g = gdf_tiles['geometry'][i]

        if m == 1:
            print(f'Building condensed stacks for h{h}v{v} ...')

            stack_tif = f'{vsi_path}/stacks/{year}/all-bands/{sitename}_stacks_{year}_h{h}v{v}.tif'

            c_rvi_vrt = vrt_dir / f'{sitename}_C-RVI_{year}_h{h}v{v}.vrt'
            build_rvi_vrt(stack_tif, c_rvi_vrt, (1, 2))

            l_rvi_vrt = vrt_dir / f'{sitename}_L-RVI_{year}_h{h}v{v}.vrt'
            build_rvi_vrt(stack_tif, l_rvi_vrt, (4, 5))

            with rasterio.open(stack_tif) as dset:
                ndvi = np.round(dset.read(7)*100).astype(np.int16)
                ndvi[dset.read_masks(7) == 0] = -9999
                tc = dset.read(8).astype(np.int16)
                tc[dset.read_masks(8) == 0] = -9999
                prodes = dset.read(9).astype(np.int16)
                prodes[dset.read_masks(9) == 0] = -9999
                profile = dset.profile
            with rasterio.Env(GDAL_VRT_ENABLE_PYTHON=True):
                with rasterio.open(c_rvi_vrt) as dset:
                    c_rvi = np.round(dset.read(1)*100).astype(np.int16)
                    c_rvi[dset.read_masks(1) == 0] = -9999
                with rasterio.open(l_rvi_vrt) as dset:
                    l_rvi = np.round(dset.read(1)*100).astype(np.int16)
                    l_rvi[dset.read_masks(1) == 0] = -9999

            profile.update(dtype=np.int16, count=5, nodata=-9999)

            cstack_tif = Path(f'{sitename}_condensed_stacks_{year}_h{h}v{v}.tif')
            with rasterio.open(cstack_tif, 'w', **profile) as dset:
                dset.write(c_rvi, 1)
                dset.write(l_rvi, 2)
                dset.write(ndvi, 3)
                dset.write(tc, 4)
                dset.write(prodes, 5)
                dset.descriptions = ('C-RVIx100', 'L-RVIx100', 'NDVIx100', 'TC', 'PRODES')

            if isinstance(proj_dir, Path):
                dst_tif = proj_dir / f'stacks/{year}/condensed/{cstack_tif}'
                if not dst_tif.parent.exists():
                    dst_tif.parent.mkdir()
                cstack_tif.rename(dst_tif)
            elif isinstance(proj_dir, str):
                dst_tif = f'{proj_dir}/stacks/{year}/condensed/{cstack_tif}'
                cmd = (f'gsutil cp {cstack_tif} {dst_tif}')
                subprocess.check_call(cmd, shell=True)
                cstack_tif.unlink()

    shutil.rmtree(vrt_dir)


def main():
    parser = argparse.ArgumentParser(
        description=('building 4-band condensed stacks that include C-RVIx100, '
                     'L-RVIx100, NDVIx100, TC')
    )
    parser.add_argument('proj_dir', metavar='proj_dir',
                        type=str,
                        help='project directory (s3:// or gs:// or local dirs)')
    parser.add_argument('tiles', metavar='tiles',
                        type=str,
                        help=('shp/geojson file that contains tiles for the '
                              'output stacks'))
    parser.add_argument('year', metavar='year',
                        type=int,
                        help='year')
    parser.add_argument('--sitename', metavar='sitename',
                        type=str,
                        help='site name')
    args = parser.parse_args()

    # Check proj_dir
    u = urlparse(args.proj_dir)
    if u.scheme in ['s3', 'gs']:
        storage = u.scheme
        bucket = u.netloc
        prefix = u.path.strip('/')
        proj_dir = f'{storage}://{bucket}/{prefix}'
        vsi_path = f'/vsi{storage}/{bucket}/{prefix}'
        subprocess.check_call(f'gsutil ls {storage}://{bucket}',
                              stdout=subprocess.DEVNULL,
                              shell=True)
    else:
        storage = 'local'
        proj_dir = Path(args.proj_dir)
        vsi_path = f'{proj_dir}'
        if not proj_dir.is_dir():
            raise Exception(f'{proj_dir} is not a valid directory path')

    build_condensed_stacks(storage, proj_dir, vsi_path, args.tiles, args.year, args.sitename)


if __name__ == '__main__':
    main()