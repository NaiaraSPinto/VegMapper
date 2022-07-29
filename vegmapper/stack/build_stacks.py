#!/usr/bin/env python

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

import geopandas as gpd
import rasterio
from urllib.parse import urlparse

t_res = 30
s1_pf_pattern = re.compile(r'^\d+_\d+$', re.ASCII)


def build_stacks(storage, proj_dir, vsi_path, tiles, year, sitename=None):
    # If sitename not specified, use proj_dir basename as sitename
    if sitename is None:
        sitename = f'{proj_dir}'.split('/')[-1]

    # Make temporary directories to store VRT files
    vrt_dir = Path('tmp_vrt')
    if not vrt_dir.exists():
        vrt_dir.mkdir()

    # List available Sentinel-1 granules (path_frame)
    available_granules = []
    if storage in ['s3', 'gs']:
        ls_cmd = f'gsutil ls {proj_dir}/sentinel_1/{year}'
        obj_list = subprocess.check_output(ls_cmd, shell=True).decode(sys.stdout.encoding).splitlines()
        for obj_path in obj_list:
            obj_name = Path(obj_path).name
            if obj_path[-1] == '/' and s1_pf_pattern.fullmatch(obj_name):
                available_granules.append(obj_name)
    elif storage == 'local':
        for p in (proj_dir / f'sentinel_1/{year}').iterdir():
            if s1_pf_pattern.fullmatch(p.name):
                available_granules.append(p.name)

    gdf_tiles = gpd.read_file(tiles)
    t_epsg = gdf_tiles.crs.to_epsg()

    # Group granules by EPSG codes (UTM zones)
    granules = {}
    for granule in available_granules:
        vv_tif = f'{proj_dir}/sentinel_1/{year}/{granule}/{year}_{granule}_VV_mean.tif'
        with rasterio.open(vv_tif) as dset:
            epsg = dset.crs.to_epsg()
        if epsg in granules.keys():
            granules[epsg].append(granule)
        else:
            granules[epsg] = [granule]

    for var in ['VV', 'VH', 'INC']:
        vrt_list = []
        for epsg, granule_list in granules.items():
            # Make VRT for each EPSG (UTM zone)
            vrt = vrt_dir / f'C-{var}-{year}-{epsg}.vrt'
            tif_list = []
            for granule in granule_list:
                tif = f'{vsi_path}/sentinel_1/{year}/{granule}/{year}_{granule}_{var}_mean.tif'
                tif_list.append(tif)
            cmd = (f'gdalbuildvrt -overwrite '
                f'{vrt} {" ".join(tif_list)}')
            subprocess.check_call(cmd, shell=True)

            # Virtually warp all VRT into target UTM projection (t_epsg)
            if epsg == t_epsg:
                vrt_list.append(str(vrt))
            else:
                vrt_t_epsg = vrt_dir / f'C-{var}-{year}-{epsg}-to-{t_epsg}.vrt'
                cmd = (f'gdalwarp -overwrite '
                       f'-t_srs EPSG:{t_epsg} -et 0 '
                       f'-tr {t_res} {t_res} -tap '
                       f'-dstnodata nan '
                       f'-r near '
                       f'-co COMPRESS=LZW '
                       f'{vrt} {vrt_t_epsg}')
                subprocess.check_call(cmd, shell=True)
                vrt_list.append(str(vrt_t_epsg))

        vrt = vrt_dir / f'C-{var}-{year}.vrt'
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

        ############################ Sentinel-1 VRT ############################
        for var in ['VV', 'VH', 'INC']:
            src_vrt = vrt_dir / f'C-{var}-{year}.vrt'
            dst_vrt = vrt_dir / f'C-{var}-{year}-h{h}v{v}.vrt'
            cmd = (f'gdalwarp -overwrite '
                   f'-t_srs EPSG:{t_epsg} -et 0 '
                   f'-te {g.bounds[0]} {g.bounds[1]} {g.bounds[2]} {g.bounds[3]} '
                   f'-tr {t_res} {t_res} '
                   f'-dstnodata nan '
                   f'-r near '
                   f'-co COMPRESS=LZW '
                   f'{src_vrt} {dst_vrt}')
            subprocess.check_call(cmd, shell=True)

        ############################## ALOS-2 VRT ##############################
        for var in ['HH', 'HV', 'INC']:
            src_vrt = f'{vsi_path}/alos2_mosaic/{year}/alos2_mosaic_{year}_{var}.vrt'
            dst_vrt = vrt_dir / f'L-{var}-{year}-h{h}v{v}.vrt'
            cmd = (f'gdalwarp -overwrite '
                   f'-t_srs EPSG:{t_epsg} -et 0 '
                   f'-te {g.bounds[0]} {g.bounds[1]} {g.bounds[2]} {g.bounds[3]} '
                   f'-tr {t_res} {t_res} '
                   f'-ot Float32 -wt Float32 '
                   f'-dstnodata nan '
                   f'-r average '
                   f'-co COMPRESS=LZW '
                   f'{src_vrt} {dst_vrt}')
            subprocess.check_call(cmd, shell=True)

        band1 = vrt_dir / f'C-VV-{year}-h{h}v{v}.vrt'
        band2 = vrt_dir / f'C-VH-{year}-h{h}v{v}.vrt'
        band3 = vrt_dir / f'C-INC-{year}-h{h}v{v}.vrt'
        band4 = vrt_dir / f'L-HH-{year}-h{h}v{v}.vrt'
        band5 = vrt_dir / f'L-HV-{year}-h{h}v{v}.vrt'
        band6 = vrt_dir / f'L-INC-{year}-h{h}v{v}.vrt'
        band7 = f'{vsi_path}/landsat_ndvi/{year}/landsat_ndvi_{sitename}_{year}_h{h}v{v}.tif'
        if year == 2020:
            band8 = f'{vsi_path}/modis_tree_cover/2019/modis_tc_{sitename}_2019_h{h}v{v}.tif'
        else:
            band8 = f'{vsi_path}/modis_tree_cover/{year}/modis_tc_{sitename}_{year}_h{h}v{v}.tif'

        vrt = vrt_dir / f'{sitename}_stacks_{year}_h{h}v{v}.vrt'
        cmd = (f'gdalbuildvrt -overwrite -separate '
               f'{vrt} {band1} {band2} {band3} {band4} {band5} {band6} {band7} {band8}')
        subprocess.check_call(cmd, shell=True)

        with rasterio.open(vrt, 'r+') as dset:
            dset.descriptions = ('C-VV', 'C-VH', 'C-INC',
                                 'L-HH', 'L-HV', 'L-INC',
                                 'NDVI', 'TC')

        print(f'Making stack tif for h{h}v{v} ...')
        stack_tif = Path(f'{sitename}_stacks_{year}_h{h}v{v}.tif')
        cmd = (f'gdalwarp '
               f'-overwrite '
               f'-dstnodata -9999 '
               f'-ot Float32 '
               f'-of COG '
               f'-co COMPRESS=LZW '
               f'-co RESAMPLING=NEAREST '
               f'{vrt} {stack_tif}')
        subprocess.check_call(cmd, shell=True)

        if isinstance(proj_dir, Path):
            dst_tif = proj_dir / f'stacks/{year}/all-bands/{stack_tif}'
            if not dst_tif.parent.exists():
                dst_tif.parent.mkdir()
            stack_tif.rename(dst_tif)
        elif isinstance(proj_dir, str):
            dst_tif = f'{proj_dir}/stacks/{year}/all-bands/{stack_tif}'
            cmd = (f'gsutil cp {stack_tif} {dst_tif}')
            subprocess.check_call(cmd, shell=True)
            stack_tif.unlink()

    shutil.rmtree(vrt_dir)


def main():
    parser = argparse.ArgumentParser(
        description=('building 8-band stacks that include C-VV, C-VH, C-INC, '
                     'L-HH, L-HV, L-INC, NDVI, TC')
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

    build_stacks(storage, proj_dir, vsi_path, args.tiles, args.year, args.sitename)


if __name__ == '__main__':
    main()