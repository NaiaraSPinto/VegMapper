import subprocess
from pathlib import Path

# import boto3
import pandas as pd
import geopandas as gpd

import rasterio

country = 'peru'
state = 'ucayali'
year = 2020
t_epsg = 32718
t_res = 30

# df = pd.read_csv('aws/servir_credentials.csv')

bucket = f'servir-public/geotiffs/{country}'

shp = f'shapefiles/Ucayali/Ucayali_tiles_UTM_18S.shp'
gdf = gpd.read_file(shp)

tile_list = []
for i in gdf.index:
    if gdf['mask'][i] == 1:
        tile_list.append((gdf['h'][i], gdf['v'][i], gdf['geometry'][i]))

s1_vrt_dir = Path(f'{year}/s1_vrt')
if not s1_vrt_dir.exists():
    s1_vrt_dir.mkdir(parents=True)

# Sentinel-1 VRT
granule_list_18s = ['171_617', '171_622',
                    '98_617', '98_622', '98_627']
granule_list_19s = ['25_621', '25_626', '25_631']

for var in ['VV', 'VH', 'INC']:
    vrt = s1_vrt_dir / f'C-{var}-{year}-18s.vrt'
    tif_list = []
    for granule in granule_list_18s:
        vsi_prefix = f'/vsis3/{bucket}/sentinel_1/{year}'
        tif_list.append(f'{vsi_prefix}/{granule}/{year}_{granule}_{var}.tif')
    cmd = (f'gdalbuildvrt -overwrite '
           f'{vrt} {" ".join(tif_list)}')
    subprocess.check_call(cmd, shell=True)

    vrt = s1_vrt_dir / f'C-{var}-{year}-19s.vrt'
    tif_list = []
    for granule in granule_list_19s:
        vsi_prefix = f'/vsis3/{bucket}/sentinel_1/{year}'
        tif_list.append(f'{vsi_prefix}/{granule}/{year}_{granule}_{var}.tif')
    cmd = (f'gdalbuildvrt -overwrite '
           f'{vrt} {" ".join(tif_list)}')
    subprocess.check_call(cmd, shell=True)

    src_vrt = s1_vrt_dir / f'C-{var}-{year}-19s.vrt'
    dst_vrt = s1_vrt_dir / f'C-{var}-{year}-19s-to-18s.vrt'

    cmd = (f'gdalwarp -overwrite '
           f'-t_srs EPSG:{t_epsg} -et 0 '
           f'-tr {t_res} {t_res} -tap '
           f'-r near '
           f'-co COMPRESS=LZW '
           f'{src_vrt} {dst_vrt}')
    subprocess.check_call(cmd, shell=True)

    vrt = s1_vrt_dir / f'C-{var}-{year}.vrt'
    vrt1 = s1_vrt_dir / f'C-{var}-{year}-18s.vrt'
    vrt2 = s1_vrt_dir / f'C-{var}-{year}-19s-to-18s.vrt'
    cmd = (f'gdalbuildvrt -overwrite '
           f'{vrt} {vrt1} {vrt2}')
    subprocess.check_call(cmd, shell=True)

for tile in tile_list:
    h = tile[0]
    v = tile[1]
    g = tile[2]
    for var in ['VV', 'VH', 'INC']:
        src_vrt = s1_vrt_dir / f'C-{var}-{year}.vrt'
        dst_vrt = s1_vrt_dir / f'C-{var}-{year}-h{h}v{v}.vrt'

        cmd = (f'gdalwarp -overwrite '
               f'-t_srs EPSG:{t_epsg} -et 0 '
               f'-te {g.bounds[0]} {g.bounds[1]} {g.bounds[2]} {g.bounds[3]} '
               f'-tr {t_res} {t_res} '
               f'-r near '
               f'-co COMPRESS=LZW '
               f'{src_vrt} {dst_vrt}')
        subprocess.check_call(cmd, shell=True)

    band1 = s1_vrt_dir / f'C-VV-{year}-h{h}v{v}.vrt'
    band2 = s1_vrt_dir / f'C-VH-{year}-h{h}v{v}.vrt'
    band3 = s1_vrt_dir / f'C-INC-{year}-h{h}v{v}.vrt'
    band4 = f'/vsis3/{bucket}/alos2_mosaic/{year}/alos2_mosaic_{state}_{year}_h{h}v{v}_HH.tif'
    band5 = f'/vsis3/{bucket}/alos2_mosaic/{year}/alos2_mosaic_{state}_{year}_h{h}v{v}_HV.tif'
    band6 = f'/vsis3/{bucket}/alos2_mosaic/{year}/alos2_mosaic_{state}_{year}_h{h}v{v}_INC.tif'
    band7 = f'/vsis3/{bucket}/landsat_ndvi/{year}/landsat_ndvi_{state}_{year}_h{h}v{v}.tif'
    if year == 2020:
        band8 = f'/vsis3/{bucket}/modis_tree_cover/2019/modis_tc_{state}_2019_h{h}v{v}.tif'
    else:
        band8 = f'/vsis3/{bucket}/modis_tree_cover/{year}/modis_tc_{state}_{year}_h{h}v{v}.tif'

    vrt = f'{year}/{state}_stacks_{year}_h{h}v{v}.vrt'
    cmd = (f'gdalbuildvrt -overwrite -separate '
           f'{vrt} {band1} {band2} {band3} {band4} {band5} {band6} {band7} {band8}')
    subprocess.check_call(cmd, shell=True)

    with rasterio.open(vrt, 'r+') as dset:
        dset.descriptions = ('C-VV', 'C-VH', 'C-INC',
                             'L-HH', 'L-HV', 'L-INC',
                             'NDVI', 'TC')
