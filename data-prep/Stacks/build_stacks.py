import argparse
import subprocess
from pathlib import Path

import boto3
import geopandas as gpd

import rasterio

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('country', help='country name')
parser.add_argument('state', help='state name')
parser.add_argument('year', type=int, help='year (1900 - 2100)')
parser.add_argument('-h', '--help', action='help', help='removes the left and right edges of Sentinel-1 images')
args = parser.parse_args()

country = args.country.lower()
state = args.state.lower()
year = args.year

if year < 1900 or year > 2100:
    raise Exception('year must be a 4-digit number between 1900 and 2100')

print(f'country: {country}')
print(f'state: {state}')
print(f'year: {year}')

t_res = 30

s3 = boto3.client('s3')
bucket = f'servir-public'
prefix = f'geotiffs/{country}'

gdf_tiles = gpd.read_file(f'../AOI/{state}/{state}_tiles.geojson')
t_epsg = gdf_tiles.crs.to_epsg()

s1_vrt_dir = Path(f'{state}/{year}/s1_vrt')
if not s1_vrt_dir.exists():
    s1_vrt_dir.mkdir(parents=True)

# Sentinel-1 VRT
gdf = gpd.read_file(f'../Sentinel/granules/{state}/{state}_sentinel_granules_{year}_dry.geojson')
gdf_granules = gdf.groupby(['pathNumber', 'frameNumber']).size().reset_index()

# Group granules by EPSG codes
granules = {}
for i in gdf_granules.index:
    path = gdf_granules['pathNumber'][i]
    frame = gdf_granules['frameNumber'][i]
    granule = f'{path}_{frame}'
    tif = f'/vsis3/{bucket}/{prefix}/sentinel_1/{year}/{granule}/{year}_{granule}_VV.tif'
    with rasterio.open(tif) as dset:
        epsg = dset.crs.to_epsg()
    if epsg in granules.keys():
        granules[epsg].append(granule)
    else:
        granules[epsg] = [granule]

for var in ['VV', 'VH', 'INC']:
    vrt_list = []
    for epsg, granule_list in granules.items():
        vrt = s1_vrt_dir / f'C-{var}-{year}-{epsg}.vrt'
        tif_list = []
        for granule in granule_list:
            tif = f'/vsis3/{bucket}/{prefix}/sentinel_1/{year}/{granule}/{year}_{granule}_{var}.tif'
            tif_list.append(tif)
        cmd = (f'gdalbuildvrt -overwrite '
               f'{vrt} {" ".join(tif_list)}')
        subprocess.check_call(cmd, shell=True)
        s3.upload_file(str(vrt), bucket, f'{prefix}/vrts/{year}/s1_vrt/{vrt.name}')

        if epsg == t_epsg:
            vrt_list.append(str(vrt))
        else:
            vrt_t_epsg = s1_vrt_dir / f'C-{var}-{year}-{epsg}-to-{t_epsg}.vrt'
            cmd = (f'gdalwarp -overwrite '
                   f'-t_srs EPSG:{t_epsg} -et 0 '
                   f'-tr {t_res} {t_res} -tap '
                   f'-r near '
                   f'-co COMPRESS=LZW '
                   f'{vrt} {vrt_t_epsg}')
            subprocess.check_call(cmd, shell=True)
            s3.upload_file(str(vrt_t_epsg), bucket, f'{prefix}/vrts/{year}/s1_vrt/{vrt_t_epsg.name}')
            vrt_list.append(str(vrt_t_epsg))

    vrt = s1_vrt_dir / f'C-{var}-{year}.vrt'
    cmd = (f'gdalbuildvrt -overwrite '
           f'{vrt} {" ".join(vrt_list)}')
    subprocess.check_call(cmd, shell=True)
    s3.upload_file(str(vrt), bucket, f'{prefix}/vrts/{year}/s1_vrt/{vrt.name}')

for i in gdf_tiles.index:
    h = gdf_tiles['h'][i]
    v = gdf_tiles['v'][i]
    m = gdf_tiles['mask'][i]
    g = gdf_tiles['geometry'][i]

    if m == 0:
        continue

    for var in ['VV', 'VH', 'INC']:
        src_vrt = s1_vrt_dir / f'C-{var}-{year}.vrt'
        dst_vrt = s1_vrt_dir / f'C-{var}-{year}-h{h}v{v}.vrt'

        cmd = (f'gdalwarp -overwrite '
               f'-t_srs EPSG:{t_epsg} -et 0 '
               f'-te {g.bounds[0]} {g.bounds[1]} {g.bounds[2]} {g.bounds[3]} '
               f'-tr {t_res} {t_res} '
               f'-dstnodata nan '
               f'-r near '
               f'-co COMPRESS=LZW '
               f'{src_vrt} {dst_vrt}')
        subprocess.check_call(cmd, shell=True)
        s3.upload_file(str(dst_vrt), bucket, f'{prefix}/vrts/{year}/s1_vrt/{dst_vrt.name}')

    band1 = s1_vrt_dir / f'C-VV-{year}-h{h}v{v}.vrt'
    band2 = s1_vrt_dir / f'C-VH-{year}-h{h}v{v}.vrt'
    band3 = s1_vrt_dir / f'C-INC-{year}-h{h}v{v}.vrt'
    band4 = f'/vsis3/{bucket}/{prefix}/alos2_mosaic/{year}/alos2_mosaic_{state}_{year}_h{h}v{v}_HH.tif'
    band5 = f'/vsis3/{bucket}/{prefix}/alos2_mosaic/{year}/alos2_mosaic_{state}_{year}_h{h}v{v}_HV.tif'
    band6 = f'/vsis3/{bucket}/{prefix}/alos2_mosaic/{year}/alos2_mosaic_{state}_{year}_h{h}v{v}_INC.tif'
    band7 = f'/vsis3/{bucket}/{prefix}/landsat_ndvi/{year}/landsat_ndvi_{state}_{year}_h{h}v{v}.tif'
    if year == 2020:
        band8 = f'/vsis3/{bucket}/{prefix}/modis_tree_cover/2019/modis_tc_{state}_2019_h{h}v{v}.tif'
    else:
        band8 = f'/vsis3/{bucket}/{prefix}/modis_tree_cover/{year}/modis_tc_{state}_{year}_h{h}v{v}.tif'

    vrt = Path(f'{state}/{year}/{state}_stacks_{year}_h{h}v{v}.vrt')
    cmd = (f'gdalbuildvrt -overwrite -separate '
           f'{vrt} {band1} {band2} {band3} {band4} {band5} {band6} {band7} {band8}')
    subprocess.check_call(cmd, shell=True)

    with rasterio.open(vrt, 'r+') as dset:
        dset.descriptions = ('C-VV', 'C-VH', 'C-INC',
                             'L-HH', 'L-HV', 'L-INC',
                             'NDVI', 'TC')
    s3.upload_file(str(vrt), bucket, f'{prefix}/vrts/{year}/{vrt.name}')

    print(f'Pushing stack tif for h{h}v{v} ...')
    src_vrt = f'/vsis3/servir-public/geotiffs/{country}/vrts/{year}/{state}_stacks_{year}_h{h}v{v}.vrt'
    dst_tif = f'/vsis3/servir-stacks/{state}/{year}/all-bands/{state}_stacks_{year}_h{h}v{v}.tif'
    cmd = (f'gdal_translate -ot Float32 -co compress=lzw '
           f'--config CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE YES '
           f'{src_vrt} {dst_tif}')
    subprocess.check_call(cmd, shell=True)
