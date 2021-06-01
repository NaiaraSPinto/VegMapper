import argparse
import subprocess
from datetime import datetime
from pathlib import Path

import boto3
import rasterio
import geopandas as gpd
import numpy as np

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

gdf = gpd.read_file(f'granules/{state}/{state}_sentinel_granules_{year}_dry.geojson')
gdf = gdf.groupby(gdf.columns[[12, 6]].to_list()).size().reset_index()

s3 = boto3.client('s3')
bucket = 'servir-public'

for i in gdf.index:
    path_frame = f'{gdf["pathNumber"][i]}_{gdf["frameNumber"][i]}'
    print(path_frame)

    prefix = f'geotiffs/{country}/sentinel_1/{year}/{path_frame}'
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    tif_list = []
    for c in response['Contents']:
        key = Path(c['Key'])
        if key.suffix == '.zip':
            # acquisition_time = datetime.strptime(key.stem.split('_')[-6], '%Y%m%dT%H%M%S')
            # if (acquisition_time.month >= 5) & (acquisition_time.month <= 9):
            granule = key.stem
            tif = f'/vsizip/vsis3/{bucket}/{key}/{granule}/{granule}_ls_map.tif'
            tif_list.append(tif)
            # print(tif)

    pixfun_name = 'average'
    pixfun = """
import numpy as np
def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    div = np.zeros(in_ar[0].shape)
    for i in range(len(in_ar)):
        div += (in_ar[i] != 0)
    div[div == 0] = 1
    out_ar[:] = np.sum(in_ar, axis=0, dtype=in_ar[0].dtype) / div
"""

    contents = f"""    <PixelFunctionType>{pixfun_name}</PixelFunctionType>
        <PixelFunctionLanguage>Python</PixelFunctionLanguage>
        <PixelFunctionCode><![CDATA[{pixfun}]]>
        </PixelFunctionCode>
    """

    ls_map_vrt = Path(f'{year}_{path_frame}_ls_map.vrt')
    cmd = (f'gdalbuildvrt -overwrite '
           f'{ls_map_vrt} {" ".join(tif_list)}')
    subprocess.check_call(cmd, shell=True)

    with open(ls_map_vrt) as f:
        lines = f.readlines()

    lines[3] = lines[3].replace('band="1"',
                                'band="1" subClass="VRTDerivedRasterBand"')

    lines.insert(4, contents)

    with open(ls_map_vrt, 'w') as f:
        f.writelines(lines)

    ls_map_tif = Path(f'{year}_{path_frame}_ls_map.tif')
    cmd = (f'gdal_translate '
           f'-co compress=lzw '
           f'--config GDAL_VRT_ENABLE_PYTHON YES '
           f'{ls_map_vrt} {ls_map_tif}')
    subprocess.check_call(cmd, shell=True)

###############################################################################

    vv_tif = f'/vsis3/{bucket}/{prefix}/{year}_{path_frame}_VV.tif'
    with rasterio.open(vv_tif) as dset:
        mask_vv = dset.read_masks(1)
    with rasterio.open(ls_map_tif) as dset:
        mask0 = dset.read_masks(1)
        mask0[(mask_vv == 255) & (mask0 == 0)] = 255
        profile = dset.profile

    mask0[mask0 == 255] = 1
    mask = np.zeros(mask0.shape, dtype=np.uint8)
    edge = np.zeros(mask0.shape, dtype=np.uint8)
    edge_lr = np.zeros(mask0.shape, dtype=np.uint8)

    for i in range(200):

        # print(i)
        mask[:] = mask0[:]
        row, col = np.where(np.diff(mask) == 255)
        mask[row, col] = 0
        row, col = np.where(np.diff(mask) == 1)
        mask[row, col+1] = 0
        row, col = np.where(np.diff(mask, axis=0) == 255)
        mask[row, col] = 0
        row, col = np.where(np.diff(mask, axis=0) == 1)
        mask[row+1, col] = 0
        edge[:] = mask0[:] - mask[:]

        row, col = np.where(edge == 1)

        row_ul = row[row == np.min(row)][0]
        col_ul = np.min(col[row == np.min(row)])

        row_ur = np.max(row[col == np.max(col)])
        col_ur = col[col == np.max(col)][0]

        row_lr = row[row == np.max(row)][0]
        col_lr = np.max(col[row == np.max(row)])

        row_ll = np.min(row[col == np.min(col)])
        col_ll = col[col == np.min(col)][0]

        # Right edge
        idx = np.where((row >= row_ur) & (row <= row_lr) &
                       (col >= col_lr) & (col <= col_ur))
        edge_lr[row[idx], col[idx]] = 1

        # Left edge
        idx = np.where((row >= row_ul) & (row <= row_ll) &
                       (col >= col_ll) & (col <= col_ul))
        edge_lr[row[idx], col[idx]] = 1

        mask0[edge_lr == 1] = 0

###############################################################################

    profile.update(dtype=np.float32, nodata=np.nan)

    for pq in ['VV', 'VH']:
        src_tif = f'/vsis3/{bucket}/{prefix}/{year}_{path_frame}_{pq}.tif'
        dst_tif = Path(f'{year}_{path_frame}_{pq}.tif')
        while True:
            try:
                print(f'Trying to read {src_tif} ...')
                with rasterio.open(src_tif) as src:
                    data = src.read(1)
                    mask = src.read_masks(1)
                print(f'Success!')
                break
            except rasterio.errors.RasterioIOError:
                print(f'Fail!')
                continue
        data[mask == 0] = np.nan
        data[edge_lr == 1] = np.nan
        mask[edge_lr == 1] = 0
        with rasterio.open(dst_tif, 'w', **profile) as dst:
            dst.write(data, 1)
        dst_key = f'{prefix}/{year}_{path_frame}_{pq}.tif'
        s3.upload_file(str(dst_tif), bucket, dst_key)
        dst_tif.unlink()

    src_tif = f'/vsis3/{bucket}/{prefix}/{year}_{path_frame}_INC.tif'
    dst_tif = Path(f'{year}_{path_frame}_INC.tif')
    while True:
        try:
            print(f'Trying to read {src_tif} ...')
            with rasterio.open(src_tif) as src:
                data = src.read(1)
            print(f'Success!')
            break
        except rasterio.errors.RasterioIOError:
            print(f'Fail!')
            continue
    data[mask == 0] = np.nan
    with rasterio.open(dst_tif, 'w', **profile) as dst:
        dst.write(data, 1)
    dst_key = f'{prefix}/{year}_{path_frame}_INC.tif'
    s3.upload_file(str(dst_tif), bucket, dst_key)
    dst_tif.unlink()

    ls_map_vrt.unlink()
    ls_map_tif.unlink()
