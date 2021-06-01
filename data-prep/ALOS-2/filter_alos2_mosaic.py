import argparse
from pathlib import Path

import boto3
import geopandas as gpd
import numpy as np
import rasterio

import py_gamma as pg
import treepeople.gamma as gm

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('country', help='country name')
parser.add_argument('state', help='state name')
parser.add_argument('year', type=int, help='year (1900 - 2100)')
parser.add_argument('-h', '--help', action='help', help='filter ALOS-2 Mosaic images')
args = parser.parse_args()

country = args.country.lower()
state = args.state.lower()
year = args.year

if year < 1900 or year > 2100:
    raise Exception('year must be a 4-digit number between 1900 and 2100')

print(f'country: {country}')
print(f'state: {state}')
print(f'year: {year}')

s3 = boto3.resource('s3')
bucket = 'servir-public'
prefix = f'geotiffs/{country}/alos2_mosaic/{year}'

shp = f'{state}_alos2_mosaic_tiles.geojson'
gdf = gpd.read_file(shp)

for tile in gdf.tile:
    print(tile)

    if year < 2014:
        # ALOS
        postfix = ''
    else:
        # ALOS-2
        postfix = 'F02DAR'

    yy = str(year)[2:]
    tarfile = f'{prefix}/tarfiles/{tile}_{yy}_MOS_{postfix}.tar.gz'

    for pq in ['HH', 'HV']:
        dn_tif = f'/vsitar/vsis3/{bucket}/{tarfile}/{tile}_{yy}_sl_{pq}_{postfix}.tif'
        g0_tif = Path(f'{tile}_{yy}_g0_{pq}_{postfix}.tif')

        # Convert DN to gamma0
        while True:
            try:
                with rasterio.open(dn_tif) as src:
                    dn = src.read(1).astype(np.float64)
                    mask = src.read_masks(1)
                    profile = src.profile
                    g0_db = np.zeros(dn.shape)
                    g0_db[mask == 255] = 10*np.log10(dn[mask == 255]**2) - 83
                    g0 = np.zeros(dn.shape)
                    g0[mask == 255] = 10**(g0_db[mask == 255]/10)
                    profile.update(driver='GTiff',
                                   dtype=np.float32,
                                   nodata=0,
                                   compress='LZW')
                break
            except rasterio.errors.RasterioIOError:
                continue
        with rasterio.open(g0_tif, 'w', **profile) as dst:
            dst.write(np.float32(g0), 1)

        # Apqly Enhanced Lee spatial filter (Lopes et al., 1990)
        g0_bin = g0_tif.with_suffix('.bin')
        g0_par = g0_tif.with_suffix('.bin.par')
        pg.dem_import(str(g0_tif), str(g0_bin), str(g0_par))
        g0_dset = gm.Dataset(g0_bin, srid=4326)
        g0_enhlee = gm.enh_lee(g0_dset, 180, 1, 5)
        g0_enhlee_tif = Path(gm.to_geotiff(g0_enhlee))

        # Upload to S3
        key = f'{prefix}/{pq}/{g0_enhlee_tif.name.replace(".bin", "")}'
        s3.meta.client.upload_file(str(g0_enhlee_tif), bucket, key)

        # Remove output files
        g0_tif.unlink()
        g0_bin.unlink()
        g0_par.unlink()
        g0_enhlee_tif.unlink()
        Path(g0_enhlee.dat).unlink()
