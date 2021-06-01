import argparse
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

s3 = boto3.client('s3')
bucket = 'servir-stacks'
prefix = f'{state}/{year}'

gdf = gpd.read_file(f'{state}_tiles.geojson')

for i in gdf.index:
    h = gdf['h'][i]
    v = gdf['v'][i]
    m = gdf['mask'][i]

    if m == 1:
        print(f'Building condensed stacks for h{h}v{v} ...')

        stack_tif = f'/vsis3/servir-stacks/{state}/{year}/all-bands/{state}_stacks_{year}_h{h}v{v}.tif'
        c_rvi_vrt = f'/vsis3/servir-stacks/{state}/{year}/C-RVI/{state}_C-RVI_{year}_h{h}v{v}.vrt'
        l_rvi_vrt = f'/vsis3/servir-stacks/{state}/{year}/L-RVI/{state}_L-RVI_{year}_h{h}v{v}.vrt'

        with rasterio.open(stack_tif) as dset:
            ndvi = np.int16(np.round(dset.read(7)*100))
            tc = np.int16(dset.read(8))
            profile = dset.profile
        with rasterio.Env(GDAL_VRT_ENABLE_PYTHON=True):
            with rasterio.open(c_rvi_vrt) as dset:
                c_rvi = np.int16(np.round(dset.read(1)*100))
            with rasterio.open(l_rvi_vrt) as dset:
                l_rvi = np.int16(np.round(dset.read(1)*100))

        profile.update(dtype=np.int16, count=4)

        cstack = Path(f'{state}_condensed_stacks_{year}_h{h}v{v}.tif')
        with rasterio.open(cstack, 'w', **profile) as dset:
            dset.write(c_rvi, 1)
            dset.write(l_rvi, 2)
            dset.write(ndvi, 3)
            dset.write(tc, 4)
            dset.descriptions = ('C-RVIx100', 'L-RVIx100', 'NDVIx100', 'TC')

        s3.upload_file(str(cstack), bucket, f'{prefix}/condensed/{cstack}')
        cstack.unlink()
