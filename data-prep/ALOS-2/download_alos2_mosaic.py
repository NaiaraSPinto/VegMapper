import argparse
import subprocess
from math import ceil, floor

import geopandas as gpd

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('country', help='country name')
parser.add_argument('state', help='state name')
parser.add_argument('year', type=int, help='year (1900 - 2100)')
parser.add_argument('-h', '--help', action='help', help='download ALOS-2 Mosaic data from JAXA website')
args = parser.parse_args()

country = args.country.lower()
state = args.state.lower()
year = args.year

if year < 1900 or year > 2100:
    raise Exception('year must be a 4-digit number between 1900 and 2100')

print(f'country: {country}')
print(f'state: {state}')
print(f'year: {year}')

shp = f'{state}_alos2_mosaic_tiles.geojson'
gdf = gpd.read_file(shp)

# Enter your JAXA credentials, which can be registered from:
# https://www.eorc.jaxa.jp/ALOS/en/palsar_fnf/registration.htm
username = input('Enter username:')
password = input('Enter password:')

for i, tile in enumerate(gdf.tile):
    if year < 2014:
        # ALOS
        file = f'{tile}_{str(year)[2:]}_MOS.tar.gz'
    else:
        # ALOS-2
        file = f'{tile}_{str(year)[2:]}_MOS_F02DAR.tar.gz'

    print(f'Download progress for year {year}: {i} / {len(gdf.tile)}')

    ns = tile[0]    # N or S
    ew = tile[3]    # E or W
    lat_tile = int(tile[1:3])   # lat. of upper-left corner of 1-deg tile
    lon_tile = int(tile[4:7])   # lon. of upper-left corner of 1-deg tile

    # lat. of upper-left coner of 5-deg grid
    if ns == 'N':
        lat_grid = ceil(lat_tile/5)*5
    else:
        lat_grid = floor(lat_tile/5)*5
    # lon. of upper-left coner of 5-deg grid
    if ew == 'E':
        lon_grid = floor(lon_tile/5)*5
    else:
        lon_grid = ceil(lon_tile/5)*5
    if lat_grid == 0:
        ns = 'N'
    if lon_grid == 0:
        ew = 'E'
    grid = f'{ns}{lat_grid:02}{ew}{lon_grid:03}'

    url = (f'https://www.eorc.jaxa.jp/ALOS/en/palsar_fnf/data/{year}/'
           f'dir_gz/{year}/{grid}/{file}')

    cmd = (f'wget --user {username} --password {password} -O- {url} | '
           f'aws s3 cp - s3://servir-public/geotiffs/{country}/alos2_mosaic/{year}/tarfiles/{file}')
    subprocess.call(cmd, shell=True)
