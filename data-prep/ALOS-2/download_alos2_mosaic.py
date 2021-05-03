from math import ceil, floor
from pathlib import Path
import subprocess

site = 'para_ne'
# year_list = [2007, 2008, 2009, 2010, 2015, 2016, 2017, 2018]
year_list = [2019]

with open(f'alos2_mosaic_list_{site}.txt') as f:
    tile_list = f.read().splitlines()

# Enter your JAXA credentials, which can be registered from:
# https://www.eorc.jaxa.jp/ALOS/en/palsar_fnf/registration.htm
username = input('Enter username:')
password = input('Enter password:')

for year in year_list:
    out_dir = Path(f'tarfiles/{year}')
    if not out_dir.exists():
        out_dir.mkdir(parents=True)

    for i, tile in enumerate(tile_list):
        if year < 2014:
            # ALOS
            file = out_dir / f'{tile}_{str(year)[2:]}_MOS.tar.gz'
        else:
            # ALOS-2
            file = out_dir / f'{tile}_{str(year)[2:]}_MOS_F02DAR.tar.gz'

        print(f'Download progress for year {year}: {i} / {len(tile_list)}')

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
               f'dir_gz/{year}/{grid}/{file.name}')

        cmd = (f'wget --user {username} --password {password} -c -O {file} {url}')
        subprocess.call(cmd, shell=True)
