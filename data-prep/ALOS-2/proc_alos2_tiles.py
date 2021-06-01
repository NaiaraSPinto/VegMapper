import argparse
import subprocess
from datetime import date

import rasterio
import geopandas as gpd

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('country', help='country name')
parser.add_argument('state', help='state name')
parser.add_argument('year', type=int, help='year (1900 - 2100)')
parser.add_argument('-h', '--help', action='help', help='warp ALOS-2 Mosaic tiles to UTM tiles')
args = parser.parse_args()

country = args.country.lower()
state = args.state.lower()
year = args.year

if year < 1900 or year > 2100:
    raise Exception('year must be a 4-digit number between 1900 and 2100')

print(f'country: {country}')
print(f'state: {state}')
print(f'year: {year}')

bucket = 'servir-public'
prefix = f'geotiffs/{country}/alos2_mosaic/{year}'

shp = f'{state}_alos2_mosaic_tiles.geojson'
gdf = gpd.read_file(shp)

if year < 2014:
    # ALOS
    postfix = ''
    launch_date = date(2006, 1, 24)
else:
    # ALOS-2
    postfix = 'F02DAR'
    launch_date = date(2014, 5, 24)

yy = str(year)[2:]

# HH & HV
for pq in ['HH', 'HV']:
    vrt = f'{pq}.vrt'
    tif_list = []
    for tile in gdf.tile:
        tif = f'/vsis3/{bucket}/{prefix}/{pq}/{tile}_{yy}_g0_{pq}_{postfix}.enhlee.tif'
        tif_list.append(tif)
    cmd = (f'gdalbuildvrt -overwrite '
           f'{vrt} {" ".join(tif_list)}')
    subprocess.check_call(cmd, shell=True)

# INC
vrt = f'INC.vrt'
inc_list = []
for tile in gdf.tile:
    tarfile = f'{tile}_{yy}_MOS_{postfix}.tar.gz'
    inc_tif = f'/vsitar/vsis3/{bucket}/{prefix}/tarfiles/{tarfile}/{tile}_{yy}_linci_{postfix}.tif'
    inc_list.append(inc_tif)
cmd = (f'gdalbuildvrt -overwrite '
       f'{vrt} {" ".join(inc_list)}')
subprocess.check_call(cmd, shell=True)

# DOY
vrt = f'DOY.vrt'
doy_list = []
for tile in gdf.tile:
    tarfile = f'{tile}_{yy}_MOS_{postfix}.tar.gz'
    doy_tif = f'/vsitar/vsis3/{bucket}/{prefix}/tarfiles/{tarfile}/{tile}_{yy}_date_{postfix}.tif'
    doy_list.append(doy_tif)
cmd = (f'gdalbuildvrt -overwrite '
       f'{vrt} {" ".join(doy_list)}')
subprocess.check_call(cmd, shell=True)

gdf = gpd.read_file(f'../AOI/{state}/{state}_tiles.geojson')

t_res = 30
t_epsg = gdf.crs.to_epsg()

for i in gdf.index:
    h = gdf['h'][i]
    v = gdf['v'][i]
    m = gdf['mask'][i]
    p = gdf['geometry'][i]

    for var in ['DOY', 'HH', 'HV', 'INC']:
        vrt = f'{var}.vrt'
        out_tif = f'/vsis3/{bucket}/{prefix}/alos2_mosaic_{state}_{year}_h{h}v{v}_{var}.tif'

        xmin = p.bounds[0]
        ymin = p.bounds[1]
        xmax = p.bounds[2]
        ymax = p.bounds[3]

        if var in ['HH', 'HV', 'INC']:
            wt = 'Float32'
            ot = 'Float32'
            nodata = 'nan'
            resampling = 'bilinear'
        else:
            wt = 'Int16'
            ot = 'Int16'
            nodata = 0
            resampling = 'near'

        cmd = (f'gdalwarp -overwrite '
               f'-t_srs EPSG:{t_epsg} -et 0 '
               f'-te {xmin} {ymin} {xmax} {ymax} '
               f'-tr {t_res} {t_res} '
               f'-wt {wt} -ot {ot} '
               f'-dstnodata {nodata} '
               f'-r {resampling} '
               f'-co COMPRESS=LZW '
               f'--config CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE YES '
               f'{vrt} {out_tif}')
        subprocess.check_call(cmd, shell=True)

        # if var == 'DOY':
        #     with rasterio.open(out_tif, 'r+') as dset:
        #         days_after_launch = dset.read(1)
        #         mask = dset.read_masks(1)
        #         doy = days_after_launch + (launch_date - date(year, 1, 1)).days + 1
        #         doy[mask == 0] = dset.nodata
        #         dset.write(doy, 1)
