import argparse
import subprocess
from pathlib import Path

import boto3
import geopandas as gpd

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

pixfun_name = 'rvi'
pixfun = """
import numpy as np
def rvi(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    copol = in_ar[0]
    cxpol = in_ar[1]
    rvi = 4 * cxpol / (copol + cxpol)
    rvi[copol + cxpol == 0] = 0
    out_ar[:] = rvi
"""

contents = f"""    <PixelFunctionType>{pixfun_name}</PixelFunctionType>
    <PixelFunctionLanguage>Python</PixelFunctionLanguage>
    <PixelFunctionCode><![CDATA[{pixfun}]]>
    </PixelFunctionCode>
"""

s3 = boto3.client('s3')
bucket = 'servir-stacks'
prefix = f'{state}/{year}'

gdf = gpd.read_file(f'{state}_tiles.geojson')

for i in gdf.index:
    h = gdf['h'][i]
    v = gdf['v'][i]
    m = gdf['mask'][i]

    if m == 1:
        stack_tif = f'/vsis3/servir-stacks/{state}/{year}/all-bands/{state}_stacks_{year}_h{h}v{v}.tif'
        c_rvi_vrt = Path(f'{state}_C-RVI_{year}_h{h}v{v}.vrt')
        l_rvi_vrt = Path(f'{state}_L-RVI_{year}_h{h}v{v}.vrt')

        cmd = (f'gdalbuildvrt -overwrite -b 1 -b 2 '
               f'{c_rvi_vrt} {stack_tif}')
        subprocess.check_call(cmd, shell=True)

        cmd = (f'gdalbuildvrt -overwrite -b 4 -b 5 '
               f'{l_rvi_vrt} {stack_tif}')
        subprocess.check_call(cmd, shell=True)

        with open(c_rvi_vrt) as f:
            lines = f.readlines()
        lines[3] = lines[3].replace('band="1"',
                                    'band="1" subClass="VRTDerivedRasterBand"')
        del lines[14:17]
        lines.insert(4, contents)
        with open(c_rvi_vrt, 'w') as f:
            f.writelines(lines)

        with open(l_rvi_vrt) as f:
            lines = f.readlines()
        lines[3] = lines[3].replace('band="1"',
                                    'band="1" subClass="VRTDerivedRasterBand"')
        del lines[13:16]
        lines.insert(4, contents)
        with open(l_rvi_vrt, 'w') as f:
            f.writelines(lines)

        s3.upload_file(c_rvi_vrt, bucket, f'{prefix}/C-RVI/{c_rvi_vrt}')
        s3.upload_file(l_rvi_vrt, bucket, f'{prefix}/L-RVI/{l_rvi_vrt}')

        c_rvi_vrt.unlink()
        l_rvi_vrt.unlink()
