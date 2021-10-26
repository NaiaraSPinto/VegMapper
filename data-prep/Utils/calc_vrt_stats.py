#!/usr/bin/env python

import argparse
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlparse

"""
TODO:
1. Add more stat pixel functions.
2. Currently assume nodata is 0, need to generalize this.
"""

parser = argparse.ArgumentParser(description='calculate stats for rasters in a VRT')
parser.add_argument('vrtpath', metavar='vrtpath',
                    type=str,
                    help='VRT path')
parser.add_argument('stat', metavar='stat',
                    type=str,
                    choices=['mean', 'variance'],
                    help='statistic to be calculated')
args = parser.parse_args()

# Download VRT
u = urlparse(args.vrtpath)
p = Path(u.path)
if p.suffix != '.vrt':
    raise Exception(f'{p.name} is not a VRT file.')
if u.scheme == 's3' or u.scheme == 'gs':
    bucket = u.netloc
    prefix = str(p.parent).strip('/')
    subprocess.check_call(f'gsutil cp {args.vrtpath} .', shell=True)
elif p.exists():
    shutil.copy(p, '.')
else:
    raise Exception(f'VRT {args.vrtpath} does not exist')

src_vrt = Path(p.name)
dst_vrt = src_vrt.with_stem(src_vrt.stem + f'_{args.stat}')

# Pixel function
if args.stat == 'mean':
    pixfun = f"""
import numpy as np
def {args.stat}(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    div = np.zeros(in_ar[0].shape)
    for i in range(len(in_ar)):
        div += (in_ar[i] != 0)
    div[div == 0] = 1
    out_ar[:] = np.sum(in_ar, axis=0, dtype=in_ar[0].dtype) / div
"""
elif args.stat == 'variance':
    pixfun = f"""
import numpy as np
def {args.stat}(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    in_ar = np.array(in_ar)
    in_ar[in_ar == 0] = np.nan
    out_ar[:] = np.nanvar(in_ar, axis=0, dtype=in_ar[0].dtype)
"""
else:
    raise Exception(f'{args.stat} is currently not supported.')

contents = f"""    <PixelFunctionType>{args.stat}</PixelFunctionType>
    <PixelFunctionLanguage>Python</PixelFunctionLanguage>
    <PixelFunctionCode><![CDATA[{pixfun}]]>
    </PixelFunctionCode>
"""

with open(src_vrt) as f:
    lines = f.readlines()

lines[3] = lines[3].replace('band="1"',
                            'band="1" subClass="VRTDerivedRasterBand"')

lines.insert(4, contents)

with open(dst_vrt, 'w') as f:
    f.writelines(lines)

if u.scheme == 's3':
    dst_tif = args.vrtpath.replace('s3:/', '/vsis3').replace('.vrt', f'_{args.stat}.tif')
else:
    dst_tif = dst_vrt.with_suffix('.tif')

print(f'\nCalculating {args.stat} for {args.vrtpath} ...')
cmd = (f'gdal_translate '
       f'-co COMPRESS=LZW '
       f'--config GDAL_VRT_ENABLE_PYTHON YES '
       f'--config CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE YES '
       f'{dst_vrt} {dst_tif}')
subprocess.check_call(cmd, shell=True)

if u.scheme == 's3':
    subprocess.check_call(f'gsutil cp {dst_vrt} s3://{bucket}/{prefix}/{dst_vrt.name}', shell=True)
elif u.scheme == 'gs':
    subprocess.check_call(f'gsutil cp {dst_tif} gs://{bucket}/{prefix}/{dst_vrt.name}', shell=True)
    subprocess.check_call(f'gsutil cp {dst_tif} gs://{bucket}/{prefix}/{dst_tif.name}', shell=True)
    dst_tif.unlink()
elif u.scheme == '':
    shutil.copy(dst_vrt, p.parent)
    shutil.copy(dst_tif, p.parent)
    dst_tif.unlink()

src_vrt.unlink()
dst_vrt.unlink()