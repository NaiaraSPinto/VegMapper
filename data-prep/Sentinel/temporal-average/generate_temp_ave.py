from pathlib import Path
import subprocess

# Directory storing the Sentinel-1 images
data_dir = Path('S1-171-617-2020')

# Search **recursively** all VV images under "data_dir"
# If you don't want the recursive search, change "rglob" to "glob"
tifs = [str(tif) for tif in sorted(data_dir.rglob('*_VV.tif'))]

# Output VRT file
vrt = data_dir / 'vv.vrt'

# Output GeoTIFF file
out_tif = data_dir / 'vv.tif'

############################## Build initial VRT ##############################
cmd = (f'gdalbuildvrt '
       f'-overwrite '
       f'{vrt} {" ".join(tifs)}')
subprocess.check_call(cmd, shell=True)

####################### Insert pixel function into VRT ########################
with open(vrt) as f:
    lines = f.readlines()

lines[3] = lines[3].replace('band="1"',
                            'band="1" subClass="VRTDerivedRasterBand"')

pixfun_name = 'average'
pixfun = """
import numpy as np
def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
  div = np.zeros(in_ar[0].shape)
  for i in range(len(in_ar)):
    div += (in_ar[i] != 0)
  div[div == 0] = 1
  out_ar[:] = np.sum(in_ar, axis=0, dtype='double') / div
"""

contents = f"""    <PixelFunctionType>{pixfun_name}</PixelFunctionType>
    <PixelFunctionLanguage>Python</PixelFunctionLanguage>
    <PixelFunctionCode><![CDATA[{pixfun}]]>
    </PixelFunctionCode>
"""

lines.insert(4, contents)

with open(vrt, 'w') as f:
    f.writelines(lines)

########################## Translate VRT to GeoTIFF ###########################
cmd = (f'gdal_translate '
       f'-co COMPRESS=LZW '
       f'--config GDAL_VRT_ENABLE_PYTHON YES '
       f'{vrt} {out_tif}')
subprocess.check_call(cmd, shell=True)
