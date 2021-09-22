import subprocess
from pathlib import Path

# User inputs
stack = '/vsis3/servir-stacks/ucayali/2020/all-bands/ucayali_stacks_2020_h1v1.tif'
out_tif = '/vsis3/servir-stacks/ucayali/2020/test.tif'

# Model parameters
intercept = 24.199260536
posteriors = [-27.817542580, 11.724057104, 0.002757478, -17.933519020,
              -18.064926884, -0.012282095, 30.887101573, -0.037257098,
              -9.236119539, -47.321276739]

# Temporary VRT to apply the model onto stack
vrt = Path('tmp.vrt')

# Pixel function for applying model
pixfun_name = 'model'
pixfun = f"""
import numpy as np
def {pixfun_name}(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    intercept = {intercept}
    posteriors = {posteriors}
    l_rvi = 4 * in_ar[4] / (in_ar[3] + in_ar[4])
    c_rvi = 4 * in_ar[1] / (in_ar[0] + in_ar[1])

    z = intercept
    for i in range(8):
        z = z + posteriors[i] * in_ar[i]
    z = z + posteriors[8] * l_rvi
    z = z + posteriors[9] * c_rvi

    out_ar[:] = np.exp(z)/(1+ np.exp(z))
"""

contents = f"""    <PixelFunctionType>{pixfun_name}</PixelFunctionType>
    <PixelFunctionLanguage>Python</PixelFunctionLanguage>
    <PixelFunctionCode><![CDATA[{pixfun}]]>
    </PixelFunctionCode>
"""

# Build VRT
cmd = (f'gdalbuildvrt -overwrite -b 1 '
       f'{vrt} ')
for i in range(8):
    cmd = cmd + f' {stack}'
subprocess.check_call(cmd, shell=True)

# Read VRT
with open(vrt) as f:
    lines = f.readlines()

# Insert pixel function
lines[3] = lines[3].replace('band="1"',
                            'band="1" subClass="VRTDerivedRasterBand"')
for i in range(8):
    lines[8+i*8] = lines[8+i*8].replace(f'<SourceBand>1</SourceBand>',
                                        f'<SourceBand>{i+1}</SourceBand>')
lines.insert(4, contents)

# Write VRT
with open(vrt, 'w') as f:
    f.writelines(lines)

# Translate VRT into GeoTIFF
cmd = (f'gdal_translate --config '
       f'GDAL_VRT_ENABLE_PYTHON YES '
       f'-co compress=lzw '
       f'--config CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE YES '
       f'{vrt} {out_tif}')
subprocess.check_call(cmd, shell=True)

# Delete VRT
vrt.unlink()
