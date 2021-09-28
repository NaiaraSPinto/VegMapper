import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

# User inputs
site = 'ucayali'
year = 2020
version = 'v1.1'    # model results will be saved under s3://servir-stacks/site/year/model/version/

# Model parameters
intercept = 24.199260536
posteriors = [-27.817542580, 11.724057104, 0.002757478, -17.933519020,
              -18.064926884, -0.012282095, 30.887101573, -0.037257098,
              -9.236119539, -47.321276739]

# Pixel function of model
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

    out_ar[:] = np.exp(z)/(1+ np.exp(z)) * 100
"""

contents = f"""    <PixelFunctionType>{pixfun_name}</PixelFunctionType>
    <PixelFunctionLanguage>Python</PixelFunctionLanguage>
    <PixelFunctionCode><![CDATA[{pixfun}]]>
    </PixelFunctionCode>
"""

bucket = 'servir-stacks'
cmd = f'gsutil ls s3://{bucket}/{site}/{year}/all-bands/*.tif'
stack_url_list = subprocess.check_output(cmd, shell=True).decode(sys.stdout.encoding).splitlines()

for i, stack_url in enumerate(stack_url_list):
    print(f'\n({i+1}/{len(stack_url_list)}) Applying model to {stack_url} ...')
    stack_key = urlparse(stack_url).path
    stack_tif = f'/vsis3/{bucket}{stack_key}'
    model_tif = stack_tif.replace('all-bands', f'model/{version}').replace(f'{site}_stacks', f'{site}_model')

    # Temporary VRT to apply the model onto stack
    vrt = Path('tmp.vrt')

    # Build VRT
    cmd = (f'gdalbuildvrt -overwrite -b 1 '
        f'{vrt} ')
    for i in range(8):
        cmd = cmd + f' {stack_tif}'
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
    cmd = (f'gdal_translate '
           f'-ot Byte '
           f'--config GDAL_VRT_ENABLE_PYTHON YES '
           f'-co compress=lzw '
           f'--config CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE YES '
           f'{vrt} {model_tif}')
    subprocess.check_call(cmd, shell=True)

    # Delete VRT
    vrt.unlink()
