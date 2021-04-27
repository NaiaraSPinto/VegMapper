import subprocess
from datetime import datetime
from pathlib import Path

state = 'ucayali'
year = 2020
path = 171
frame = 617
dry_season_only = True

# Directory storing the Sentinel-1 images
data_dir = Path(f'{state}/{year}/{path}-{frame}')

out_tif_list = []
for polarization in ['VV', 'VH']:
    tif_list = []
    for zip_file in sorted(data_dir.glob('*.zip')):
        granule_name = zip_file.stem
        acquisition_time = datetime.strptime(zip_file.name.split('_')[2],
                                             '%Y%m%dT%H%M%S')
        tif = (f'/vsizip/{data_dir}/{zip_file.name}/'
               f'{granule_name}/{granule_name}_{polarization}.tif')
        if dry_season_only:
            if (acquisition_time.month >= 5) & (acquisition_time.month <= 9):
                tif_list.append(tif)
        else:
            tif_list.append(tif)

    # Output VRT file
    vrt = data_dir / f'{state}_{path}-{frame}_{year}_{polarization}.vrt'

    # Output GeoTIFF file
    out_tif = vrt.with_suffix('.tif')

    if dry_season_only:
        vrt = vrt.with_suffix('.dry.vrt')
        out_tif = out_tif.with_suffix('.dry.tif')

    cmd = (f'gdalbuildvrt '
           f'-overwrite '
           f'{vrt} {" ".join(tif_list)}')
    subprocess.check_call(cmd, shell=True)

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

    cmd = (f'gdal_translate '
           f'-co COMPRESS=LZW '
           f'--config GDAL_VRT_ENABLE_PYTHON YES '
           f'{vrt} {out_tif}')
    # subprocess.check_call(cmd, shell=True)

    out_tif_list.append(out_tif)

g0_tif = data_dir / f'{state}_{path}-{frame}_{year}_gamma0.tif'
if dry_season_only:
    g0_tif = g0_tif.with_suffix('.dry.tif')
cmd = (f'gdal_merge.py '
       f'-init "0 0 0" '
       f'-separate '
       f'-co COMPRESS=LZW '
       f'-o {g0_tif} '
       f'{out_tif_list[0]} '
       f'{out_tif_list[1]}')
subprocess.call(cmd, shell=True)
