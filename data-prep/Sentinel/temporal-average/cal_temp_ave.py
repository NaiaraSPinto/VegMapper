from copy import copy
from pathlib import Path

import numpy as np
import rasterio

in_vrt = './171-617/VV.vrt'

with rasterio.open(in_vrt) as vdset:
    width = vdset.width
    height = vdset.height

    # Raster profile for bands in VRT
    out_prof = copy(vdset.profile)
    out_prof.update(driver='GTiff', count=1)

    dsum = np.zeros((height, width))    # data sum
    npix = np.zeros((height, width))    # number of pixels
    ave = np.zeros((height, width))     # temporal average

    for i in range(vdset.count):
        fp = Path(vdset.files[i+1])
        print(i, fp.name)
        data = vdset.read(i+1)
        mask = vdset.read_masks(i+1)
        dsum = dsum + data
        npix = npix + mask/255
        # note that npix won't necessarily be number of rasters in VRT for all pixels

out_dir = Path(f'out_temp_ave')
if not out_dir.exists():
    out_dir.mkdir()

ave[npix != 0] = dsum[npix != 0] / npix[npix != 0]

out_tif = out_dir / 'vv_temp_ave.tif'
with rasterio.open(out_tif, 'w', **out_prof) as dset:
    dset.write(np.float32(ave), 1)
