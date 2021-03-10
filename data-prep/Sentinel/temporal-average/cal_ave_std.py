from copy import copy
from pathlib import Path

import numpy as np
import rasterio

in_vrt = './amif_4_200_19/vh/vh_ami_filt.vrt'

with rasterio.open(in_vrt) as vdset:
    width = vdset.width
    height = vdset.height

    # Raster profile for bands in VRT
    out_prof = copy(vdset.profile)
    out_prof.update(driver='GTiff', count=1)

    x1 = np.zeros((height, width))
    x2 = np.zeros((height, width))
    n = np.zeros((height, width))
    ave = np.zeros((height, width))
    std = np.zeros((height, width))

    for i in range(vdset.count):
        fp = Path(vdset.files[i+1])
        print(i, fp.name)

        data = vdset.read(i+1)
        mask = vdset.read_masks(i+1)
        x1 = x1 + data
        x2 = x2 + data**2
        n = n + mask/255

out_dir = Path(f'out_ave_std')
if not out_dir.exists():
    out_dir.mkdir()

ave[n != 0] = x1[n != 0] / n[n != 0]
std[n != 0] = np.sqrt(x2[n != 0]/n[n != 0] - ave[n != 0]**2)

out_tif = out_dir / 'vh_ami_ave.tif'
with rasterio.open(out_tif, 'w', **out_prof) as dset:
    dset.write(np.float32(ave), 1)
out_tif = out_dir / 'vh_ami_std.tif'
with rasterio.open(out_tif, 'w', **out_prof) as dset:
    dset.write(np.float32(std), 1)
