#!/usr/bin/env python

import subprocess
from pathlib import Path

import numpy as np
import rasterio


def build_condensed_stack(stack_name, condensed_stack_name, stack_dir):
    stack_dir = Path(stack_dir)
    stacks = sorted(stack_dir.glob(f'{stack_name}_h*v*.tif'))
    for stack_tif in stacks:
        print(f'Building condensed version of {stack_tif.name} ...')
        with rasterio.open(stack_tif) as dset:
            # C-band RVI x 100
            c_vv = dset.read(1)
            c_vh = dset.read(2)
            c_rvi =  4 * c_vh / (c_vv + c_vh)
            c_rvi = np.round(c_rvi*100).astype(np.int16)
            c_rvi[c_vv == dset.nodata] = -9999
            c_rvi[c_vv + c_vh == 0] = -9999
            # L-band RVI x 100
            l_hh = dset.read(4)
            l_hv = dset.read(5)
            l_rvi =  4 * l_hv / (l_hh + l_hv)
            l_rvi = np.round(l_rvi*100).astype(np.int16)
            l_rvi[l_hh == dset.nodata] = -9999
            l_rvi[l_hh + l_hv == 0] = -9999
            # NDVI x 100
            ndvi = np.round(dset.read(7)*100).astype(np.int16)
            ndvi[dset.read_masks(7) == 0] = -9999
            # Percent Tree Cover
            tc = dset.read(8).astype(np.int16)
            tc[dset.read_masks(8) == 0] = -9999
            profile = dset.profile

        profile.update(dtype=np.int16, count=4, nodata=-9999)

        # Write to a temporary GeoTIFF that will be translated to COG later
        tmp_tif = Path('tmp.tif')
        with rasterio.open(tmp_tif, 'w', **profile) as dset:
            dset.write(c_rvi, 1)
            dset.write(l_rvi, 2)
            dset.write(ndvi, 3)
            dset.write(tc, 4)
            dset.descriptions = ('C-RVIx100', 'L-RVIx100', 'NDVIx100', 'TC')

        # Translate to COG
        cog_tif = stack_tif.with_stem(stack_tif.stem.replace(stack_name, condensed_stack_name))
        cmd = (f'gdal_translate '
               f'-of COG '
               f'-co COMPRESS=LZW '
               f'-co RESAMPLING=NEAREST '
               f'{tmp_tif} {cog_tif}')
        subprocess.check_call(cmd, shell=True)
        tmp_tif.unlink()
