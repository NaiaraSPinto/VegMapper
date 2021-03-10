from copy import copy
from pathlib import Path
import shutil
import subprocess

import numpy as np
import rasterio

import py_gamma as pg
import treepeople.gamma as gm

in_vrt = './171-617/VV.vrt'

# enl:     estimate for the ENL of unfiltered individual input data files
# enl_ave: estimate for the ENL of the average image
# winsz:   size of the Lee filter window (valid values: 7, 13 (default), 19)
enl = 4
enl_ave = 200
winsz = 19

tmp_dir = Path('tmp')
if not tmp_dir.exists():
    tmp_dir.mkdir()

mli_list = []
par_list = []

with rasterio.open(in_vrt) as vdset:
    width = vdset.width
    height = vdset.height

    # Raster profile for bands in VRT
    out_prof = copy(vdset.profile)
    out_prof.update(driver='GTiff', count=1)

    # Write out tif/mli/par files for each band in VRT
    for i in range(vdset.count):
        fp = Path(vdset.files[i+1])
        print(i, fp.name)

        out_tif = tmp_dir / fp.name
        out_mli = out_tif.with_suffix('.mli')
        out_par = out_tif.with_suffix('.par')

        if not out_tif.exists():
            with rasterio.open(out_tif, 'w', **out_prof) as dset:
                data = vdset.read(i+1)
                dset.write(data, 1)

        if not out_mli.exists():
            if out_par.exists():
                out_par.unlink()
            pg.dem_import(str(out_tif), str(out_mli), str(out_par))

        mli_list.append(str(out_mli))
        par_list.append(str(out_par))

out_dir = Path(f'out_amif_{enl}_{enl_ave}_{winsz}')
if not out_dir.exists():
    out_dir.mkdir()

rmli_tab = out_dir / 'rmli_tab'
rmli_tab_filt = out_dir / 'rmli_tab_filt'
ave = out_dir / 'ave.mli'

if rmli_tab.exists():
    rmli_tab.unlink()
if rmli_tab_filt.exists():
    rmli_tab_filt.unlink()
if ave.exists():
    ave.unlink()

for i in range(len(mli_list)):
    with open(rmli_tab, 'a') as f:
        f.write(f'{mli_list[i]} {par_list[i]}\n')
    with open(rmli_tab_filt, 'a') as f:
        f.write(f'{mli_list[i]}.filt {par_list[i]}\n')

cmd = (f'AMI_FILTER_GEO '
       f'{rmli_tab} '
       f'{rmli_tab_filt} '
       f'{ave} '
       f'{width} {height} {enl} {enl_ave} {winsz} 1 1')
subprocess.call(cmd, shell=True)

for i in range(len(mli_list)):
    mli = f'{mli_list[i]}.filt'
    par = f'{par_list[i]}'
    dset = gm.Dataset(mli, par=par)
    tif = Path(gm.to_geotiff(dset))
    shutil.move(tif, out_dir / tif.name)

dset = gm.Dataset(ave, par=par)
tif = Path(gm.to_geotiff(dset))
shutil.move(tif, out_dir / tif.name)
