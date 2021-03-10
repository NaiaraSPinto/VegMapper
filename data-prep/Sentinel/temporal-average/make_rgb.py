from pathlib import Path
import subprocess

data_dir = Path('out_amif_4_200_19')

vv_tif = sorted(data_dir.rglob('*_VV.mli.filt.tif'))
vh_tif = sorted(data_dir.rglob('*_VH.mli.filt.tif'))

for i in range(len(vv_tif)):
    out_tif = data_dir / str(vv_tif[i].name).replace('_VV.mli', '_RGB.mli')
    print(i, out_tif)
    cmd = (f'gdal_merge.py '
           f'-init "0 0 0" '
           f'-separate '
           f'-co COMPRESS=LZW '
           f'-o {out_tif} {vv_tif[i]} {vh_tif[i]}')
    subprocess.call(cmd, shell=True)
