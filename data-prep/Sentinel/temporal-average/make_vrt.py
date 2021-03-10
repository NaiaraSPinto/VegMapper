from pathlib import Path
import subprocess

data_dir = Path('./171-617')

vv_tifs = sorted(data_dir.rglob('*_VV.tif'))

vrt = data_dir / 'VV.vrt'
cmd = (f'gdalbuildvrt -overwrite '
       f'-separate '
       f'{vrt}')

for tif in vv_tifs:
    cmd = cmd + ' ' + str(tif)

subprocess.check_call(cmd, shell=True)
