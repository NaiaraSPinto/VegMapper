from pathlib import Path
import subprocess

year = 2019
state = 'para'

t_res = 30
t_epsg = 32722
t_size = 300000
t_xmin = -600000
t_ymin = 8700000
t_xmax = 1200000
t_ymax = 10500000
t_dim = int(t_size/t_res)

in_tif = 'alos2_mosaic_para_ne_2019_HV.tif'
for h in range(4, 6):
    for v in range(2, 3):
        out_tif = f'alos2_mosaic_para_2019_HV_h{h}v{v}.tif'
        xmin = t_xmin + h*t_size
        ymax = t_ymax - v*t_size
        xmax = xmin + t_size
        ymin = ymax - t_size
        cmd = (f'gdalwarp -overwrite '
               f'-t_srs EPSG:{t_epsg} -et 0 '
               f'-te {xmin} {ymin} {xmax} {ymax} '
               f'-tr {t_res} {t_res} '
               f'-srcnodata 200 -dstnodata 0 '
               f'-r bilinear '
               f'-co COMPRESS=LZW '
               f'{in_tif} {out_tif}')
        subprocess.check_call(cmd, shell=True)
