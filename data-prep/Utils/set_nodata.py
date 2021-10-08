import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

country = 'brazil'
state = 'para'
data = 'landsat_ndvi'
nodata = 'nan'

for year in [2017, 2018, 2019, 2020]:
    # Get list of stacks
    ls_cmd = f'gsutil ls s3://servir-public/geotiffs/{country}/{data}/{year}'
    url_list = subprocess.check_output(ls_cmd, shell=True).decode(sys.stdout.encoding).splitlines()
    for url in url_list:
        u = urlparse(url)
        bucket = u.netloc
        key = Path(u.path)
        if key.suffix == '.tif':
            print(key)
            cmd = (f'gdal_translate '
                   f'-a_nodata {nodata} -co COMPRESS=LZW '
                   f'/vsis3/{bucket}{key} {key.name}')
            subprocess.check_call(cmd, shell=True)

            cmd = (f'gsutil cp {key.name} s3://{bucket}{key}')
            subprocess.check_call(cmd, shell=True)

            Path(key.name).unlink()