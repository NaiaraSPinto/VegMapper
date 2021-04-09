from datetime import date
from pathlib import Path
import tarfile

import numpy as np
from osgeo import gdal
import rasterio

# site = 'ucayali'
site = 'para_ne'
# year_list = [2007, 2008, 2009, 2010, 2015, 2016, 2017, 2018]
year_list = [2019]
data_list = ['date', 'linci', 'mask', 'sl_HH', 'sl_HV']

for year in year_list:
    print(f'\nProcessing data for year {year}:')
    data_dir = Path(f'tarfiles/{year}')

    # Launch dates of ALOS and ALOS-2
    if year < 2014:
        # ALOS
        launch_date = date(2006, 1, 24)
    else:
        # ALOS-2
        launch_date = date(2014, 5, 24)

    # Extract all tarball files (will skip if extracted dir exists)
    for tar_gz in data_dir.rglob(f'*_{str(year)[2:]}_*.tar.gz'):
        dst_dir = data_dir / tar_gz.stem.split('.')[0]
        if not dst_dir.exists():
            with tarfile.open(tar_gz) as tar:
                tar.extractall(path=dst_dir)

    # Built VRT for each data layer
    for data in data_list:
        files = []
        for p in sorted(data_dir.rglob(f'*_{data}*')):
            if p.suffix == '' or p.suffix == '.tif':
                files.append(str(p))
        vrt = data_dir / f'{data}.vrt'
        gdal.BuildVRT(destName=str(vrt), srcDSOrSrcDSTab=files)

    # Convert DN to gamma0 and save to GeoTiff
    for data in ['sl_HH', 'sl_HV']:
        vrt = data_dir / f'{data}.vrt'

        print(f'Loading DN for {data[-2:]} ...')
        with rasterio.open(vrt) as dset:
            dn = dset.read(1).astype(np.float64)
            profile = dset.profile

        print(f'Converting DN to gamma0 ...')
        r0_db = np.zeros(dn.shape)
        r0 = np.zeros(dn.shape)
        r0_db[dn != 0] = 10*np.log10(dn[dn != 0]**2) - 83
        r0[dn != 0] = 10**(r0_db[dn != 0]/10)

        profile.update(driver='GTiff',
                       dtype=np.float32,
                       nodata=0,
                       count=1,
                       compress='DEFLATE',
                       tiled=True,
                       blockxsize=512,
                       blockysize=512)

        out_tif = f'alos2_mosaic_{site}_{year}_{data[-2:]}.tif'
        print(f'Writing gamma0 to {out_tif} ...')
        with rasterio.open(out_tif, 'w', **profile) as dset:
            dset.write(np.float32(r0), 1)

    # Covert date to DOY and save to GeoTiff
    vrt = data_dir / f'date.vrt'
    print(f'Loading days after launch ...')
    with rasterio.open(vrt) as dset:
        days_after_launch = dset.read(1).astype(int)
        mask = np.where(days_after_launch != 0, 1, 0)

    print(f'Converting date to DOY ...')
    doy = days_after_launch + (launch_date - date(year, 1, 1)).days + 1
    doy[mask == 0] = 0

    profile.update(dtype=np.int16)

    out_tif = f'alos2_mosaic_{site}_{year}_DOY.tif'
    print(f'Writing DOY to {out_tif} ...')
    with rasterio.open(out_tif, 'w', **profile) as dset:
        dset.write(np.int16(doy), 1)

    vrt = data_dir / f'linci.vrt'
    out_tif = f'alos2_mosaic_{site}_{year}_INC.tif'
    print(f'Writing local incidence angle to {out_tif} ...')
    gdal.Translate(destName=out_tif,
                   srcDS=str(vrt),
                   noData=0,
                   creationOptions=['COMPRESS=DEFLATE'])

    vrt = data_dir / f'mask.vrt'
    out_tif = f'alos2_mosaic_{site}_{year}_mask.tif'
    print(f'Writing mask to {out_tif} ...')
    gdal.Translate(destName=out_tif,
                   srcDS=str(vrt),
                   creationOptions=['COMPRESS=DEFLATE'])
