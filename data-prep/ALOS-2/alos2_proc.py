#!/usr/bin/env python

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

import cv2 as cv
import numpy as np
import rasterio


def enhanced_lee_filter(img, win_size, num_looks=1, nodata=None):
    src_dtype = img.dtype
    img = img.astype(np.float64)

    # Get image mask (0: nodata; 1: data)
    mask = np.ones(img.shape)
    mask[img == nodata] = 0
    mask[np.isnan(img)] = 0     # in case there are pixels of NaNs

    # Change nodata pixels to 0 so they don't contribute to the sums
    img[mask == 0] = 0

    # Kernel size
    ksize = (win_size, win_size)

    # Window sum of image values
    img_sum = cv.boxFilter(img, -1, ksize,
                           normalize=False, borderType=cv.BORDER_ISOLATED)
    # Window sum of image values squared
    img2_sum = cv.boxFilter(img**2, -1, ksize,
                            normalize=False, borderType=cv.BORDER_ISOLATED)
    # Pixel number within window
    pix_num = cv.boxFilter(mask, -1, ksize,
                           normalize=False, borderType=cv.BORDER_ISOLATED)

    # There might be a loss of accuracy as how boxFilter handles floating point
    # number subtractions/additions, causing a window of all zeros to have
    # non-zero sum, hence correction here using np.isclose.
    img_sum[np.isclose(img_sum, 0)] = 0
    img2_sum[np.isclose(img2_sum, 0)] = 0

    # Get image mean and std within window
    img_mean = np.full(img.shape, np.nan, dtype=np.float64)     # E[X]
    img2_mean = np.full(img.shape, np.nan, dtype=np.float64)    # E[X^2]
    img_mean2 = np.full(img.shape, np.nan, dtype=np.float64)    # (E[X])^2
    img_std = np.full(img.shape, 0, dtype=np.float64)           # sqrt(E[X^2] - (E[X])^2)

    idx = np.where(pix_num != 0)                # Avoid division by zero
    img_mean[idx] = img_sum[idx]/pix_num[idx]
    img2_mean[idx] = img2_sum[idx]/pix_num[idx]
    img_mean2 = img_mean**2

    idx = np.where(~np.isclose(img2_mean, img_mean2))           # E[X^2] and (E[X])^2 are close
    img_std[idx] = np.sqrt(img2_mean[idx] - img_mean2[idx])

    # Get weighting function
    k = 1
    cu = 0.523/np.sqrt(num_looks)
    cmax = np.sqrt(1 + 2/num_looks)
    ci = img_std / img_mean         # it's fine that img_mean could be zero here
    w_t = np.zeros(img.shape)
    w_t[ci <= cu] = 1
    idx = np.where((cu < ci) & (ci < cmax))
    w_t[idx] = np.exp((-k * (ci[idx] - cu)) / (cmax - ci[idx]))

    # Apply weighting function
    img_filtered = (img_mean * w_t) + (img * (1 - w_t))

    # Assign nodata value
    img_filtered[pix_num == 0] = nodata

    return img_filtered.astype(src_dtype)


def proc_tarfile(tarfile, year, proj_dir, vsi_path):
    print(f'\nProcessing {tarfile} ...')

    tile = tarfile.split('_')[0]
    yy = tarfile.split('_')[1]

    if year < 2014:
        # ALOS
        postfix = ''
    else:
        # ALOS-2
        postfix = '_F02DAR'

    if year < 2019:
        suffix = ''
    else:
        suffix = '.tif'

    for pq in ['HH', 'HV']:
        # Read in DN
        dn_raster = f'{vsi_path}/tarfiles/{tarfile}/{tile}_{yy}_sl_{pq}{postfix}{suffix}'
        # For some reasons, there are some .tif in .tar.gz can't even be
        # accessed by gdalinfo. But if we attempt to read it the second time
        # with rasterio, it can go through. It could be the problem of those
        # tarfiles.
        # Examples: S06W056_20_MOS_F02DAR.tar.gz/S06W056_20_sl_HH_F02DAR.tif
        #           N63W142_19_MOS_F02DAR.tar.gz/N63W142_19_sl_HH_F02DAR.tif
        while True:
            try:
                with rasterio.open(dn_raster) as dset:
                    dn = dset.read(1).astype(np.float64)
                    profile = dset.profile
                break
            except rasterio.errors.RasterioIOError:
                continue

        # Convert DN to gamma0
        g0 = dn**2 * 10**(-83/10)

        # Filter gamma0 using enhanced Lee filter
        g0_filtered = enhanced_lee_filter(g0, 5, num_looks=1, nodata=np.nan)

        # Write to GeoTIFF
        profile.update(driver='GTiff', dtype=np.float32, nodata=np.nan)
        g0_filtered_tif = Path(f'{tile}_{yy}_{pq}_filtered.tif')
        with rasterio.open(g0_filtered_tif, 'w', **profile) as dset:
            dset.write(g0_filtered.astype(np.float32), 1)

        # Move/copy filtered GeoTIFF to proj_dir/alos2_mosaic/year/tile
        if isinstance(proj_dir, Path):
            dst_tif = proj_dir / f'alos2_mosaic/{year}/{tile}/{g0_filtered_tif}'
            if not dst_tif.parent.exists():
                dst_tif.parent.mkdir()
            shutil.move(g0_filtered_tif, dst_tif)
        elif isinstance(proj_dir, str):
            dst_tif = f'{proj_dir}/alos2_mosaic/{year}/{tile}/{g0_filtered_tif}'
            cmd = (f'gsutil cp {g0_filtered_tif} {dst_tif}')
            subprocess.check_call(cmd, shell=True)
            g0_filtered_tif.unlink()

    hh_tif = f'{vsi_path.replace("/vsitar", "")}/{tile}/{tile}_{yy}_HH_filtered.tif'
    hv_tif = f'{vsi_path.replace("/vsitar", "")}/{tile}/{tile}_{yy}_HV_filtered.tif'
    inc_tif = f'{vsi_path}/tarfiles/{tarfile}/{tile}_{yy}_linci{postfix}{suffix}'

    return hh_tif, hv_tif, inc_tif


def main():
    parser = argparse.ArgumentParser(
        description='processing ALOS/ALOS-2 yearly mosaic data'
    )
    parser.add_argument('proj_dir', metavar='proj_dir',
                        type=str,
                        help=('project directory (s3:// or gs:// or local dirs); '
                              'ALOS/ALOS-2 mosaic data (.tar.gz) are expected '
                              'to be found under proj_dir/alos2_mosaic/year/tarfiles/'))
    parser.add_argument('year', metavar='year',
                        type=str,
                        help='year')
    args = parser.parse_args()
    year = int(args.year)

    # Check proj_dir
    u = urlparse(args.proj_dir)
    if u.scheme == 's3' or u.scheme == 'gs':
        storage = u.scheme
        bucket = u.netloc
        prefix = u.path.strip('/')
        proj_dir = f'{storage}://{bucket}/{prefix}'
        vsi_path = f'/vsitar/vsi{storage}/{bucket}/{prefix}/alos2_mosaic/{year}'
        subprocess.check_call(f'gsutil ls {storage}://{bucket}',
                              stdout=subprocess.DEVNULL,
                              shell=True)
    else:
        storage = 'local'
        proj_dir = Path(args.proj_dir)
        vsi_path = f'/vsitar/{proj_dir}/alos2_mosaic/{year}'
        if not proj_dir.is_dir():
            raise Exception(f'{proj_dir} is not a valid directory path')

    # .tar.gz filename pattern
    if year < 2014:
        # ALOS
        tarfile_pattern = re.compile(r'^[N|S]{1}\w{2}[E|W]{1}\w{3}_\w{2}_MOS.tar.gz$')
    else:
        # ALOS-2
        tarfile_pattern = re.compile(r'^[N|S]{1}\w{2}[E|W]{1}\w{3}_\w{2}_MOS_F02DAR.tar.gz$')

    # List all tarfiles
    ls_cmd = f'ls {proj_dir}/alos2_mosaic/{year}/tarfiles/*.tar.gz'
    if isinstance(proj_dir, str):
        ls_cmd = 'gsutil ' + ls_cmd
    tarfile_list = [Path(p).name for p in subprocess.check_output(ls_cmd, shell=True).decode(sys.stdout.encoding).splitlines()]

    if not tarfile_list:
        raise Exception(f'No .tar.gz files found under {proj_dir}/alos2_mosaic/{year}/tarfiles/.')

    tif_lists = {
        'HH': [],
        'HV': [],
        'INC': [],
    }

    # Processing mosaic data and get list of processed .tif
    print(f'\nProcessing ALOS-2 yearly mosaic data in {proj_dir}/alos2_mosaic/{year}/tarfiles ...')
    for tarfile in tarfile_list:
        if tarfile_pattern.fullmatch(tarfile):
            hh_tif, hv_tif, inc_tif = proc_tarfile(tarfile, year, proj_dir, vsi_path)
            tif_lists['HH'].append(hh_tif)
            tif_lists['HV'].append(hv_tif)
            tif_lists['INC'].append(inc_tif)

    # Make VRT for HH, HV, INC
    for var, tif_list in tif_lists.items():
        vrt = Path(f'alos2_mosaic_{year}_{var}.tif')
        cmd = f'gdalbuildvrt -overwrite {vrt} {" ".join(tif_list)}'
        subprocess.check_call(cmd, shell=True)
        if isinstance(proj_dir, Path):
            dst_vrt = proj_dir / f'alos2_mosaic/{year}/{vrt}'
            shutil.move(vrt, dst_vrt)
        elif isinstance(proj_dir, str):
            dst_vrt = f'{proj_dir}/alos2_mosaic/{year}/{vrt}'
            cmd = (f'gsutil cp {vrt} {dst_vrt}')
            subprocess.check_call(cmd, shell=True)
            vrt.unlink()

    print('\nDONE processing ALOS-2 yearly mosaic data.')

if __name__ == '__main__':
    main()