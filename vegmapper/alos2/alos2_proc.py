#!/usr/bin/env python

import argparse
import re
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

import numpy as np
import rasterio

import vegmapper
from vegmapper import ProjDir


def proc_tarfile(tarfile, year, proj_dir, vsi_path, lee_win_size=5, lee_num_looks=1):
    print(f'\nProcessing {tarfile} ...')

    tile = tarfile.split('_')[0]
    yy = tarfile.split('_')[1]

    if year < 2014:
        # ALOS
        postfix = ''
        launch_date = date(2006, 1, 24)
    else:
        # ALOS-2
        postfix = '_F02DAR'
        launch_date = date(2014, 5, 24)

    if year < 2019:
        suffix = ''
    else:
        suffix = '.tif'

    p = ProjDir(proj_dir)

    # Process backscatter (HH/HV)
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
                    mask = dset.read_masks(1)
                    dn[mask == 0] = np.nan
                    profile = dset.profile
                break
            except rasterio.errors.RasterioIOError:
                continue

        # Convert DN to gamma0
        g0 = dn**2 * 10**(-83/10)

        # Filter gamma0 using enhanced Lee filter
        g0_filtered = vegmapper.filter.enhanced_lee(g0, lee_win_size, lee_num_looks, nodata=np.nan)

        # Write to GeoTIFF
        profile.update(driver='GTiff', dtype=np.float32, nodata=np.nan)
        g0_filtered_tif = Path(f'{tile}_{yy}_{pq}_filtered.tif')
        with rasterio.open(g0_filtered_tif, 'w', **profile) as dset:
            dset.write(g0_filtered.astype(np.float32), 1)

        # Move/copy filtered GeoTIFF to proj_dir/alos2_mosaic/year/tile
        if p.is_cloud:
            dst_tif = f'{p.proj_dir}/alos2_mosaic/{year}/{tile}/{g0_filtered_tif}'
            cmd = (f'gsutil -q cp {g0_filtered_tif} {dst_tif}')
            subprocess.check_call(cmd, shell=True)
            g0_filtered_tif.unlink()
        else:
            dst_tif = p.proj_dir / f'alos2_mosaic/{year}/{tile}/{g0_filtered_tif}'
            if not dst_tif.parent.exists():
                dst_tif.parent.mkdir()
            shutil.move(g0_filtered_tif, dst_tif)

    # Process local incidence angle (INC)
    cmd = (f'gdal_translate '
           f'-co COMPRESS=LZW '
           f'--config CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE YES '
           f'{vsi_path}/tarfiles/{tarfile}/{tile}_{yy}_linci{postfix}{suffix} '
           f'{vsi_path.replace("/vsizip", "")}/{tile}/{tile}_{yy}_INC.tif')
    subprocess.check_call(cmd, shell=True)

    # Process acquisition day of year (DOY)
    tmp_doy_tif = Path(f'{tile}_{yy}_DOY.tif')
    cmd = (f'gdal_translate '
           f'-ot Int16 '
           f'-co COMPRESS=LZW '
           f'--config CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE YES '
           f'{vsi_path}/tarfiles/{tarfile}/{tile}_{yy}_date{postfix}{suffix} '
           f'{tmp_doy_tif}')
        #    f'{vsi_path.replace("/vsitar", "")}/{tile}/{tile}_{yy}_DOY.tif')
    subprocess.check_call(cmd, shell=True)
    with rasterio.open(tmp_doy_tif, 'r+') as dset:
        days_after_launch = dset.read(1)
        mask = dset.read_masks(1)
        doy = days_after_launch + (launch_date - date(year, 1, 1)).days + 1
        dset.nodata = -9999
        doy[mask == 0] = -9999
        dset.write(doy, 1)
    if p.is_cloud:
        dst_tif = f'{p.proj_dir}/alos2_mosaic/{year}/{tile}/{tmp_doy_tif}'
        cmd = (f'gsutil -q cp {tmp_doy_tif} {dst_tif}')
        subprocess.check_call(cmd, shell=True)
        tmp_doy_tif.unlink()
    else:
        dst_tif = p.proj_dir / f'alos2_mosaic/{year}/{tile}/{tmp_doy_tif}'
        shutil.move(g0_filtered_tif, dst_tif)

    hh_tif = f'{vsi_path.replace("/vsizip", "")}/{tile}/{tile}_{yy}_HH_filtered.tif'
    hv_tif = f'{vsi_path.replace("/vsizip", "")}/{tile}/{tile}_{yy}_HV_filtered.tif'
    inc_tif = f'{vsi_path.replace("/vsizip", "")}/{tile}/{tile}_{yy}_INC.tif'
    doy_tif = f'{vsi_path.replace("/vsizip", "")}/{tile}/{tile}_{yy}_DOY.tif'

    return hh_tif, hv_tif, inc_tif, doy_tif


def proc_tiles(proj_dir, year, filter_win_size=5, filter_num_looks=1):
    # Check proj_dir
    p = ProjDir(proj_dir)
    if p.is_cloud:
        vsi_path = f'/vsizip/vsi{p.storage}/{p.bucket}/{p.prefix}/alos2_mosaic/{year}'
    else:
        vsi_path = f'/vsizip/{p.proj_dir}/alos2_mosaic/{year}'

    # .tar.gz filename pattern
    if year < 2014:
        # ALOS
        tarfile_pattern = re.compile(r'^[N|S]{1}\w{2}[E|W]{1}\w{3}_\w{2}_MOS.zip$')
    else:
        # ALOS-2
        tarfile_pattern = re.compile(r'^[N|S]{1}\w{2}[E|W]{1}\w{3}_\w{2}_MOS_F02DAR.zip$')

    # List all tarfiles
    ls_cmd = f'ls {p.proj_dir}/alos2_mosaic/{year}/tarfiles/*.zip'
    if p.is_cloud:
        ls_cmd = 'gsutil ' + ls_cmd
    tarfile_list = [Path(p).name for p in subprocess.check_output(ls_cmd, shell=True).decode(sys.stdout.encoding).splitlines()]

    if not tarfile_list:
        raise Exception(f'No .tar.gz files found under {p.proj_dir}/alos2_mosaic/{year}/tarfiles/.')

    tif_lists = {
        'HH': [],
        'HV': [],
        'INC': [],
        'DOY': [],
    }

    # Processing mosaic data and get list of processed .tif
    print(f'\nProcessing ALOS-2 yearly mosaic data in {p.proj_dir}/alos2_mosaic/{year}/tarfiles ...')
    for tarfile in tarfile_list:
        if tarfile_pattern.fullmatch(tarfile):
            hh_tif, hv_tif, inc_tif, doy_tif = proc_tarfile(tarfile, year, proj_dir, vsi_path, filter_win_size, filter_num_looks)
            tif_lists['HH'].append(hh_tif)
            tif_lists['HV'].append(hv_tif)
            tif_lists['INC'].append(inc_tif)
            tif_lists['DOY'].append(doy_tif)

    # Make VRT for HH, HV, INC, DOY
    for var, tif_list in tif_lists.items():
        vrt = Path(f'alos2_mosaic_{year}_{var}.vrt')
        cmd = f'gdalbuildvrt -overwrite {vrt} {" ".join(tif_list)}'
        subprocess.check_call(cmd, shell=True)
        if p.is_cloud:
            dst_vrt = f'{p.proj_dir}/alos2_mosaic/{year}/{vrt}'
            cmd = (f'gsutil -q cp {vrt} {dst_vrt}')
            subprocess.check_call(cmd, shell=True)
            vrt.unlink()
        else:
            dst_vrt = p.proj_dir / f'alos2_mosaic/{year}/{vrt}'
            shutil.move(vrt, dst_vrt)

    print('\nDONE processing ALOS-2 yearly mosaic data.')


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
                        type=int,
                        help='year')
    parser.add_argument('--filter_win_size', metavar='win_size',
                        type=int,
                        default=5,
                        help='Filter window size')
    parser.add_argument('--filter_num_looks', metavar='num_looks',
                        type=int,
                        default=1,
                        help='Filter number of looks')
    args = parser.parse_args()

    proc_tiles(args.proj_dir, args.year, args.filter_win_size, args.filter_num_looks)


if __name__ == '__main__':
    main()