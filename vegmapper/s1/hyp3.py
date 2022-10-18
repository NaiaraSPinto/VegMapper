#!/usr/bin/env python

import json
import shutil
import subprocess
from copy import copy
from datetime import datetime
from pathlib import Path

import asf_search as asf
import geopandas as gpd
import pandas as pd
from hyp3_sdk import HyP3, Batch

from vegmapper import pathurl
from vegmapper.asf import granule_search
from .search import group_granules


def batch_to_dict(batch):
    """
    Convert a Batch to a dictionary of job information - {(path, frame): Batch}.
    """
    granule_list = [job.to_dict()['job_parameters']['granules'][0] for job in batch]
    gdf_results = granule_search(granule_list).set_index('sceneName')

    batch_dict = {}
    for granule, job in zip(granule_list, batch):
        p = gdf_results.loc[granule, 'pathNumber']
        f = gdf_results.loc[granule, 'frameNumber']
        if (p, f) not in batch_dict:
            batch_dict[(p, f)] = {
                'batch': Batch([job]),
                'granules': [granule]
            }
        else:
            batch_dict[(p, f)]['batch'] += Batch([job])
            batch_dict[(p, f)]['granules'].append(granule)

    return batch_dict


def batch_to_df(batch: Batch):
    """
    Convert a Batch to a DataFrame of product information.
    [filename, job_id, sceneName, pathNumber, frameNumber, startTime, stopTime]
    """
    if not batch.complete():
        raise Exception(f'Batch is not complete. Wait for it to be completed and try again.')

    granule_list = [job.to_dict()['job_parameters']['granules'][0] for job in batch]
    gdf_results = granule_search(granule_list).set_index('sceneName')

    df_products = pd.DataFrame()
    for granule, job in zip(granule_list, batch):
        df = pd.DataFrame(
            {
                'filename': [job.to_dict()['files'][0]['filename']],
                'job_id': [job.to_dict()['job_id']],
                'sceneName': [granule],
                'pathNumber': [gdf_results.loc[granule, 'pathNumber']],
                'frameNumber': [gdf_results.loc[granule, 'frameNumber']],
                'startTime': [gdf_results.loc[granule, 'startTime']],
                'stopTime': [gdf_results.loc[granule, 'stopTime']],
            }
        )
        df_products = pd.concat([df_products, df])

    df_products.sort_values(by=['pathNumber', 'frameNumber', 'startTime']).reset_index(drop=True)

    return df_products


def submit_rtc_jobs(granules,
                    proj_dir: pathurl.ProjDir,
                    hyp3=None,
                    job_name=None,
                    resubmit=False,
                    **rtc_opts):
    """
    Submit RTC jobs for granules in search results (geojson or GeoDataFrame).
    """
    # Sentinel-1 directory
    s1_dir = proj_dir / 'Sentinel-1'

    # Group granules by frames
    gdf_frames = group_granules(granules)

    # Initialize HyP3 API if needed
    if hyp3 is None:
        hyp3 = HyP3(prompt=True)

    # Check quota for HyP3 jobs
    quota = hyp3.check_quota()
    print(f'\nRemaining quota for HyP3 jobs: {quota}')

    # Get submitted and not expired jobs on HyP3 server
    batch = hyp3.find_jobs().filter_jobs(include_expired=False)
    jobs_on_hyp3 = {job.to_dict()['job_id']: job.to_dict()['job_parameters'] for job in batch}

    # Load rtc_jobs.json on s1_dir
    rtc_jobs_file = s1_dir / 'rtc_jobs.json'
    if rtc_jobs_file.exists():
        pathurl.copy(s1_dir / 'rtc_jobs.json', '.', overwrite=True)
        with open('rtc_jobs.json') as f:
            jobs_on_proj_dir = json.load(f)
        Path('rtc_jobs.json').unlink()
    else:
        jobs_on_proj_dir = {}

    num_granules_requested = gdf_frames.num_granules.sum()
    print(f'\n{num_granules_requested} granules requested for RTC processing:')
    for _, row in gdf_frames.iterrows():
        print(f"    Path {row['pathNumber']}, Frame {row['frameNumber']} - {row['num_granules']} granules")

    if not resubmit:
        for i, row in gdf_frames.iterrows():
            granules_requested = row['granules'].split(',')
            granules_to_be_submitted = copy(granules_requested)
            for granule in granules_requested:
                job_parameters = {'granules': [granule], **rtc_opts}
                if job_parameters in jobs_on_hyp3.values():
                    granules_to_be_submitted.remove(granule)
                    print(f'Skipping {granule} - it was previously submitted for RTC processing and is still available on HyP3 server.')
                    continue
                if job_parameters in jobs_on_proj_dir.values():
                    granules_to_be_submitted.remove(granule)
                    print(f'Skipping {granule} - it was previously submitted for RTC processing and could still be available under project directory. To re-submit these ganules, quit below and re-run with the "resubmit" switch set to True.')
                    continue
            gdf_frames.loc[i, 'granules'] = ','.join(granules_to_be_submitted)
            gdf_frames.loc[i, 'num_granules'] = len(granules_to_be_submitted)

    num_granules_to_be_submitted = gdf_frames.num_granules.sum()

    if num_granules_to_be_submitted == 0:
        print('\nNo jobs submitted.')
        return

    if quota >= num_granules_to_be_submitted:
        print(f'\n{num_granules_to_be_submitted} granules will be submitted for RTC processing. After submission, the quota will come down to {quota - num_granules_to_be_submitted}.')
    else:
        print(f'\n{num_granules_to_be_submitted} granules requested for RTC processing, but the remaining quota only has {quota} jobs left. If proceed below, only the first {quota} granules will be submitted.')

    user_input = input('\nEnter "yes" to confirm and proceed to submit the granules for RTC processing. Enter other keys to quit.')
    if user_input.lower() != 'yes':
        print('\nNo jobs submitted.')
        return

    if job_name is None:
        job_name = datetime.now().strftime('%Y%m%dT%H%M%S')

    batch = Batch()
    for _, row in gdf_frames.iterrows():
        for granule in row['granules'].split(','):
            batch += hyp3.submit_rtc_job(granule, job_name, **rtc_opts)
    print(f'\nJob {job_name} submitted.')

    return batch, job_name


def download_batch(batch, dst_dir, quiet=True):
    for job in batch:
        files = job.files
        for file in files:
            url = file['url']
            cmd = f'wget '
            if quiet:
                cmd = cmd + '-q '
            cmd = cmd + f'-c -P {dst_dir} {url}'
            subprocess.call(cmd, shell=True)


def download_files(batch, proj_dir: pathurl.ProjDir, wget=True, quiet=True):
    """
    Download files for a batch and sort them into corresponding path_frame folders in dst directory.
    """
    # Download directly to project directory if it's local
    s1_dir = proj_dir / 'Sentinel-1'
    if s1_dir.is_local:
        download_dir = s1_dir
    else:
        download_dir = Path('hyp3_downloads')

    # Download files
    batch_dict = batch_to_dict(batch)
    for (p, f), d in batch_dict.items():
        dst_dir = download_dir / f'{p}_{f}'
        if not dst_dir.exists():
            dst_dir.path.mkdir(parents=True)
        print(f'Downloading files of (Path {p}, Frame {f}) to {dst_dir}:')
        if wget:
            download_batch(d['batch'], dst_dir, quiet)
        else:
            d['batch'].download_files(f'{dst_dir}')

    # Load rtc_jobs.json in dst if any
    rtc_jobs_file = Path(f'{download_dir}/rtc_jobs.json')
    if rtc_jobs_file.exists():
        with open('rtc_jobs.json') as f:
            dict_jobs = json.load(f)
    else:
        dict_jobs = {}

    # Update rtc_jobs.json
    for _, job in enumerate(batch):
        job_id = job.to_dict()['job_id']
        job_parameters = job.to_dict()['job_parameters']
        if job_id not in dict_jobs:
            dict_jobs[job_id] = job_parameters
    with open(rtc_jobs_file, 'w') as f:
        json.dump(dict_jobs, f)
    print(f'{rtc_jobs_file} updated.')

    # Read rtc_products.csv in dst if any
    rtc_products_file = Path(f'{download_dir}/rtc_products.csv')
    if rtc_products_file.exists():
        df_products = pd.read_csv(rtc_products_file)
    else:
        df_products = pd.DataFrame()

    # Update rtc_products.csv
    df_products = pd.concat([df_products, batch_to_df(batch)]).drop_duplicates(ignore_index=True)
    df_products.sort_values(by=['pathNumber', 'frameNumber', 'startTime']).to_csv(rtc_products_file, index=False)
    print(f'{rtc_products_file} updated.')


def copy_files(proj_dir: pathurl.ProjDir, download_dir='hyp3_downloads'):
    """
    Copy downloaded files to project directory and update rtc_jobs.json and rtc_products.csv.
    """

    download_dir = Path(download_dir)
    s1_dir = proj_dir / 'Sentinel-1'

    for p in download_dir.iterdir():
        if p.is_dir():
            print(f'Copying {p} to {s1_dir / p.name}')
            pathurl.copy(p, s1_dir / p.name, overwrite=True)

    # Load rtc_jobs.json
    src_rtc_jobs = download_dir / 'rtc_jobs.json'
    dst_rtc_jobs = s1_dir / 'rtc_jobs.json'
    with open(src_rtc_jobs) as f:
        dict_jobs_src = json.load(f)
    if dst_rtc_jobs.exists():
        pathurl.copy(dst_rtc_jobs, '.')
        with open('rtc_jobs.json') as f:
            dict_jobs_dst = json.load(f)
    else:
        dict_jobs_dst = {}

    # Update rtc_jobs.json
    for job_id, job_parameters in dict_jobs_src.items():
        if job_id not in dict_jobs_dst:
            dict_jobs_dst[job_id] = job_parameters
    with open('rtc_jobs.json', 'w') as f:
        json.dump(dict_jobs_dst, f)

    # Overwrite rtc_jobs.json on s1_dir
    pathurl.copy('rtc_jobs.json', dst_rtc_jobs, overwrite=True)
    Path('rtc_jobs.json').unlink()
    print(f'{dst_rtc_jobs} updated.')

    # Read rtc_products.csv
    src_rtc_products = download_dir / 'rtc_products.csv'
    dst_rtc_products = s1_dir / 'rtc_products.csv'
    df_products_src = pd.read_csv(src_rtc_products)
    if dst_rtc_products.exists():
        pathurl.copy(dst_rtc_products, '.')
        df_products_dst = pd.read_csv('rtc_products.csv')
    else:
        df_products_dst = pd.DataFrame()

    # Update rtc_products.csv
    df_products = pd.concat([df_products_dst, df_products_src]).drop_duplicates(ignore_index=True)
    df_products.sort_values(by=['pathNumber', 'frameNumber', 'startTime']).to_csv('rtc_products.csv', index=False)

    # Overwrite rtc_products.csv on s1_dir
    pathurl.copy('rtc_products.csv', dst_rtc_products, overwrite=True)
    Path('rtc_products.csv').unlink()
    print(f'{dst_rtc_products} updated.')
