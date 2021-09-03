import argparse
import getpass
import os
import subprocess
import traceback
from datetime import datetime, timezone
from pathlib import Path

import boto3
import geopandas as gpd
import pandas as pd
from hyp3_sdk import HyP3

from s1_metadata_summary import generate_granules_group_dict

# TODO: Use global variables for things like s3, bucket_name?
supported_metadata_formats = ['.csv', '.geojson']
today = datetime.now(timezone.utc)


def main():
    # Setup argument parsing
    parser = argparse.ArgumentParser(
        description='submit ASF HyP3 RTC processing jobs for Sentinel-1 granules')
    parser.add_argument('metadata', metavar='csv/geojson',
                        type=Path,
                        help='metadata file downloaded from ASF Vertex after data search')
    parser.add_argument('--dst', metavar='dstpath', dest='dstpath',
                        type=str,
                        help=('destination path to store processed granules '
                              '(AWS S3 - s3://dstpath, GCS - gs://dstpath, local storage - dstpath)'))
    args = parser.parse_args()

    # Check metadata
    if not args.metadata.exists():
        raise Exception(f'Metadata file {args.metadata} does not exist')
    if args.metadata.suffix not in supported_metadata_formats:
        raise Exception(f'Metadata file format ({args.metadata.suffix}) not supported')

    # Check and identify dstpath
    if args.dstpath:
        if args.dstpath[0:5] == 's3://':
            try:
                dst = 's3'
                s3_path = Path(args.dstpath[5:])
                s3_bucket = str(Path(s3_path.parts[0]))
                s3_prefix = str(Path(*s3_path.parts[1:]))
                s3 = boto3.resource('s3')
            except:
                raise Exception("Connection to S3 failed. Use 'aws configure' to configure.")
        elif args.dstpath[0:5] == 'gs://':
            try:
                dst = 'gs'
                gs_path = Path(args.dstpath[5:])
                gs_bucket = Path(gs_path.parts[0])
                gs_prefix = Path(*gs_path.parts[1:])
                print(f'Listing gs://{gs_bucket}')
                subprocess.check_call(f'gsutil ls gs://{gs_bucket}', shell=True)
            except:
                raise Exception("Listing gs://{gs_bucket} failed. Use 'gsutil config' to configure.")
        elif Path(args.dstpath).exists():
            dst = 'local'
            local_path = Path(args.dstpath)
        else:
            raise Exception(f'Destination path {args.dstpath} does not exist')
    else:
        dst = None
        print(f'Destination path for processed granules not provided. '
              f'The download links will be listed at the end.')

    # Get Earthdata credentials and authenticate
    earthdata_username = input('\nEnter Earthdata Username: ')
    earthdata_password = getpass.getpass('Enter Earthdata Password: ')
    hyp3 = HyP3(username=earthdata_username, password=earthdata_password)

    # Dictionary keyed by year_path_frame (e.g. "2018_25_621"), value is list of granules names for that particular year_path_fame
    granules_group_dict = generate_granules_group_dict(args.metadata)

    # Submit granules
    submit_granules(hyp3, granules_group_dict)

    # Loop through and copy to bucket, or if no bucket specified, show user which ASF bucket granule is in
    for year_path_frame in granules_group_dict.keys():
        year, path_frame = year_path_frame.split('_', 1)
        granule_sources = get_granule_sources(hyp3, year_path_frame)

        if dst == 's3':
            try:
                print(f'\n{year_path_frame}: copying processed granules to {args.dstpath}')
                copy_granules_to_s3(s3, s3_bucket, s3_prefix, year, path_frame, granule_sources)
                print(f'{year_path_frame}: DONE copying processed granules to {args.dstpath}')
            except Exception as e:
                print(f'{year_path_frame}: There was an error when copying processed granules from ASF S3 bucket to {args.dstpath}. Continuing to the next granule ...')
                traceback.print_exc()
        elif dst == 'gs':
            try:
                print(f'\n{year_path_frame}: copying processed granules to {args.dstpath}')
                copy_granules_to_gs(gs_bucket, gs_prefix, year, path_frame, granule_sources)
                print(f'{year_path_frame}: DONE copying processed granules to {args.dstpath}')
            except Exception as e:
                print(f'{year_path_frame}: There was an error when copying processed granules from ASF S3 bucket to {args.dstpath}. Continuing to the next granule ...')
                traceback.print_exc()
        elif dst == 'local':
            try:
                print(f'\n{year_path_frame}: downloading processed granules to {args.dstpath}')
                download_granules(local_path, year, path_frame, granule_sources)
                print(f'{year_path_frame}: DONE downloading processed granules to {args.dstpath}')
            except Exception as e:
                print(f'{year_path_frame}: There was an error when downloaing processed granules from ASF S3 bucket to {args.dstpath}. Continuing to the next granule ...')
                traceback.print_exc()
            
        else:
            print(f'\nYour processed granules for year_path_frame {year_path_frame} are available here:')
            for copy_source, expiration_time, _ in granule_sources:
                print(f"\n{copy_source['Bucket']}/{copy_source['Key']}")
                print(f'Expiration Time: {expiration_time}')
    print('\nDone with everything.')


def submit_granules(hyp3, granules_group_dict):
    """
    Submit HyP3 RTC jobs
    """

    quota = hyp3.check_quota()
    print(f'\nYour remaining quota for HyP3 jobs: {quota} granules.')

    print('\nYou will be submitting the following granules for HyP3 RTC processing:')
    for year_path_frame, granule_name_list in granules_group_dict.items():
        print(f'    {year_path_frame} - {len(granule_name_list)} granules')
    user_input = input((
        "\nEnter 'Y' to confirm you would like to submit these granules, "
        "or 'N' if you have already submitted the granules and want to copy the processed granules to your dstpath: "))
    if user_input.lower() != 'y':
        return

    # submit the jobs for all year_path_frame's
    for year_path_frame, granule_name_list in granules_group_dict.items():
        for granule_name in granule_name_list:
                hyp3.submit_rtc_job(granule_name, year_path_frame, resolution=30.0, radiometry='gamma0',
                                    scale='power', speckle_filter=False, dem_matching=True,
                                    include_dem=True, include_inc_map=True, include_scattering_area=True)

    print('Jobs successfully submitted.')


def get_granule_sources(hyp3, job_name):
    """
    Return a list of dictionaries where files are located on ASF's bucket
    """
    batch = hyp3.find_jobs(name=job_name)
    if not batch.complete():
        print(f"\nThe jobs for {job_name} not complete yet. You can see the progress below (Ctrl+C if you don't want to wait).")
        batch = hyp3.watch(batch)

    # List of tuples:
    # item 1: dictionary with Bucket and Key
    # item 2: datetime of expiration time
    # item 3: URL of processed granule
    return [({'Bucket': job.files[0]['s3']['bucket'], 'Key': job.files[0]['s3']['key']}, job.expiration_time, job.files[0]['url'])
            for job in batch.jobs]


def copy_granules_to_s3(s3, dst_bucket, dst_prefix, year, path_frame, granule_sources):
    for copy_source, expiration_time, _ in granule_sources:
        filename = Path(copy_source['Key']).name
        dst_key = f'{dst_prefix}/{year}/{path_frame}/{filename}'
        if not today > expiration_time:
            try:
                s3.meta.client.copy(copy_source, Bucket=dst_bucket, Key=dst_key)
            except Exception as e:
                print('\nError copying processed granule to your bucket. Traceback:')
                print(traceback.print_exc())
                print(f'Failed granule: {copy_source}')
        else:
            raise Exception('\nJobs already expired and cannot be copied.')


def copy_granules_to_gs(dst_bucket, dst_prefix, year, path_frame, granule_sources):
    for copy_source, expiration_time, _ in granule_sources:
        src_bucket = copy_source['Bucket']
        src_key = copy_source['Key']
        src_path = f's3://{src_bucket}/{src_key}'
        dst_path = f'gs://{dst_bucket}/{dst_prefix}/{year}/{path_frame}'
        if not today > expiration_time:
            try:
                subprocess.check_call(f'gsutil cp {src_path} {dst_path}', shell=True)
            except Exception as e:
                print('\nError copying processed granule to your bucket. Traceback:')
                print(traceback.print_exc())
                print(f'Failed granule: {copy_source}')
        else:
            raise Exception('\nJobs already expired and cannot be copied.')


def download_granules(dst_path, year, path_frame, granule_sources):
    for copy_source, expiration_time, url in granule_sources:
        dst_dir = dst_path / year / path_frame
        if not dst_dir.exists():
            dst_dir.mkdir(parents=True)
        if not today > expiration_time:
            try:
                subprocess.check_call(f'wget -P {dst_dir} {url}', shell=True)
            except Exception as e:
                print(f'\nError downloading processed granule to {dst_dir}. Traceback:')
                print(traceback.print_exc())
                print(f'Failed granule: {copy_source}')
        else:
            raise Exception('\nJobs already expired and cannot be copied.')


if __name__ == '__main__':
    main()

