import os
import sys
import numpy as np
import rasterio
import dask
import dask.array as da
import certifi
import boto3
from botocore.exceptions import ClientError

"""
This file calculates yearly NDVI statistics (min, mean, max, standard deviation) 
per pixel for Landsat tiles in an S3 bucket and outputs a .tif file. This 
file is expected to be used after data is downloaded via 
landsat-download-ndvi.py.

This file takes as input a listfile and yearfile specifying the tile and year
that statistics should be calculated for. It also takes the name of the bucket
and the particular folder within the bucket where images are stored.

If a year's worth of images for a particular tile are not available
the program will crash, so please ensure the images are available.
This can be fixed in the future. 

Example run:
python3 landsat-calculate-yearly-stats.py listfile yearfile bucket folder
python3 landsat-calculate-yearly-stats.py ucayali_tile_list.txt ucayali_year_list.txt sar-optical-images GLAD/

ucayali_tile_list.txt
----------------------
070W_07S
070W_08S
070W_09S
070W_10S
070W_11S
071W_07S

ucayali_year_list.txt
----------------------
2018
2019

"""

listfile = sys.argv[1]
yearfile = sys.argv[2]
bucket = sys.argv[3]
folder = sys.argv[4]

with open(listfile, 'r') as f:
    tiles = [tile.strip() for tile in f.readlines()]

with open(yearfile, 'r') as f:
    years = [int(year.strip()) for year in f.readlines()]

# from 16d_intervals.xlsx file, these are the intervals that define a year 
years_intervals = [] # list of years and associated interval. Ex: [(2015, 806, 828), (2016, 829, 851), ...]
for year in years:
    start = ((year - 1980) * 23) + 1 # from GLAD documentation
    end = start + 23 - 1 # 23 intervals per year
    years_intervals.append((year, start, end))

# the functions to compute the min, mean, max, std. dev composites we are making
functions = [
    ("min", np.nanmin),
    ("mean", np.nanmean),
    ("max", np.nanmax),
    ("std_dev", np.nanstd)
]

# establish connection to S3
s3 = boto3.client('s3')

os.environ["AWS_REQUEST_PAYER"] = "requester"
#os.environ["CURL_CA_BUNDLE"] = "/etc/ssl/certs/ca-certificates.crt"
rasterio.Env(CURL_CA_BUNDLE=certifi.where()) # for rasterio.open() to work with S3

folder_path = os.path.join('s3://', bucket, folder)
for year, start, end in years_intervals:
    for tile in tiles:
        print("Starting composite calculation for tile {} year {}".format(tile, year))

        # find which functions/composites actually need to be calculated by checking if the composites are already in S3
        functions_to_calculate = []
        for func_name, func in functions:
            filename = "ndvi_{}_composite_{}.tif".format(func_name, year)
            key = os.path.join(folder, tile, filename)
            try:
                s3.head_object(Bucket=bucket, Key=key) 
                print("{} already present. Skipping to next function...".format(filename))
                continue
            except:
                functions_to_calculate.append((func_name, func))

        if len(functions_to_calculate) == 0:
            print("No composites to calculate for tile {} year {}. Skipping to next tile...\n".format(tile, year))
            continue

        ndvi_list = []
        kwargs = None
        for i in range(start, end+1):
            path_to_file_s3 = os.path.join(folder_path, tile, '{}.tif'.format(i))
            # Check if file exists before opening to prevent crash
            try:
                path_to_file = os.path.join(folder, tile, '{}.tif'.format(i)) # without s3 prefix
                # if this succeeds file is present so we can open it with rasterio
                s3.head_object(Bucket=bucket, Key=path_to_file)
            except:
                print("Missing file: {}.".format(key))
                continue
            
            with rasterio.open(path_to_file_s3) as src:
                nodata_value = src.profile['nodata']
                # read data into dask array with 500x500 chunks in memory
                ndvi = da.from_array(src.read(1), chunks=(500, 500))
                ndvi[ndvi == nodata_value] = np.nan
                ndvi_list.append(ndvi)
                print("Downloaded {} {} for year {}".format(tile, i, year))

                # grab the profile from the first image's profile.
                # use it when saving the composte image. 
                # TODO: is this the right way to do this?
                if i == start:
                    kwargs = src.profile
                    
        # stack many existing Dask arrays into a new array along new axis
        ndvi_array = da.stack(ndvi_list, axis=0) 

        for func_name, func in functions_to_calculate:
            ndvi_composite = func(ndvi_array, axis=0)
            print("Done calculating {} composite for year {}".format(func_name, year))

            # if there are still NaN values, replace with 9999 (no data)
            nodata_value = 9999
            ndvi_composite[np.isnan(ndvi_composite)] = nodata_value
            kwargs.update(nodata = nodata_value)
            
            filename = "ndvi_{}_composite_{}.tif".format(func_name, year)
            with rasterio.open(filename, 'w', **kwargs) as dst:
                dst.write_band(1, ndvi_composite.astype(rasterio.float32))
                dst.set_band_description(1, "NDVI {} composite for year {}".format(func_name, year))

            key = os.path.join(folder, tile, filename)
            try:
                response = s3.upload_file(filename, bucket, key)
            except ClientError as e:
                print("S3 Upload Error:", e)
                
            print("Done uploading {} composite for year {}".format(func_name, year))

            os.remove(filename)

    print("Done calculating all composites for {}\n".format(year))

