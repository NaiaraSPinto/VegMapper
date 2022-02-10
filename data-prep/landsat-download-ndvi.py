import os
import sys
import requests
import numpy as np
import rasterio
import boto3
from botocore.exceptions import ClientError

"""
This Python code is very similar to the Perl script (download_V1.1.pl) provided
by GLAD. It takes mostly the same arguments as the Perl script. In addition, 
it requires the name of the S3 bucket to store data and the name of the 'folder'
to store data within the bucket. It will download tiles one by one, calculate 
the NDVI, and save the resulting .tif to S3.

Currently when calculating NDVI there are divide by 0 errors resulting in lots
of NaNs. I left them as is, but this can be changed later.

Example command: 
python3 landsat-download-ndvi.py username password listfile start end outbucket outfolder 
python3 landsat-download-ndvi.py lemarpopal password tile_list.txt 805 810 landsat-images-bucket data/

"""

# TODO: error handling if arguments in wrong format
username = sys.argv[1]
password = sys.argv[2]
listfile = sys.argv[3]
start = int(sys.argv[4])
end = int(sys.argv[5])
outbucket = sys.argv[6]
outfolder = sys.argv[7]

# establish connection to S3
s3 = boto3.client('s3')

with open(listfile, 'r') as f:
    tiles_list = [line.strip() for line in f.readlines()]
    
for tile in tiles_list:
    for i in range(start, end+1):
        key = os.path.join(outfolder, tile, "{}.tif".format(i))
        try:
            # skip download and NDVI calculation if file in bucket already
            # https://stackoverflow.com/questions/44978426/boto3-file-upload-does-it-check-if-file-exists
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object
            metadata = s3.head_object(Bucket=outbucket, Key=key) 
            print("Skipping {} {} download, NDVI file already present in S3.".format(tile, i))
            continue
        except:
            pass
        
        lon = tile.split('_')[1]
        url = 'https://glad.umd.edu/dataset/landsat_v1.1/{}/{}/{}.tif'.format(lon, tile, i)
        r = requests.get(url, auth=(username, password))
        
        print("Downloaded {} {}.".format(tile, i))
        
        # calculate ndvi in memory
        with rasterio.io.MemoryFile(r.content).open() as src:
            b3 = src.read(3)
            b4 = src.read(4)
            
            b8 = src.read(8)
            # boolean array where True pixels are contaminated by clouds and excluded from processing
            mask = (b8 == 3) | (b8 == 4) | ((b8 >= 7) & (b8 <= 10)) 
            
            ndvi = (b4.astype(float) - b3.astype(float)) / (b4.astype(float) + b3.astype(float))
            # ndvi[mask] = np.nan
            ndvi[np.isnan(ndvi) | mask] = 9999 # replace NaN and bad QA pixels with nodata=9999
            
        print("Calculated NDVI.")
            
        # Define spatial characteristics of output object (basically they are analog to the input)
        kwargs = src.meta

        # Update kwargs (change in data type)
        kwargs.update(
            dtype=rasterio.float32,
            compress='lzw',
            nodata=9999,
            count = 1)

        # Let's see what is in there
        # print(kwargs)

        # save temporary file to disk, upload to S3, delete temporary file
        # TODO: figure out how to save to band to memory and upload rather than create temporary file
        temp_filename = 'ndvi_{}_{}.tif'.format(tile, i)
        with rasterio.open(temp_filename, 'w', **kwargs) as dst:
            dst.write_band(1, ndvi.astype(rasterio.float32))
            dst.set_band_description(1, "NDVI with possible NaNs")
            
        # with rasterio.io.MemoryFile() as memfile:
            # with memfile.open(**kwargs) as dst:
                # dst.write(ndvi)
                # s3.put_object(Body=dst, Bucket=outbucket, Key=key)
        
        try:
            response = s3.upload_file(temp_filename, outbucket, key)
        except ClientError as e:
            print("S3 Upload Error:", e)
            
        os.remove(temp_filename) # delete the temporary file
            
        print("Uploaded {} NDVI.".format(key))
    print("Done with tile {}.\n".format(tile))
