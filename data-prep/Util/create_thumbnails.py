from osgeo import gdal
import boto3
from botocore.exceptions import ClientError

import os

# TODO: get rid of hardcoded values, maybe config file or command-line args
BUCKET_NAME = "servir-public"
PREFIX = "geotiffs/peru/sentinel_1/2019/171_617/2019_171_617_VV"
RESAMPLE_SIZE = 500

""" Given an s3 bucket and a folder, resample the geotiffs in the folder to the specified resolution. """
def create_thumbnails(bucket, prefix, resolution):
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(bucket)
    for obj in s3_bucket.objects.filter(Prefix=prefix):
        # avoid making duplicate thumbnails
        if obj.key.endswith(".tif") and not obj.key.endswith("_thumbnail.tif"):
            # separate folder and object key
            path = obj.key.split('/')
            filename = path[-1] # always last element of key
            # everything up to the direct filename is the folder
            folder = "/".join(path[0:len(path) - 1])
            thumbnail = resample(filename, bucket, folder, resolution)
            upload_to_s3(thumbnail, bucket, folder, s3)
    print("Done.")

""" Given the name of a geotiff in an s3 bucket, resample it to the specified resolution.
    Returns the name of the file created. """
def resample(filename, bucket, folder, resolution):
    # 'vsis3' tells gdal the file is located in an s3 bucket
    # this will insert backslashes on windows machines, but s3 uses forward slashes
    full_path = os.path.join("/vsis3/", bucket, folder, filename).replace("\\", "/")
    thumbnail_path = os.path.splitext(filename)[0] + "_thumbnail.tif"
    ds = gdal.Open(full_path)
    print("Creating thumbnail for " + filename + "...")
    # TODO: this will throw warning messages related to NODATA values. Is there a way to clean those up?
    thumbnail = gdal.Warp(thumbnail_path, ds, xRes=resolution, yRes=resolution)
    ds = thumbnail = None # free data so it saves properly
    return thumbnail_path
    
""" Given the name of a file on local storage, upload it to an s3 bucket then delete the local copy. """
def upload_to_s3(filename, bucket, folder, s3):
    try:
        key = os.path.join(folder, filename).replace("\\", "/")
        # TODO: should we place thumbnails in their own folder,
        # or is it okay to keep them in the same folder as the source files?
        print("Uploading " + filename + " to s3...")
        s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=key)
    except ClientError as e:
        # TODO: better error handling?
        print("Error while trying to upload " + filename + ": " +  e)
    os.remove(filename)
    
def main():
    create_thumbnails(BUCKET_NAME, PREFIX, RESAMPLE_SIZE)
            
            

if __name__ == "__main__":
    main()
