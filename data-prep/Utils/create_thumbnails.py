from osgeo import gdal
import boto3
from botocore.exceptions import ClientError

import os
import argparse

""" Given an s3 bucket and a folder, resample the geotiffs in the folder to the specified resolution. """
def create_thumbnails(bucket_path, resolution=500, folder=""):
    bucket_path = bucket_path.split("/", 1)
    bucket = bucket_path[0]
    if len(bucket_path) > 1:
        prefix = bucket_path[1]
    else:
        prefix = ""
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(bucket)
    for obj in s3_bucket.objects.filter(Prefix=prefix):
        # avoid making duplicate thumbnails
        if obj.key.endswith(".tif") and not obj.key.endswith("_thumbnail.tif"):
            # separate folder and object key
            path = obj.key.split('/')
            filename = path[-1] # always last element of key
            # everything up to the direct filename is the prefix
            prefix = s3_join(*path[0:len(path) - 1])
            thumbnail = resample(filename, bucket, prefix, resolution)
            # upload thumbnail to subfolder, if specified
            prefix = s3_join(prefix, folder)
            upload_to_s3(thumbnail, bucket, prefix, s3)
    print("Done.")

""" Given the name of a geotiff in an s3 bucket, resample it to the specified resolution.
    Returns the name of the file created. """
def resample(filename, bucket, prefix, resolution):
    # 'vsis3' tells gdal the file is located in an s3 bucket
    full_path = s3_join("/vsis3", bucket, prefix, filename)
    thumbnail_path = os.path.splitext(filename)[0] + "_thumbnail.tif"
    ds = gdal.Open(full_path)
    print("Creating thumbnail for " + filename + "...")
    # TODO: this will throw warning messages related to NODATA values. Is there a way to clean those up?
    thumbnail = gdal.Warp(thumbnail_path, ds, xRes=resolution, yRes=resolution)
    ds = thumbnail = None # free data so it saves properly
    return thumbnail_path
    
""" Given the name of a file on local storage, upload it to an s3 bucket then delete the local copy. """
def upload_to_s3(filename, bucket, prefix, s3):
    try:
        key = s3_join(prefix, filename)
        print("Uploading " + filename + " to s3...")
        s3.meta.client.upload_file(Filename=filename, Bucket=bucket, Key=key)
    except ClientError as e:
        # TODO: better error handling?
        print("Error while trying to upload " + filename + ": " +  e)
    os.remove(filename)

""" Intelligently join objects of elements with forward slashes.
    os.path.join will use backslashes on Windows machines, which must be corrected. """
def s3_join(*elements):
    if len(elements) > 0:
        return os.path.join(*elements).replace("\\", "/")
    return ""

def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('bucket_path', type=str, help='S3 bucket path in which geotiffs are stored to create thumbnails of (e.g. servir-stacks/peru/ucayali/2019/)')
    parser.add_argument('-r', '--resolution', type=int, help='Resolution of the thumbnails (default: 500)', default=500)
    parser.add_argument('-f', '--folder', type=str, help=('A subfolder to place the thumbnail images in (optional)'), default="")
    parser.add_argument('-h', '--help', action='help', help='Display help information.')
    args = parser.parse_args()

    create_thumbnails(args.bucket_path,  args.resolution, args.folder)
            
if __name__ == "__main__":
    main()
