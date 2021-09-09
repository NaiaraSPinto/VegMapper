import os
import ntpath
import time
import subprocess
import re
import argparse
import traceback
from pathlib import Path

import numpy as np
import rasterio

import boto3
from botocore.exceptions import ClientError

from s1_metadata_summary import generate_granules_group_dict

def main():    
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--dst', metavar='dstpath', dest='dstpath',
                        type=str,
                        help=('destination path to store VRTS (ex s3://dstpath)'),
                        required=True)
    parser.add_argument('metadata', metavar='csv/geojson',
                        type=Path,
                        help='metadata file downloaded from ASF Vertex after data search')
    args = parser.parse_args()
    metadata = args.metadata

    # TODO: add support for GCS or local storage?
    if args.dstpath[0:5] == 's3://':
        try:
            dst = 's3'
            s3_path = Path(args.dstpath[5:])
            s3_bucket = str(Path(s3_path.parts[0]))
            s3_prefix = str(Path(*s3_path.parts[1:]))
            s3 = boto3.resource('s3')
        except:
            raise Exception("Connection to S3 failed. Use 'aws configure' to configure.")
    else:
        raise Exception("Support for sytems other than S3 not supported at this time."
                        "Prefix your bucket path with s3://")

    granules_group_dict = generate_granules_group_dict(metadata)

    for year_path_frame in granules_group_dict.keys():
        year, path_frame = year_path_frame.split('_', 1)
        try:
            print(year, path_frame, "building vrt and uploading to s3")
            VV_VRT_filename, VH_VRT_filename, INC_VRT_filename = build_vrt_and_upload_to_s3(s3, s3_bucket, s3_prefix, year, path_frame)
            print(year, path_frame, "DONE building vrt and uploading to s3")
        
            print(year, path_frame, "calc temp avg and uploading to s3")
            calc_temp_avg_and_upload_to_s3(s3, s3_bucket, s3_prefix, year, path_frame, VV_VRT_filename, VH_VRT_filename, INC_VRT_filename)
            print(year, path_frame, "DONE calc temp avg and uploading to s3")
        # TODO: what sort of errors might occur? need to be more descriptive/precise here
        except Exception as e:
            print("There was an error building the VRT and calculating the temporal average. Continuing to the next granule...")
            traceback.print_exc()
            
    print("done with everything")


def build_vrt_and_upload_to_s3(s3, dest_bucket, prefix_str, year, path_frame):

    VV_VRT_filename, VH_VRT_filename, INC_VRT_filename = build_vrt(s3, dest_bucket, prefix_str, year, path_frame)

    VV_VRT_s3_key = os.path.join(prefix_str, year, path_frame, VV_VRT_filename)
    VH_VRT_s3_key = os.path.join(prefix_str, year, path_frame, VH_VRT_filename)
    INC_VRT_s3_key = os.path.join(prefix_str, year, path_frame, INC_VRT_filename)

    upload_to_s3(s3, VV_VRT_filename, dest_bucket, VV_VRT_s3_key)
    upload_to_s3(s3, VH_VRT_filename, dest_bucket, VH_VRT_s3_key)
    upload_to_s3(s3, INC_VRT_filename, dest_bucket, INC_VRT_s3_key)

    return VV_VRT_filename, VH_VRT_filename, INC_VRT_filename

def build_vrt(s3, bucket_str, prefix_str, year, path_frame):
    bucket_object = s3.Bucket(bucket_str)

    VV_list = []
    VH_list = []
    INC_list = []

    folder = os.path.join(prefix_str, year, path_frame)
    for file in bucket_object.objects.filter(Prefix=folder):
        if file.key.endswith('.zip'):
            VV_list.append(generate_tif_full_filename(bucket_str, file.key, "VV"))
            VH_list.append(generate_tif_full_filename(bucket_str, file.key, "VH"))
            INC_list.append(generate_tif_full_filename(bucket_str, file.key, "inc_map"))

    # TODO: any other kwargs we're missing? targetAlignedPixels?
    VV_VRT_filename = f"{year}_{path_frame}_VV.vrt"
    gdal_build_vrt(VV_VRT_filename, VV_list)

    VH_VRT_filename = f"{year}_{path_frame}_VH.vrt"
    gdal_build_vrt(VH_VRT_filename, VH_list)

    INC_VRT_filename = f"{year}_{path_frame}_INC.vrt"
    gdal_build_vrt(INC_VRT_filename, INC_list)

    # prevent race condition where we attempt to upload the files to s3 before gdal creates the vrt
    block_until_file_created(VV_VRT_filename)
    block_until_file_created(VH_VRT_filename)
    block_until_file_created(INC_VRT_filename)

    # returns the keys to the VV and VRT files in S3
    return VV_VRT_filename, VH_VRT_filename, INC_VRT_filename

"""
Given the name of a .zip file in an AWS bucket, generates the full filename
of the corresponding .tif file inside the .zip file that we want to build the
VRT with in a format that GDAL understands.
Polarization should either be "VV", "VH", or "inc_map"
"""
def generate_tif_full_filename(bucket, filename, polarization):
    # TODO: Make this a parameter.
    # Hardcoded variable controls whether we are building links for S3 or local filesystem
    cloud = True
    local_path = "/data3/lepopal_vault/datasets/2018_data"

    regex = re.compile(r"""
                    (.*[/\\])?   # matches any folder names before the granule name
                    (.+)         # matches the granule name
                    (?:\.zip)    # matches the .zip file extension
                    """, re.VERBOSE)

    match = re.search(regex, filename)
    if not match:
        raise ValueError("unrecognized filename format: " + filename)

    folder = match.group(1)
    if not folder:
        folder = ""
    granule_name = match.group(2)

    if cloud:
        # build the full filename of the .tif file we want
        # 'vsizip' tells GDAL that we are working with a .zip file
        # 'vsis3' tells GDAL that the file is hosted in an s3 bucket
        # return ("/vsizip/vsis3/" + bucket + "/" + folder + granule_name + ".zip/" + granule_name
        #         + "/" + granule_name + "_" + polarization + ".tif")
        return os.path.join(
            "/vsizip/vsis3/", bucket, folder, granule_name + ".zip/", granule_name,
            granule_name + "_" + polarization + ".tif"
        )
    else:
        return os.path.join(
            "/vsizip/" + local_path, granule_name + ".zip/", granule_name,
            granule_name + "_" + polarization + ".tif"
        )

def gdal_build_vrt(filename, tif_list):
    # TODO: Move these static variables to global?
    pixfun_name = 'average'
    pixfun = """
import numpy as np
def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    div = np.zeros(in_ar[0].shape)
    for i in range(len(in_ar)):
        div += (in_ar[i] != 0)
    div[div == 0] = 1
    out_ar[:] = np.sum(in_ar, axis=0, dtype='double') / div
"""

    contents = f"""    <PixelFunctionType>{pixfun_name}</PixelFunctionType>
        <PixelFunctionLanguage>Python</PixelFunctionLanguage>
        <PixelFunctionCode><![CDATA[{pixfun}]]>
        </PixelFunctionCode>
    """

    cmd = (f'gdalbuildvrt -overwrite '
          f'{filename} {" ".join(tif_list)}')
    subprocess.check_call(cmd, shell=True)

    with open(filename) as f:
        lines = f.readlines()

    lines[3] = lines[3].replace('band="1"',
                                'band="1" subClass="VRTDerivedRasterBand"')

    lines.insert(4, contents)

    with open(filename, 'w') as f:
        f.writelines(lines)

"""
Blocks the thread until we can confirm that filename exists.
Used to prevent a race condition in build_vrt.
"""
def block_until_file_created(filename, max_attempts=5):
    attempts = 0

    while attempts < max_attempts:
        if os.path.isfile(filename):
            # exit the function
            return None
        attempts += 1
        time.sleep(5)

    # if we exceed max_attempts and file still not found
    # TODO: Raise FileNotFound Error
    raise RuntimeError("{} not found".format(filename))

def calc_temp_avg_and_upload_to_s3(s3, dest_bucket, prefix_str, year, path_frame, VV_VRT_filename, VH_VRT_filename, INC_VRT_filename):
    print("VV...", end=None)
    vv_tif_filename = calc_temp_avg(VV_VRT_filename)
    vv_tif_s3_key = os.path.join(prefix_str, year, path_frame, vv_tif_filename)
    upload_to_s3(s3, vv_tif_filename, dest_bucket, vv_tif_s3_key)
    print("DONE")

    print("VH...", end=None)
    vh_tif_filename = calc_temp_avg(VH_VRT_filename)
    vh_tif_s3_key = os.path.join(prefix_str, year, path_frame, vh_tif_filename)
    upload_to_s3(s3, vh_tif_filename, dest_bucket, vh_tif_s3_key)
    print("DONE")

    print("INC...", end=None)
    inc_tif_filename = calc_temp_avg(INC_VRT_filename)
    with rasterio.open(inc_tif_filename, 'r+') as dset:
        dset.write(np.rad2deg(dset.read(1)), 1)
    inc_tif_s3_key = os.path.join(prefix_str, year, path_frame, inc_tif_filename)
    upload_to_s3(s3, inc_tif_filename, dest_bucket, inc_tif_s3_key)
    print("DONE")

    # after uploading VRTs and averaging, delete them from local filesystem
    os.remove(VV_VRT_filename)
    os.remove(VH_VRT_filename)
    os.remove(INC_VRT_filename)

    # after uploading .tifs, delete them from local filesystem
    os.remove(vv_tif_filename)
    os.remove(vh_tif_filename)
    os.remove(inc_tif_filename)

def calc_temp_avg(vrt_filename):
    tif_filename = os.path.splitext(ntpath.basename(vrt_filename))[0] + ".tif"
    cmd = (f'gdal_translate --config '
          f'GDAL_VRT_ENABLE_PYTHON YES '
          f'{vrt_filename} {tif_filename}')
    subprocess.check_call(cmd, shell=True)

    return tif_filename

def upload_to_s3(s3, local_filepath, bucket, key):
    try:
        s3.meta.client.upload_file(Filename=local_filepath, Bucket=bucket, Key=key)
    except ClientError as e:
        print(f"error uploading {local_filepath} to s3:", e)

if __name__ == '__main__':
   main()
