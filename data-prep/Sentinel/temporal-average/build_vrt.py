from osgeo import gdal
import boto3
import re

"""
Given the name of a .zip file in an AWS bucket, generates the full filename
of the corresponding .tif file inside the .zip file that we want to build the 
VRT with in a format that GDAL understands.

Polarization should either be "VV" or "VH"
"""
def generate_tif_full_filename(bucket, str, polarization):
    regex = re.compile(r"""
                    (.*[/\\])?   # matches any folder names before the granule name
                    (.+)         # matches the granule name
                    (?:\.zip)    # matches the .zip file extension
                    """, re.VERBOSE)
    
    # 'vsizip' tells GDAL that we are working with a .zip file, and
    # 'vsis3' tells GDAL that the file is hosted in an s3 bucket
    leading_str = "/vsizip/vsis3/"
    
    match = re.search(regex, str)
    if not match:
        raise ValueError("unrecognized filename format: " + str)
    
    folder = match.group(1)
    if not folder:
        folder = ""
    granule_name = match.group(2)

    return (leading_str + bucket + "/" + folder + granule_name + ".zip/" + granule_name 
            + "/" + granule_name + "_" + polarization + ".tif")
   
# assuming AWS credentials have been set up separately 
s3 = boto3.resource('s3')
bucket = 'MY_BUCKET'
# if the bucket has more files that we don't care about, use this to filter only files that start with the given prefix string
# can make bucket and prefix_str command-line arguments in the future
prefix_str = ''
bucket_object = s3.Bucket(bucket)

VV_list = []
VH_list = []

for file in bucket_object.objects.filter(Prefix=prefix_str):
    VV_list.append(generate_tif_full_filename(bucket, file.key, "VV"))
    VH_list.append(generate_tif_full_filename(bucket, file.key, "VH"))
    
# any other kwargs we're missing? targetAlignedPixels?
VV_VRT = gdal.BuildVRT("VV.vrt", VV_list, separate = True)
VH_VRT = gdal.BuildVRT("VH.vrt", VH_list, separate = True)
VV_VRT = VH_VRT = None
