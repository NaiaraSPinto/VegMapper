"""
This script takes an input a list of coordinates that form a polygon (make sure the first 
coordinate is the same as the last). In the same list, it takes the flight direction, start
and end dates, as well as the API username to perform the search for images that cover the
polygon. 

This was mainly used for testing if we could get a list of granule names and submit for 
processing via Hyp3. 
"""

import sys
import requests
from asf_hyp3 import API
from asf_hyp3 import scripts
from asf_hyp3.scripts import download_products

if len(sys.argv) == 2:
    metadata_file = sys.argv[1]
else:
    print("Usage: python3 image_search_and_processing.py metadate_file")

### Hardcoded values
# polarization
polarization = "VV%2bVH" # VV+VH

# platform # question: do we want images from both Sentinel 1 satellites, or just one?
platform = "S1" # both platforms for this one 

# processingLevel. only want GRD-HD images
processing_level = "GRD_HD"

# output format
output = "JSON"
###

### Read metadata from file, other metadata is hardcoded in file
metadata_dict = {
    "coordinate_list": [],
    "flightDirection": None,
    "start": None,
    "end": None
}
with open(metadata_file, "r") as metadata:
    for line in metadata:
        line = line.strip()
        if len(line) == 0:
            continue
            
        key, val = line.split(": ")
        
        if key == "Coordinate":
            metadata_dict["coordinate_list"].append(val.strip())
            continue
            
        metadata_dict[key.strip()] = val.strip()
###

base_url_list = ["https://api.daac.asf.alaska.edu/services/search/param?"]

# add polarization parameter
base_url_list.append("polarization=" + polarization + "&")

# add flight direction parameter
base_url_list.append("flightDirection=" + metadata_dict["flightDirection"] + "&")

# add platform
base_url_list.append("platform=" + platform + "&")

# add polygon to url

polygon = []
# trying to make this: intersectsWith=polygon((-119.543 37.925, -118.443 37.7421, -118.682 36.8525, -119.77 37.0352, -119.543 37.925 ))
# using URL encoding 
polygon.append("polygon%28%28")
for coord in metadata_dict["coordinate_list"]:
    coord = coord.replace(" ", "+") # replace spaces in url with "+"
    polygon.append(coord)
    polygon.append(",")
    
# remove the last ","
polygon.pop()
# add closing parentheses 
polygon.append("%29%29")
# convert list of strings to string
polygon_str = "".join(polygon)
# append parameter to url
base_url_list.append("intersectsWith=" + polygon_str + "&")

# add dates to url
base_url_list.append("start=" + metadata_dict["start"] + "&" + "end=" + metadata_dict["end"] + "&")

# add processing level to url
base_url_list.append("processingLevel=" + processing_level + "&")

# add output type
base_url_list.append("output=" + output)

# convert list of strings to string
base_url = "".join(base_url_list)

# submit url and get results back
r = requests.get(base_url)

# convert to JSON
json_output = r.json()[0]

# get a list of granule names/ids to request processing in Hyp3
scene_names = [scene['granuleName'] for scene in json_output]

### Log in to API
api = API(metadata_dict["API_username"])

print("Logging into ASF Hyp3 API...")
api.login()
###

### Test submission of a single granule for processing
api.one_time_process('S1B_IW_GRDH_1SDV_20200201T102919_20200201T102944_020074_025FE4_24FF', 2)

# uncomment below to submit all granules for processing
# api.one_time_process(scene_names)
###

### Download products after processing
# download_products.download_products(api)
###