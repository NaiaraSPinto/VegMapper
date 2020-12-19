#!/usr/bin/env 

"""
This script takes an input a list of Path and Frame combinations as well as a specified 
start and end date and outputs "output.csv" which contains metadata about all 
Sentinel images in that area. 

The start and end date range should occur before or after 2016-04-24, the date Sentinel-1B
started collecting images. The path and frames are also different between 1A and 1B so ensure
the correct list of path and frame and correct dates are passed to the script. 
"""

import requests
import io
import pandas as pd # using pandas to concatenate CSV files
import sys

# polarization
polarization = "VV%2bVH" # VV+VH

# processingLevel. only want GRD-HD images
processing_level = "GRD_HD"

flightDirection = "DESCENDING"

# output format
output = "CSV"

# first available date for data from Sentinel-1B
s1b_data_available_date = "2016-04-28T00:00:00UTC"

# Read metadata from file, other metadata is hardcoded above
metadata_dict = {
    "path_frame_list": [], # list of lists, each of length 2, containing path and frame
    "start": None,
    "end": None
}
with open(sys.argv[1], "r") as metadata:
    for line in metadata:
        line = line.strip()
        if len(line) == 0:
            continue
            
        key, val = line.split(": ")
        
        if key == "PathFrame":
            metadata_dict["path_frame_list"].append(val.split(" ")) # append list e.g. ['25', '621']
            continue
            
        metadata_dict[key.strip()] = val.strip()

base_url_list = ["https://api.daac.asf.alaska.edu/services/search/param?"]

# add polarization parameter
base_url_list.append("polarization=" + polarization + "&")

# add processing level
base_url_list.append("processingLevel=" + processing_level + "&")

# add flight direction parameter
base_url_list.append("flightDirection=" + flightDirection + "&")

# add output format 
base_url_list.append("output=" + output + "&")

# a list of dataframes from each API call. Needs to be combined later. 
df_list = [] 

# If start date is before the first available date data is available for Sentinel-1B, 
# then get images from Sentinel-1A until images are available for 1B
if metadata_dict["start"] < s1b_data_available_date: # Since dates are in year-month-day format we can do a simple string comparison 
    print("start date is before s1 data available date")
    s1a_url_list_copy = base_url_list.copy() # make a copy of the base url specifically for s1a data
    # add platform
    platform = "SA"
    s1a_url_list_copy.append("platform=" + platform + "&")
    
    # add dates to url
    s1a_url_list_copy.append("start=" + metadata_dict["start"] + "&" + "end=" + s1b_data_available_date + "&")
    
    print("collecting s1a data between start", metadata_dict["start"], "and s1b_data_available_date", s1b_data_available_date)
    
    # for each path frame, add it to the url and call API
    for path, frame in metadata_dict["path_frame_list"]:
        print("path", path, "frame", frame)
        s1a_url_list_with_path_frame = s1a_url_list_copy.copy() # url will change each loop so make a copy before changing it
        
        s1a_url_list_with_path_frame.append("relativeOrbit=" + path + "&")
        s1a_url_list_with_path_frame.append("frame=" + frame + "&")
        
        s1a_url = "".join(s1a_url_list_with_path_frame)
        print(s1a_url)
        r = requests.get(s1a_url)
        
        df = pd.read_csv(io.StringIO(r.text)) # convert csv string to data frame
        
        print("got", len(df), "images")
        
        df_list.append(df)

# Now get images from Sentinel-1B between start date and end date. 
# There should not be any images for 1B between (start) and (the first available date images are available for 1B). 
print("collecting s1b data between start", metadata_dict["start"], "and end", metadata_dict["end"])
# add platform
platform = "SB"
base_url_list.append("platform=" + platform + "&")

# add dates to url 
base_url_list.append("start=" + metadata_dict["start"] + "&" + "end=" + metadata_dict["end"] + "&")

for path, frame in metadata_dict["path_frame_list"]:
    print("path", path, "frame", frame)
    s1b_url_list_with_path_frame = base_url_list.copy()
    
    s1b_url_list_with_path_frame.append("relativeOrbit=" + path + "&")
    s1b_url_list_with_path_frame.append("frame=" + frame + "&")
    
    s1b_url = "".join(s1b_url_list_with_path_frame)
    print("request url:", s1b_url)
    r = requests.get(s1b_url)
        
    df = pd.read_csv(io.StringIO(r.text)) # convert csv string to data frame
    
    print("got", len(df), "images")

    df_list.append(df)

# combine all CSVs into a single one
df = pd.concat(df_list).sort_values("Acquisition Date")

# Write dataframe to csv file
df.to_csv("output.csv", index=False)

