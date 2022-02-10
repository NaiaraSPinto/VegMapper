### Get granule IDs from ASF API based on path, frame, and year ###

import requests
import io
import pandas as pd # using pandas to concatenate CSV files
import sys
from datetime import datetime

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--path',type=int,default=171,required=True)
parser.add_argument('--frame',type=int,default=617,required=True)
parser.add_argument('--year',type=int,default=2017,required=True)
args = parser.parse_args()

polarization = "VV+VH"
processing_level = "GRD_HD"
flightDirection = "DESCENDING"
output = "CSV"
platform = "SA,SB"
start=datetime(args.year,1,1,0,0,0).isoformat() + 'UTC'
end=datetime(args.year,12,31,11,59,59).isoformat() + 'UTC'

params = {'polarization':polarization,
      'processingLevel':processing_level,
      'flightDirection':flightDirection,
      'output':output,
      'relativeOrbit':args.path,
      'asfframe':args.frame,
      'platform':platform,
      'start':start,
      'end':end}

url = "https://api.daac.asf.alaska.edu/services/search/param"

r = requests.get(url,params=params)
        
df = pd.read_csv(io.StringIO(r.text)) # convert csv string to data frame

granules = df['Granule Name'].tolist()
for g in granules: print(g)

#df.to_csv('out.csv')
