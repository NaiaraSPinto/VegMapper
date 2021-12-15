import os
import h5py
import numpy as np
import pandas as pd

inDir = os.getcwd() + os.sep  # Set input directory to the current working directory
pd.options.mode.chained_assignment = None  # default='warn'

gediFiles = [g for g in os.listdir() if g.startswith('GEDI02_A') and g.endswith('.h5')]  
# List GEDI L2A .h5 files in the inDir
print(gediFiles)

for j in range(len(gediFiles)):
###READ DATA
   gediL2A = h5py.File(gediFiles[j], 'r')  
   beamNames = [g for g in gediL2A.keys() if g.startswith('BEAM')]

   highBeam = 'BEAM0110'                                                        #Choose high beam

   gediL2A_objs = []
   gediL2A.visit(gediL2A_objs.append)                                           # Retrieve list of datasets
   gediSDS = [o for o in gediL2A_objs if isinstance(gediL2A[o], h5py.Dataset)]  # Search for relevant SDS inside data file
                                   
   ###FILTER DATA 
   # Open the SDS
   lats = gediL2A[f'{highBeam}/lat_lowestmode'][()]
   lons = gediL2A[f'{highBeam}/lon_lowestmode'][()]
   shots = gediL2A[f'{highBeam}/shot_number'][()]
   quality = gediL2A[f'{highBeam}/quality_flag'][()]
   solar_elev = gediL2A[f'{highBeam}/solar_elevation'][()]
   sensitivity = gediL2A[f'{highBeam}/sensitivity'][()]
   rh = gediL2A[f'{highBeam}/rh'][()]
   elevLow = gediL2A[f'{highBeam}/elev_lowestmode'][()]

   elev1 = gediL2A[f'{highBeam}/geolocation/elev_lowestmode_a1'][()]
   elev2 = gediL2A[f'{highBeam}/geolocation/elev_lowestmode_a2'][()]
   elev3 = gediL2A[f'{highBeam}/geolocation/elev_lowestmode_a3'][()]
   elev4 = gediL2A[f'{highBeam}/geolocation/elev_lowestmode_a4'][()]
   elev5 = gediL2A[f'{highBeam}/geolocation/elev_lowestmode_a5'][()]
   elev6 = gediL2A[f'{highBeam}/geolocation/elev_lowestmode_a6'][()]

   rh_cols = []
   for i in range(len(rh[0])):
      rh_cols.append('rh'+str(i))

   raw_df = pd.DataFrame({'Shot Number': shots, 'Longitude': lons, 'Latitude': lats,'Quality': quality, 'Solar_Elevation': solar_elev, 'Sensitivity': sensitivity, "elev_lowestmode": elevLow,'ElevationA1': elev1, 'ElevationA2': elev2, 'ElevationA3': elev3, 'ElevationA4': elev4, 'ElevationA5': elev5, 'ElevationA6': elev6})
   raw_df[rh_cols] = pd.DataFrame(rh, index= raw_df.index)
   raw_df = raw_df.dropna()
   filtered = raw_df[(raw_df.Sensitivity >= 0.9)  & (raw_df.Solar_Elevation < 0) & (raw_df.Quality == 1)]
   filtered['MaxElevation'] = filtered[["ElevationA1", "ElevationA2","ElevationA3","ElevationA4","ElevationA5","ElevationA6"]].max(axis=1)
   filtered['MinElevation'] = filtered[["ElevationA1", "ElevationA2","ElevationA3","ElevationA4","ElevationA5","ElevationA6"]].min(axis=1)
   filtered['Elev_Range'] = abs(filtered.MaxElevation - filtered.MinElevation)

   GEDI_DF = filtered[filtered.Elev_Range <= 2]

   GEDI_DF[["Shot Number", "Latitude", "Longitude", "elev_lowestmode"]+rh_cols].to_csv(gediFiles[j][:-3] + ".csv", index=False)
   os.remove(gediFiles[j])
