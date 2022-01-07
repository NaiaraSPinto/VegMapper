import os
import h5py
import numpy as np
import pandas as pd
import glob
import itertools

gediFiles = []
for filepath in glob.glob(os.getcwd()+"/*/*/*"):
    if filepath.endswith((".h5")):
        gediFiles.append(filepath)

gedi_dfs = []
for j in range(len(gediFiles)):
    ###READ DATA
    print(gediFiles[j])
    gediL2A = h5py.File(gediFiles[j], 'r')  
    beamNames = [g for g in gediL2A.keys() if g.startswith('BEAM')]

    gediL2A_objs = []
    gediL2A.visit(gediL2A_objs.append)                                           # Retrieve list of datasets
    gediSDS = [o for o in gediL2A_objs if isinstance(gediL2A[o], h5py.Dataset)]  # Search for relevant SDS inside data file
    
    ###FITLER DATA 
    rh_cols = []
    for i in range(101):
        rh_cols.append('rh'+str(i))
        
    beam_dfs = []
    for highBeam in beamNames:
        
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

        raw_df = pd.DataFrame({'Shot Number': shots, 'Beam':np.full(len(shots), highBeam), 'Longitude': lons, 'Latitude': lats,
                                 'Quality': quality, 'Solar_Elevation': solar_elev, 'Sensitivity': sensitivity, "elev_lowestmode": elevLow,
                                 'ElevationA1': elev1, 'ElevationA2': elev2, 'ElevationA3': elev3, 'ElevationA4': elev4, 'ElevationA5': elev5, 'ElevationA6': elev6})
        raw_df[rh_cols] = pd.DataFrame(rh, index= raw_df.index)
        raw_df = raw_df.dropna()
        beam_dfs.append(raw_df)
        
    raw_df = pd.concat(beam_dfs)

    filtered = raw_df[(raw_df.Sensitivity >= 0.9)  & (raw_df.Solar_Elevation < 0) & (raw_df.Quality == 1)]
    filtered['MaxElevation'] = filtered[["ElevationA1", "ElevationA2","ElevationA3","ElevationA4","ElevationA5","ElevationA6"]].max(axis=1)
    filtered['MinElevation'] = filtered[["ElevationA1", "ElevationA2","ElevationA3","ElevationA4","ElevationA5","ElevationA6"]].min(axis=1)
    filtered['Elev_Range'] = abs(filtered.MaxElevation - filtered.MinElevation)

    GEDI_DF = filtered[filtered.Elev_Range <= 2]
    gedi_dfs.append(GEDI_DF[["Shot Number", "Beam", "Latitude", "Longitude", "elev_lowestmode"]+rh_cols])
    
    #.to_csv(gediFiles[j][:-3] + ".csv", index=False)
df = pd.concat(gedi_dfs)
df.to_csv("processedShots.csv", index = False)