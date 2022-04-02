import os
import h5py
import numpy as np
import pandas as pd
import glob
import itertools
import statistics
import time

pd.options.mode.chained_assignment = None
import warnings
warnings.filterwarnings("ignore")

def rangeCalculator(elevList, sd, mean):
    elevList = [item for item in elevList if item <= (mean + (2*sd)) and item >= (mean - (2*sd)) ]
    if elevList:
        return max(elevList) - min(elevList)
    else:
        return -99

gedi_dfs = []

f = open("h5S3files.txt", "r")

for h5file in f.readlines():
    h5file = h5file.strip()
    cpS3url = "aws s3 cp s3://servir-public/gedi/peru/" + h5file + " ./"
    
    print("DOWNLOAD FILE: " + cpS3url)
    os.system(cpS3url)
    ###READ DATA
    skipFlag = False
    try:
        gediL2A = h5py.File(h5file, 'r')
    except Exception as e:
        print(e)
        continue

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
        gedi_beam_data = {}
        try:
            gedi_beam_data["lats"] = gediL2A[f'{highBeam}/lat_lowestmode'][()]
            gedi_beam_data["long"] = gediL2A[f'{highBeam}/lon_lowestmode'][()]
            gedi_beam_data["shotNumber"] = gediL2A[f'{highBeam}/shot_number'][()]
            gedi_beam_data["quality"] = gediL2A[f'{highBeam}/quality_flag'][()]
            gedi_beam_data["solar_elev"] = gediL2A[f'{highBeam}/solar_elevation'][()]
            gedi_beam_data["sensitivity"] = gediL2A[f'{highBeam}/sensitivity'][()]
            #gedi_beam_data["rh"] = gediL2A[f'{highBeam}/rh'][()]
            gedi_beam_data["elevLow"] = gediL2A[f'{highBeam}/elev_lowestmode'][()]

            gedi_beam_data["delta_time"] = gediL2A[f'{highBeam}/delta_time'][()]
            gedi_beam_data["energy_total"] = gediL2A[f'{highBeam}/energy_total'][()]
                
            gedi_beam_data["rx_gbias"] = gediL2A[f'{highBeam}/rx_1gaussfit/rx_gbias'][()]
            gedi_beam_data["rx_gamplitude_error"] = gediL2A[f'{highBeam}/rx_1gaussfit/rx_gamplitude_error'][()]
                
            gedi_beam_data["rx1_energy_sm"] = gediL2A[f'{highBeam}/rx_processing_a1/energy_sm'][()]
            gedi_beam_data["rx2_energy_sm"] = gediL2A[f'{highBeam}/rx_processing_a2/energy_sm'][()]
            gedi_beam_data["rx3_energy_sm"] = gediL2A[f'{highBeam}/rx_processing_a3/energy_sm'][()]
            gedi_beam_data["rx4_energy_sm"] = gediL2A[f'{highBeam}/rx_processing_a4/energy_sm'][()]
            gedi_beam_data["rx5_energy_sm"] = gediL2A[f'{highBeam}/rx_processing_a5/energy_sm'][()]
            gedi_beam_data["rx6_energy_sm"] = gediL2A[f'{highBeam}/rx_processing_a6/energy_sm'][()]
             
            gedi_beam_data["rx1_lastmodeenergy"] = gediL2A[f'{highBeam}/rx_processing_a1/lastmodeenergy'][()]
            gedi_beam_data["rx2_lastmodeenergy"] = gediL2A[f'{highBeam}/rx_processing_a2/lastmodeenergy'][()]
            gedi_beam_data["rx3_lastmodeenergy"] = gediL2A[f'{highBeam}/rx_processing_a3/lastmodeenergy'][()]
            gedi_beam_data["rx4_lastmodeenergy"] = gediL2A[f'{highBeam}/rx_processing_a4/lastmodeenergy'][()]
            gedi_beam_data["rx5_lastmodeenergy"] = gediL2A[f'{highBeam}/rx_processing_a5/lastmodeenergy'][()]
            gedi_beam_data["rx6_lastmodeenergy"] = gediL2A[f'{highBeam}/rx_processing_a6/lastmodeenergy'][()]
                
            gedi_beam_data["elev1_low_energy"] = gediL2A[f'{highBeam}/geolocation/energy_lowestmode_a1'][()]
            gedi_beam_data["elev2_low_energy"] = gediL2A[f'{highBeam}/geolocation/energy_lowestmode_a2'][()]
            gedi_beam_data["elev3_low_energy"] = gediL2A[f'{highBeam}/geolocation/energy_lowestmode_a3'][()]
            gedi_beam_data["elev4_low_energy"] = gediL2A[f'{highBeam}/geolocation/energy_lowestmode_a4'][()]
            gedi_beam_data["elev5_low_energy"] = gediL2A[f'{highBeam}/geolocation/energy_lowestmode_a5'][()]
            gedi_beam_data["elev6_low_energy"] = gediL2A[f'{highBeam}/geolocation/energy_lowestmode_a6'][()]
                
            gedi_beam_data["elev1_num_modes"] = gediL2A[f'{highBeam}/geolocation/num_detectedmodes_a1'][()]
            gedi_beam_data["elev2_num_modes"] = gediL2A[f'{highBeam}/geolocation/num_detectedmodes_a2'][()]
            gedi_beam_data["elev3_num_modes"] = gediL2A[f'{highBeam}/geolocation/num_detectedmodes_a3'][()]
            gedi_beam_data["elev4_num_modes"] = gediL2A[f'{highBeam}/geolocation/num_detectedmodes_a4'][()]
            gedi_beam_data["elev5_num_modes"] = gediL2A[f'{highBeam}/geolocation/num_detectedmodes_a5'][()]
            gedi_beam_data["elev6_num_modes"] = gediL2A[f'{highBeam}/geolocation/num_detectedmodes_a6'][()]
             
            gedi_beam_data["elev1"] = gediL2A[f'{highBeam}/geolocation/elev_lowestmode_a1'][()]
            gedi_beam_data["elev2"] = gediL2A[f'{highBeam}/geolocation/elev_lowestmode_a2'][()]
            gedi_beam_data["elev3"] = gediL2A[f'{highBeam}/geolocation/elev_lowestmode_a3'][()]
            gedi_beam_data["elev4"] = gediL2A[f'{highBeam}/geolocation/elev_lowestmode_a4'][()]
            gedi_beam_data["elev5"] = gediL2A[f'{highBeam}/geolocation/elev_lowestmode_a5'][()]
            gedi_beam_data["elev6"] = gediL2A[f'{highBeam}/geolocation/elev_lowestmode_a6'][()]
            rh = gediL2A[f'{highBeam}/rh'][()]
            gedi_beam_data["beam"] = np.full(len(gedi_beam_data['shotNumber']), highBeam)
                #raw_df = pd.DataFrame({shots, 'Beam':np.full(len(shots), highBeam), 'Longitude': lons, 'Latitude': lats,
                 #                        'Quality': quality, 'Solar_Elevation': solar_elev, 'Sensitivity': sensitivity, "elev_lowestmode": elevLow,
                 #                        'ElevationA1': elev1, 'ElevationA2': elev2, 'ElevationA3': elev3, 'ElevationA4': elev4, 'ElevationA5': elev5, 'ElevationA6': elev6})
                
            raw_df = pd.DataFrame(gedi_beam_data)
            raw_df[rh_cols] = pd.DataFrame(rh, index= raw_df.index)
            raw_df = raw_df.dropna()
            beam_dfs.append(raw_df)
        except Exception as e:
            print("EXCEPTION: ", e)
            skipFlag = True


    if not skipFlag:
        raw_df = pd.concat(beam_dfs)
        filtered = raw_df[raw_df.sensitivity >= 0.9] #& (raw_df.Quality == 1)
        
        elevs = pd.concat([filtered.elev1, filtered.elev2, filtered.elev3, filtered.elev4, filtered.elev5, filtered.elev6])
        filtered['elev_sd'] = elevs.std()
        filtered['elev_mean'] = elevs.mean() #calculate mean of elevs
        filtered['elev_range'] = filtered.apply(lambda x: rangeCalculator([x['elev1'], x['elev2'], x['elev3'], x['elev4'], x['elev5'], x['elev6']], x['elev_sd'], x['elev_mean']), axis = 1)
        
        GEDI_DF = filtered[filtered.elev_range < 2]

        GEDI_DF = GEDI_DF[GEDI_DF.rh95 > 0]
        
        csv_file = "filtered_" + h5file[:-3]+".csv"
        print("CREATING CSV: " + csv_file)
        GEDI_DF.to_csv(csv_file)
        cp_csv = "aws s3 cp " + csv_file + " s3://servir-public/gedi/peru/" + csv_file
        print("COPY CSV TO S3: " + cp_csv)
        os.system(cp_csv)
        os.remove(csv_file)
    try:
        os.remove(h5file)
    except:
        continue

