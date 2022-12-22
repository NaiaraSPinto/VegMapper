import os
import h5py
import numpy as np
import pandas as pd
import warnings

from .data_download import download_from_lpdaac, delete_local_files, divide_download_file

pd.options.mode.chained_assignment = None
warnings.filterwarnings("ignore")

#h5FilesToProcess = r"C:\Users\conductor\Desktop\Oil_Palm_Mapping\cmr_spatial_query_demo\DDD_demo\daac_data_download_python\h5_files.txt"
#sourceDirectory = r"C:\Users\conductor\Desktop\Oil_Palm_Mapping\cmr_spatial_query_demo\DDD_demo\daac_data_download_python"

#destinationDirectory = r"C:\Users\conductor\Desktop\Oil_Palm_Mapping\cmr_spatial_query_demo\DDD_demo\daac_data_download_python\output_test1"


def saveFilteredData(GEDI_DF, h5file, csv_files):
    # save final filtered dataframe to a csv file in the destination directory with the correct prefix
    csv_file = os.path.join(h5file[:-3] + ".csv")
    csv_files.append(csv_file)
    print("CREATING CSV: " + csv_file)
    GEDI_DF.to_csv(csv_file)


def filterBeamData(all_beam_dfs, h5file, csv_files):
    try:
        filtered = all_beam_dfs[all_beam_dfs.sensitivity >= 0.9]  # sensitivity > 90%

        # filter based on elevation
        elevs = pd.concat(
            [filtered.elev1, filtered.elev2, filtered.elev3, filtered.elev4, filtered.elev5, filtered.elev6])
        filtered['elev_sd'] = elevs.std()
        filtered['elev_mean'] = elevs.mean()  # calculate mean of elevs
        filtered['elev_range'] = filtered.apply(
            lambda x: rangeCalculator([x['elev1'], x['elev2'], x['elev3'], x['elev4'], x['elev5'], x['elev6']],
                                      x['elev_sd'], x['elev_mean']), axis=1)

        GEDI_DF = filtered[abs(filtered.elev_range) < 2]  # only select rows that have valid ranges

        GEDI_DF = GEDI_DF[GEDI_DF.rh95 > 0]  ## excludes ground points
    except Exception as e:
        print("EXCEPTION: ", e)
        return
    else:
        saveFilteredData(GEDI_DF, h5file, csv_files)


# returns the range between valid elevations (elevations that are within 2SD of the mean)
def rangeCalculator(elevList, sd, mean):
    elevList = [item for item in elevList if item <= (mean + (2 * sd)) and item >= (mean - (2 * sd))]
    if elevList:
        return max(elevList) - min(elevList)
    else:
        # all elevations are abnormal, return -99 to indicate data from this row should not be included in analysis
        return 99


def extractBeamData(highBeam, gediL2A, rh_cols):
    gedi_beam_data = {}
    try:
        gedi_beam_data["lats"] = gediL2A[f'{highBeam}/lat_lowestmode'][()]
        gedi_beam_data["long"] = gediL2A[f'{highBeam}/lon_lowestmode'][()]
        gedi_beam_data["shotNumber"] = gediL2A[f'{highBeam}/shot_number'][()]
        gedi_beam_data["quality"] = gediL2A[f'{highBeam}/quality_flag'][()]
        gedi_beam_data["solar_elev"] = gediL2A[f'{highBeam}/solar_elevation'][()]
        gedi_beam_data["sensitivity"] = gediL2A[f'{highBeam}/sensitivity'][()]
        # gedi_beam_data["rh"] = gediL2A[f'{highBeam}/rh'][()]
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

        raw_df = pd.DataFrame(gedi_beam_data)
        raw_df[rh_cols] = pd.DataFrame(rh, index=raw_df.index)  # add rh values
        raw_df = raw_df.dropna()
    except Exception as e:
        print("EXCEPTION: ", e)
        return None
    else:
        return raw_df


def processBeams(gediL2A, h5file, csv_files, rh_cols):
    ## approx 4 beams in every file
    beamNames = [g for g in gediL2A.keys() if g.startswith('BEAM')]
    beam_dfs = []  # create list to hold resulting dataframes for each beam

    gediL2A_objs = []
    gediL2A.visit(gediL2A_objs.append)  # Retrieve list of datasets
    gediSDS = [o for o in gediL2A_objs if
               isinstance(gediL2A[o], h5py.Dataset)]  # Search for relevant SDS inside data file

    for highBeam in beamNames:  # extract data from each beam in the shot
        beam_df = extractBeamData(highBeam, gediL2A, rh_cols)
        if not beam_df.empty:  # if beam data extraction is successful, append resulting dataframe to list
            beam_dfs.append(beam_df)

    if len(beam_dfs) >= 2:
        all_beam_dfs = pd.concat(beam_dfs)  # combine all beam dataframes
        filterBeamData(all_beam_dfs, h5file, csv_files)
    elif len(beam_dfs) == 1:
        filterBeamData(beam_dfs, h5file, csv_files)
    else:
        return


def readH5Files(h5FilesToProcess, sourceDirectory):
    csv_files = []  ## list that keeps track of all csv files generated

    ##generate column names for rh vals (ranges 1-100)
    rh_cols = []
    for i in range(101):
        rh_cols.append('rh' + str(i))

    f = open(h5FilesToProcess, "r")

    for h5file in f.readlines():
        h5file = os.path.join(sourceDirectory, h5file.strip())

        ###READ DATA
        try:
            gediL2A = h5py.File(h5file, 'r')
        except Exception as e:
            print(e)
            os.remove(h5file)
        else:
            processBeams(gediL2A, h5file, csv_files, rh_cols)


def divide_download_process_and_delete_h5_files(h5_download_file, work_dir, runName, token):
    
    try:
        #Verify that the h5 download file exists, if not error eout of the function
        assert os.path.exists(h5_download_file), f"Input H5 file not found! {h5_download_file}"
       
        #Define a save_dir sub directory for the run in the work directory
        save_dir = os.path.join(work_dir, runName)

        #define a log file that lives in that save_dir. note the way this processing
        # is logged is super sloppy. This could be cleaned up, but its better than nothing
        logfile = os.path.join(save_dir, f'{runName}_log.txt')

        print(f' Files will be processed in {save_dir}')
        print(f'logfile for {runName}: {logfile}')

        # create save directory if it doesnt already exist
        if not os.path.exists(save_dir):
            os.mkdir(save_dir)

        # split the h5 file with a massive amount of files into individual files (.download extension)
        divided_h5_download_files = divide_download_file(h5_download_file, save_dir)

        # Trackers for what gets processed/not_processed
        files_successfully_processed = []
        files_not_processed = []
        i = 1

        #loop through each input text file with url to wget
        for file in divided_h5_download_files:
            print(f'<----Processing file {i}/{len(divided_h5_download_files)}:\t{file}---->')

            # This file <h5 file name>.txt contains the name of the h5 file once it is downloaded.
            # This is used to keep track of which file needs to be processed, and later removed
            downloaded_file_tracker = os.path.join(save_dir,
                                                os.path.basename(file).split(
                                                    '.')[0] + '.txt')

            # Download the h5 file
            download_from_lpdaac(
                h5_download_file=file,
                out_text_file_name=downloaded_file_tracker,
                save_dir=save_dir,
                token=token
            )
            # Process the H5 file
            readH5Files(downloaded_file_tracker, save_dir)

            # Verify that a .csv file was produced
            expected_output_file = os.path.join(save_dir,
                                                os.path.basename(file).split('.')[
                                                    0] + '.csv')

            if os.path.exists(expected_output_file) == True:
                print(f'Expected output file exists: {expected_output_file}')
                files_successfully_processed.append(file)
            elif os.path.exists(expected_output_file) != True:
                print(
                    f'ERROR expected output file not found: {expected_output_file}')
                files_not_processed.append(file)

            # remove .h5 file and .txt
            delete_local_files(downloaded_file_tracker, save_dir)  # Delete H5 file
            os.remove(downloaded_file_tracker)  # delete tracker file
            os.remove(file)  # delete .download file
            print(f' Deleted .h5 file and intermediate .txt file')

            print(
                f'<----Processing Completed for File {i}/{len(divided_h5_download_files)}:\t{file}---->\n')
            i += 1

        # Write to logfile
        with open(logfile, 'w') as fw:
            # Write succesfully processed files
            fw.writelines(
                f'Successfully processed files: {len(files_successfully_processed)}\n')
            for file in files_successfully_processed:
                fw.writelines(f'{file}\n')

            # Write files that failed to process
            fw.writelines(f'Files failed to process: {len(files_not_processed)}\n')
            for file in files_not_processed:
                fw.writelines(f'{file}\n')

        print(
            f'##### PROCESSING COMPLETE, Successfully processed {len(files_successfully_processed)}/{len(divided_h5_download_files)} files, see details in logfile located at {logfile}')
    except AssertionError as err:
        print(err)
        return -1