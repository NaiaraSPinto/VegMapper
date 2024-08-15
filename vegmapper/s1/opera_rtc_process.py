#!/usr/bin/env python

import os
import re
import time
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
import xarray as xr
import s3fs

# get all related burst
def get_burstid_list(burst_name, granule_gdf):
    """
    Function to get list of available bursts 
    for the same ID. 
    inputs
    ------
    burst_name = string.
    granule_gdf = geodataframe with RTC
        granules for a specific time and region.
    
    returns
    -------
    burstid_list = list.
    """
    burst_id_df = granule_gdf[granule_gdf['fileID'].str.startswith(burst_name)]
    burstid_list = sorted(burst_id_df['fileID'].tolist())

    return burstid_list


# get timestamp
def get_dt(opera_id, date_regex):
    acquisition_time = re.search(date_regex, opera_id)
    try:
        return acquisition_time.group(0)
    except AttributeError:
        raise Exception(f"Acquisition timestamp not found in scene ID: {opera_id}") 


# get burst time series dataframe 
def get_burst_ts_df(opera_rtc_ids):
    # date time regular expression definitions
    acquisition_date_regex = r"(?<=_)\d{8}T\d{6}Z(?=_\d{8}T\d{6})"
    process_dt_regex = r"(?<=\d{8}T\d{6}Z_)\d{8}T\d{6}Z(?=_S1)"
    acquisition_dt = pd.to_datetime([get_dt(id, acquisition_date_regex) for id in opera_rtc_ids])
    process_dt = pd.to_datetime([get_dt(id, process_dt_regex) for id in opera_rtc_ids])

    times_series_df = (pd.DataFrame(data={
        'OPERA L2-RTC-S1 ID': opera_rtc_ids, 
        'AcquisitionDateTime': acquisition_dt,
        'ProcessDateTime': process_dt
    })
    .sort_values(by='ProcessDateTime')
    .drop_duplicates(subset=['AcquisitionDateTime'], keep='last')
    .drop('ProcessDateTime', axis=1)
    .sort_values(by='AcquisitionDateTime')
    .reset_index(drop=True))

    return times_series_df


# load array using a burst timeseries dataframe
def load_burst_ts(burst_ts_df, creds, event):
    """
    Inputs
    ------
    burst_ts_df = dataframe with burst time series.
    creds = S3 access key dictionary 
    """
    fs = s3fs.S3FileSystem(key=creds['accessKeyId'], secret=creds['secretAccessKey'], token=creds['sessionToken'])

    polarizations = ['VV', 'VH']
    da_stack = []
    
    for t, row in burst_ts_df.iterrows():
        opera_id = row['OPERA L2-RTC-S1 ID']
        time = pd.to_datetime(row['AcquisitionDateTime'])
        polarization_stack = []
    
        for polarization in polarizations:
            filename = f"{opera_id}_{polarization}.tif"
            object_key = f"OPERA_L2_RTC-S1/{opera_id}/{filename}"
            s3_path = f"s3://{event['Bucket']}/{object_key}"
    
            with fs.open(s3_path, mode='rb') as f:
                da = xr.open_dataarray(f, engine="rasterio")
                da = da.expand_dims(time=pd.Index([time], name='time'))
                polarization_stack.append(da)
    
        da_polarized = xr.concat(polarization_stack, dim=pd.Index(polarizations, name='polarization'))
        da_stack.append(da_polarized)
    
    ds = xr.concat(da_stack, dim='time')
    
    return ds 


# estimate temporal mean
def xarray_tmean(ds, pol):
    ts = ds.sel(polarization=pol)
    temporal_avg = ts.mean(dim='time')
    epsg_code = 'EPSG:' + str(ds.attrs['BOUNDING_BOX_EPSG_CODE'])
    
    return temporal_avg, epsg_code


# export to xarray temporal mean to tiff
def tmean2tiff(temporal_avg, file_output, epsg_code):
    from rasterio.transform import from_origin
    # Define the GeoTIFF metadata
    transform = from_origin(temporal_avg.x.values.min(), temporal_avg.y.values.max(), 
                            abs(temporal_avg.x.values[1] - temporal_avg.x.values[0]), 
                            abs(temporal_avg.y.values[1] - temporal_avg.y.values[0]))
    
    # Save the temporal mean as a GeoTIFF file
    with rasterio.open(file_output, 'w', driver='GTiff', 
                       height=temporal_avg.shape[1], width=temporal_avg.shape[2],
                       count=temporal_avg.shape[0], dtype=temporal_avg.dtype, crs=epsg_code, transform=transform) as dst:
        dst.write(temporal_avg.values)


# main driver
def run_rtc_temp_mean(burst_id_list, granule_gdf, creds, event):
    t_all = time.time() # track processing time
    
    out_dir = "./rtc_tmeans"
    # Check if the directory exists
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
        print(f"Directory {out_dir} created.")
    
    for burst in burst_id_list:
        polarization = ['VV', 'VH']
        # check if the image already exists, skip it it does
        check_vv = f"{out_dir}/{burst}_tmean_{polarization[0]}.tif"
        check_vh = f"{out_dir}/{burst}_tmean_{polarization[1]}.tif"
        if os.path.exists(check_vv) and os.path.exists(check_vh):
            print(f"Temporal VV & VH means for burst {burst} exist, skipping to the next")
            continue
            
        burst_ids = get_burstid_list(burst, granule_gdf)
        if len(burst_ids) < 2:
            print(f"Only one burst available for ID {burst}, skippin temporal average")
            continue
            
        # create df
        burst_ts_df = get_burst_ts_df(burst_ids)
        # load xarray
        burst_ds = load_burst_ts(burst_ts_df, creds, event)
        for pol in polarization:
            # calculate temp mean
            burst_tmean, epsg_code = xarray_tmean(burst_ds, pol)
            # export tiff
            out_tif = f"{out_dir}/{burst}_tmean_{pol}.tif"
            tmean2tiff(burst_tmean, out_tif, epsg_code)
    # Track processing time 
    t_all_elapsed = time.time() - t_all # track processing time
    hours, rem = divmod(t_all_elapsed, 3600)
    minutes, seconds = divmod(rem, 60)
    print("Tiffs generated in  {:0>2}:{:0>2}:{:05.2f} hours:min:secs".format(int(hours),int(minutes),seconds)) # track processing time

