#!/usr/bin/env python

import os
import re
import time
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.shutil import copy as rio_copy
from rasterio.errors import RasterioIOError
import xarray as xr
from concurrent.futures import ThreadPoolExecutor, as_completed
import backoff
from requests.exceptions import ConnectionError, HTTPError
from rasterio.errors import RasterioIOError
import s3fs
import gc
from tqdm import tqdm


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
@backoff.on_exception(
    backoff.expo,
    (ConnectionError, HTTPError, RasterioIOError),
    max_tries=10,
    max_time=60,
    jitter=backoff.full_jitter,
)


def load_single_image(s3_path, fs, time):
    """Helper function to load a single image with retries."""
    with fs.open(s3_path, mode='rb') as f:
        da = xr.open_dataarray(f, engine="rasterio")  # Adjust chunking if needed
        da = da.expand_dims(time=pd.Index([time], name='time'))
    return da


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

    # ThreadPoolExecutor for parallel loading
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_data = {}

        for t, row in burst_ts_df.iterrows():
            opera_id = row['OPERA L2-RTC-S1 ID']
            time = pd.to_datetime(row['AcquisitionDateTime'])

            # Submit tasks for both polarizations for each row
            for polarization in polarizations:
                filename = f"{opera_id}_{polarization}.tif"
                object_key = f"OPERA_L2_RTC-S1/{opera_id}/{filename}"
                s3_path = f"s3://{event['Bucket']}/{object_key}"

                # Submit the image load task to the executor
                future = executor.submit(load_single_image, s3_path, fs, time)
                future_to_data[future] = (t, polarization)

        # Collect and organize the results
        results_by_time = {}

        for future in as_completed(future_to_data):
            try:
                t, polarization = future_to_data[future]
                da = future.result()

                # Organize results by time index
                if t not in results_by_time:
                    results_by_time[t] = []
                results_by_time[t].append((polarization, da))
            except Exception as e:
                print(f"An error occurred: {e}")

        # Assemble the final dataset
        for t, polarized_results in sorted(results_by_time.items()):
            # Ensure results are ordered correctly by polarization
            polarized_results.sort(key=lambda x: polarizations.index(x[0]))
            da_polarized = xr.concat(
                [result[1] for result in polarized_results], 
                dim=pd.Index(polarizations, name='polarization')
            )
            da_stack.append(da_polarized)

    ds = xr.concat(da_stack, dim='time')
    return ds


# estimate temporal mean
def xarray_tmean(ds, pol):
    ts = ds.sel(polarization=pol)
    temporal_avg = ts.mean(dim='time').persist()
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
def run_rtc_temp_mean(burst_id_list, granule_gdf, creds, event, out_dir, start_date, end_date):
    t_all = time.time() # track processing time
    
    # Check if the directory exists
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
        print(f"Directory {out_dir} created.")
    
    # for burst in burst_id_list:
    for burst in tqdm(burst_id_list, desc="Processing bursts", unit="burst"):
        polarization = ['VV', 'VH']
        # check if the image already exists, skip it it does
        check_vv = f"{out_dir}/{burst}_tmean_{start_date}_{end_date}_{polarization[0]}.tif"
        check_vh = f"{out_dir}/{burst}_tmean_{start_date}_{end_date}_{polarization[1]}.tif"
        if os.path.exists(check_vv) and os.path.exists(check_vh):
            # print(f"Temporal VV & VH means for burst {burst} exist, skipping to the next")
            continue
            
        burst_ids = get_burstid_list(burst, granule_gdf)
        if len(burst_ids) < 2:
            print(f"Only one burst available for ID {burst}, skippin temporal average")
            continue
            
        # Load xarray and ensure cleanup
        try:
            burst_ts_df = get_burst_ts_df(burst_ids)
            burst_ds = load_burst_ts(burst_ts_df, creds, event)
            
            for pol in polarization:
                # calculate temp mean
                burst_tmean, epsg_code = xarray_tmean(burst_ds, pol)
                # export tiff
                out_tif = f"{out_dir}/{burst}_tmean_{pol}.tif"
                tmean2tiff(burst_tmean, out_tif, epsg_code)
        finally:
            # Ensure dataset is closed even in case of error
            burst_ds.close()
            gc.collect()

    # Track processing time 
    t_all_elapsed = time.time() - t_all # track processing time
    hours, rem = divmod(t_all_elapsed, 3600)
    minutes, seconds = divmod(rem, 60)
    print("Tiffs generated in  {:0>2}:{:0>2}:{:05.2f} hours:min:secs".format(int(hours),int(minutes),seconds)) # track processing time


# Compute RVI for availabel tiles
def compute_rvi_from_files(vv_path, vh_path):
    """
    Compute Radar Vegetation Index (RVI) from VV and VH radar backscatter in linear scale.

    Parameters:
        vv_path (str): File path to VV raster (in linear scale).
        vh_path (str): File path to VH raster (in linear scale).

    Returns:
        np.ndarray: RVI values as a NumPy array.
    """
    with rasterio.open(vv_path) as vv_src, rasterio.open(vh_path) as vh_src:
        vv = vv_src.read(1).astype(np.float32)
        vh = vh_src.read(1).astype(np.float32)

        # Make sure shapes match
        if vv.shape != vh.shape:
            raise ValueError("VV and VH rasters must have the same shape")

        denominator = vv + vh
        with np.errstate(divide='ignore', invalid='ignore'):
            rvi = (4 * vh) / denominator
            rvi[denominator == 0] = np.nan  # handle divide-by-zero
        
        meta = vv_src.meta.copy()
        meta.update({
            'dtype': 'float32',
            'count': 1,
            'nodata': np.nan
        })
    
    return rvi, meta


def compute_rvi_tiles(tile_dir, sitename, s_date, e_date):
    vv_pattern = re.compile(
        fr"s1_tile_{sitename}_{s_date}_{e_date}_h(\d+)_v(\d+)_VV\.tif$")
    vh_pattern = re.compile(
        fr"s1_tile_{sitename}_{s_date}_{e_date}_h(\d+)_v(\d+)_VH\.tif$")

    all_files = os.listdir(tile_dir)

    # Build a dict of VV and VH tiles keyed by (h, v)
    vv_tiles = {}
    vh_tiles = {}

    for f in all_files:
        vv_match = vv_pattern.match(f)
        vh_match = vh_pattern.match(f)

        if vv_match:
            h, v = vv_match.groups()
            vv_tiles[(h, v)] = os.path.join(tile_dir, f)
        elif vh_match:
            h, v = vh_match.groups()
            vh_tiles[(h, v)] = os.path.join(tile_dir, f)

    # Find matching (h, v) keys
    matching_keys = set(vv_tiles.keys()) & set(vh_tiles.keys())

    for h, v in matching_keys:
        output_name = f"s1_tile_{sitename}_{s_date}_{e_date}_h{h}_v{v}_RVI.tif"
        output_path = os.path.join(tile_dir, output_name)

        # Skip if RVI file already exists
        if os.path.exists(output_path):
            print(f"Skipping RVI for h{h} v{v} (already exists).")
            continue

        vv_path = vv_tiles[(h, v)]
        vh_path = vh_tiles[(h, v)]

        try:
            rvi_array, meta = compute_rvi_from_files(vv_path, vh_path)
        except RasterioIOError as e:
            print(f"Failed to read input files for h{h} v{v}: {e}")
            continue

        # Write temporary GeoTIFF
        with rasterio.open("/vsimem/temp_rvi.tif", 'w', **meta) as dst:
            dst.write(rvi_array, 1)

        # Export as Cloud Optimized GeoTIFF
        rio_copy("/vsimem/temp_rvi.tif", output_path, driver='COG', copy_src_overviews=True)

        print(f"Saved RVI to {output_path}")