#!/usr/bin/env python

import os
import geopandas as gpd
import rasterio
from rasterio.crs import CRS
import subprocess

def map_burst2tile(reference_tiles, burst_summary_gdf):
    """
    This function reads the reference tiles
    and creates a geodataframe that will be 
    used to map the burst into their 
    corresponding tile. 
    Inputs:
        reference_tiles - Geojson
        burst_summary_gdf - Geodataframe
    
    """
    
    # Load the GeoJSON file into a GeoDataFrame
    tile_gdf = gpd.read_file(reference_tiles)
    burst_gdf = burst_summary_gdf.copy()
    # extract target crs from tiles
    tile_crs = tile_gdf.crs
    tile_epsg_number = tile_crs.to_string().split(':')[-1]
    # set burst crs
    burst_gdf.set_crs(epsg=4326, inplace=True) # This epsg is hard coded because its expected to be always the same.
    # Ensure both GeoDataFrames have the same CRS before overlapping
    tile_gdf = tile_gdf.to_crs(epsg=tile_epsg_number)
    burst_gdf = burst_gdf.to_crs(epsg=tile_epsg_number)
    
    # Perform spatial join to find overlapping geometries
    overlapping_gdf = gpd.sjoin(burst_gdf, tile_gdf, how="inner", op="intersects")
    
    # Group by the index of geojson_gdf to collect overlapping 'names'
    overlap_dict = overlapping_gdf.groupby('index_right')['burst_id'].apply(list).to_dict()
    
    # Add the overlapping names back to the GeoJSON GeoDataFrame
    tile_gdf['overlapping_bursts'] = tile_gdf.index.map(overlap_dict)

    # Export geodataframe as geojson
    def join_lists(cell):
        if isinstance(cell, list):
            return ','.join(cell)
        return cell

    gdf2export = tile_gdf.copy()
    # Convert the list of strings in 'overlapping_names' to a single string
    gdf2export['overlapping_bursts'] = gdf2export['overlapping_bursts'].apply(join_lists)
    # Convert geometry to WKT format for CSV export
    gdf2export['geometry'] = gdf2export['geometry'].apply(lambda x: x.wkt)
    # Export to CSV
    gdf2export.to_csv(f"./rtc_tmeans/burst_to_tile_map.csv", index=False)

    return tile_gdf


def get_epsg(file_path):
    with rasterio.open(file_path) as dataset:
        crs = dataset.crs
        if crs:
            return crs.to_epsg()
        return None
        

def build_opera_vrt(burst2tile_gdf):

    # directory where all rtc means are located. 
    rtc_dir = './rtc_tmeans'
    # output directory
    out_vrt_dir = f'{rtc_dir}/tile_vrts'
    
    # Check if the directory exists, if not, create it
    if not os.path.exists(out_vrt_dir):
        os.makedirs(out_vrt_dir)
    
    polarizations = ['VV', 'VH']
    target_crs = burst2tile_gdf.crs.to_string()  # Using reference coordinate from tiles
    for index, row in burst2tile_gdf.iterrows():
        if row['mask'] != 0:
            overlapping_names = row['overlapping_bursts']
            for pol in polarizations:
                files_2_merge = [f"{rtc_dir}/{name}_tmean_{pol}.tif" for name in overlapping_names]
                files_2_merge = [file for file in files_2_merge if os.path.exists(file)]
                bbox = row['geometry'].bounds  # Get bounding box [minx, miny, maxx, maxy]
                position_h = row['h']
                position_v = row['v']
                output_vrt = f'{out_vrt_dir}/tile_h{str(position_h)}_v{str(position_v)}_{pol}.vrt'
                output_vrt_mosaic = f'{out_vrt_dir}/mosaic_h{str(position_h)}_v{str(position_v)}_{pol}.vrt'
                
                reprojected_files = []
                target_epsg = CRS.from_string(target_crs).to_epsg()
                for file in files_2_merge:
                    file_epsg = get_epsg(file)
                    if file_epsg != target_epsg:  # Compare EPSG code
                        reprojected_file = file.replace('.tif', '_reprojected.vrt')
                        if not os.path.exists(reprojected_file):
                            warp_command_reproject = [
                                'gdalwarp', '-q',
                                '-t_srs', target_crs,
                                '-r', 'near',
                                '-dstnodata', 'nan',
                                '-of', 'VRT',
                                file, reprojected_file
                            ]
                            subprocess.run(warp_command_reproject)
                        reprojected_files.append(reprojected_file)
                    else:
                        reprojected_files.append(file)
                
                # Construct gdalbuildvrt command to create mosaic VRT with reprojected files
                buildvrt_command = ['gdalbuildvrt', '-q', '-srcnodata', 'nan', '-vrtnodata', 'nan', output_vrt_mosaic] + reprojected_files
                subprocess.run(buildvrt_command)
    
                # Construct gdalwarp command for final VRT
                warp_command = [
                    'gdalwarp',
                    '-overwrite',
                    '-t_srs', target_crs, '-et', '0',  # Target CRS
                    '-te', str(bbox[0]), str(bbox[1]), str(bbox[2]), str(bbox[3]),  # Target extents
                    '-srcnodata', 'nan', '-dstnodata', 'nan',
                    '-r', 'near',
                    '-co', 'COMPRESS=LZW',
                    '-of', 'VRT',  # Output format
                    output_vrt_mosaic,  # Input mosaic VRT
                    output_vrt,  # Output final VRT
                ]
    
                # Run gdalwarp command
                print('Running tile')
                subprocess.run(warp_command)
