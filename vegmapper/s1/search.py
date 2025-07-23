#!/usr/bin/env python

import warnings
from datetime import datetime
from functools import reduce
from pathlib import Path

import geopandas as gpd

from vegmapper.asf import geo_search
from vegmapper.pathurl import PathURL


def group_granules(granules):
    """
    Group granules by frames.
    """

    # TODO: support for granules in csv format?
    # ASF's csv metadata file has different column names.
    if isinstance(granules, (str, Path)):
        gdf_granules = gpd.read_file(granules)
    elif isinstance(granules, gpd.GeoDataFrame):
        gdf_granules = granules.copy()

    gb = gdf_granules.groupby(['pathNumber', 'frameNumber'])
    gdf_frames = gpd.GeoDataFrame(
        [[p,
          f,
          len(gb.get_group((p, f))),
          ','.join(gb.get_group((p, f)).sceneName.to_list()),
          reduce(lambda x, y: x.union(y), gb.get_group((p, f)).geometry)]
        for p, f in gb.groups.keys()],
        columns=['pathNumber', 'frameNumber', 'num_granules', 'granules', 'geometry']
    )

    return gdf_frames


def skim_granules(aoifile, gdf_granules, gdf_frames):
    gdf_aoi = gpd.read_file(aoifile).dissolve()

    # Find indices of neighbor frames
    neighbors = []
    for i, row in gdf_frames.iterrows():
        overlap = gdf_frames.geometry.apply(lambda x: x.intersection(row['geometry']).area/row['geometry'].area)
        indices = overlap[overlap > 0.95].index.to_list()
        if len(indices) > 1 and indices not in neighbors:
            neighbors.append(indices)

    # Merge neighbor frames
    for indices in neighbors:
        frameNumber = reduce(lambda x, y: f'{x}-{y}', gdf_frames.frameNumber[indices])
        num_granules = reduce(lambda x, y: x+y, gdf_frames.num_granules[indices])
        granules = reduce(lambda x, y: f'{x},{y}', gdf_frames.granules[indices])
        geometry = reduce(lambda x, y: x.union(y), gdf_frames.geometry[indices])
        gdf_frames.loc[indices, 'frameNumber'] = frameNumber
        gdf_frames.loc[indices, 'num_granules'] = num_granules
        gdf_frames.loc[indices, 'granules'] = granules
        gdf_frames.loc[indices, 'geometry'] = geometry
    gdf_frames = gdf_frames.drop_duplicates().reset_index(drop=True)

    gdf_frames['overlap_with_aoi'] = 0.0
    # gdf_frames['exclusive_area'] = None
    for i, row in gdf_frames.iterrows():
        # Move the current frame i to the front of list
        idx = [i] + gdf_frames.index.drop(i).to_list()

        # Subtract other frames from the current frame i and get remaining area
        # intersected with aoi
        geom = reduce(lambda x, y: x.difference(y), gdf_frames.geometry[idx])
        area = gdf_aoi.geometry[0].intersection(geom).area
        gdf_frames.loc[i, 'overlap_with_aoi'] = area
        # gdf_frames.loc[i, 'exclusive_area'] = geom

    granules_to_drop = ','.join(gdf_frames.loc[gdf_frames.overlap_with_aoi == 0, 'granules']).split(',')
    gdf_granules = gdf_granules[~gdf_granules['sceneName'].isin(granules_to_drop)].reset_index(drop=True)
    print(f'\n{(gdf_frames.overlap_with_aoi == 0).sum()} frames are skimmed from the search results')

    gdf_frames = gdf_frames[gdf_frames.overlap_with_aoi > 0].reset_index(drop=True)

    # Reorder columns
    gdf_frames = gdf_frames[['pathNumber', 'frameNumber', 'num_granules', 'granules', 'overlap_with_aoi', 'geometry']]

    return gdf_granules, gdf_frames


def skim_opera_granules(aoifile, gdf_granules):
    gdf_aoi = gpd.read_file(aoifile).dissolve()
    gdf_granules = gdf_granules.set_crs("EPSG:4326")
    # Convert gdf_granules to match the CRS of gdf_aoi
    gdf_granules = gdf_granules.to_crs(gdf_aoi.crs)
    # Create a single polygon from the AOI
    aoi_polygon = gdf_aoi.unary_union
    # Filter granules that intersect with the AOI polygon
    granules_in_aoi = gdf_granules.loc[gdf_granules.geometry.intersects(aoi_polygon, align=False)]

    return granules_in_aoi


def search_granules(sitename,
                    aoifile,
                    start_date,
                    end_date,
                    skim=True,
                    **search_opts):
    """
    Search Sentinel-1 granules that intersect with AOI and were acquired between the start and end dates.
    """

    dt_start = datetime.strptime(start_date, '%Y-%m-%d')
    dt_end = datetime.strptime(end_date, '%Y-%m-%d')

    # Search granules
    search_opts['start'] = start_date
    search_opts['end'] = end_date
    if 'flightDirection' not in search_opts:
        gdf_granules = geo_search(aoifile, **search_opts)

        num_ascending = (gdf_granules.flightDirection == 'ASCENDING').sum()
        num_descending = (gdf_granules.flightDirection == 'DESCENDING').sum()

        print(f'{num_ascending} granules found for ASCENDING orbits.')
        print(f'{num_descending} granules found for DESCENDING orbits.')

        if num_ascending >= num_descending:
            gdf_granules = gdf_granules[gdf_granules.flightDirection == 'ASCENDING'].reset_index(drop=True)
            print('The granules of ASCENDING orbits will be used.')
        else:
            gdf_granules = gdf_granules[gdf_granules.flightDirection == 'DESCENDING'].reset_index(drop=True)
            print('The granules of DESCENDING orbits will be used.')
    else:
        gdf_granules = geo_search(aoifile, **search_opts)
        print(f"{len(gdf_granules)} granules found for {search_opts['flightDirection']} orbits.")

    # Some tweaks for saving gdf_granules in a GeoJSON file
    gdf_granules['browse'] = gdf_granules['browse'].apply(lambda x: ' '.join(x))
    gdf_granules['pathNumber'] = gdf_granules['pathNumber'].astype(int)

    if 'dataset' in search_opts and search_opts['dataset'] == 'OPERA-S1': # check here if the data search is for opera-rtc
        gdf_granules = gdf_granules.sort_values(by=['pathNumber', 'groupID', 'startTime']) # Testing
        if skim:
            gdf_granules = skim_opera_granules(aoifile, gdf_granules)

        return gdf_granules
        
    else:
        # Some tweaks for saving gdf_granules in a GeoJSON file
        gdf_granules['frameNumber'] = gdf_granules['frameNumber'].astype(int)
        gdf_granules = gdf_granules.sort_values(by=['pathNumber', 'frameNumber', 'startTime'])
        # Group granules by frames
        gdf_frames = group_granules(gdf_granules)
        # Skim the unnecessary frames
        if skim:
            gdf_granules, gdf_frames = skim_granules(aoifile, gdf_granules, gdf_frames)
            
        # Save search results to geojsons
        out_dir = PathURL(aoifile).parent
        geojson_granules = f"{out_dir}/{sitename}_s1_granules_{dt_start.strftime('%Y%m%d')}-{dt_end.strftime('%Y%m%d')}.geojson"
        geojson_frames = geojson_granules.replace('s1_granules', 's1_frames')
        gdf_granules.drop(columns=['s3Urls'], inplace=True)
        with warnings.catch_warnings():
            # Ignore the FutureWarning, which will be fixed in the next release of geopandas
            warnings.simplefilter('ignore')
            gdf_granules.to_file(geojson_granules, driver='GeoJSON')
            gdf_frames.to_file(geojson_frames, driver='GeoJSON')
        print(f'\nMetadata of S1 granules saved to: {geojson_granules}')
        print(f'Metadata of S1 frames saved to: {geojson_frames}')
    
        return gdf_granules, gdf_frames
