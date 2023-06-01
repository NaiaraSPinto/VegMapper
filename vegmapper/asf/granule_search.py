#!/usr/bin/env python

import asf_search as asf
import geopandas as gpd
import pandas as pd


def granule_search(granule_list, processingLevel='GRD_HD'):
    """
    A wrapper of asf_search.granule_search with following modifications:
        1. Return search results as a GeoDataFrame.
        2. Convert pathNumber and frameNumber to int type.
        3. Convert startTime and stopTime to datatime type.
    """

    search_results = asf.granule_search(granule_list)
    gdf_results = gpd.GeoDataFrame.from_features(search_results.geojson()['features'])
    gdf_results = gdf_results[gdf_results.processingLevel == processingLevel].reset_index(drop=True)
    gdf_results['pathNumber'] = gdf_results['pathNumber'].astype(int)
    gdf_results['frameNumber'] = gdf_results['frameNumber'].astype(int)
    gdf_results['startTime']= pd.to_datetime(gdf_results['startTime'])
    gdf_results['stopTime']= pd.to_datetime(gdf_results['stopTime'])

    return gdf_results
