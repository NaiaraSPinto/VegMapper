#!/usr/bin/env python

import asf_search as asf
import geopandas as gpd


def geo_search(aoifile, **search_opts):
    """
    A wrapper of asf_search.geo_search with following modifications:
        1. Take a vector-based spatial data format (e.g., shapefile, GeoJSON, etc.) as the input for AOI rather than using WKT.
        2. Simplify the geometric object (e.g., polygon) so that ASF SearchAPI won't run out of time.
        3. Return search results as a GeoDataFrame.
    """

    gdf_aoi = gpd.read_file(aoifile).dissolve()
    aoi_wkt = gdf_aoi.simplify(0.1).geometry[0].wkt

    search_results = asf.geo_search(intersectsWith=aoi_wkt, **search_opts)

    gdf_results = gpd.GeoDataFrame.from_features(search_results.geojson()['features'])

    return gdf_results
