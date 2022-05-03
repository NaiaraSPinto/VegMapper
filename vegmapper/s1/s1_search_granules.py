#!/usr/bin/env python

import argparse
import warnings
from functools import reduce

import asf_search as asf
import geopandas as gpd


def skim_granules(geojson_granules, geojson_boundary, frame_number_tol=1):
    # Read in GeoDataFrames
    gdf_granules = gpd.read_file(geojson_granules)
    gdf_boundary = gpd.read_file(geojson_boundary).dissolve()

    # Group path-frames
    gb = gdf_granules.groupby(['pathNumber', 'frameNumber'])
    gdf_frames = gpd.GeoDataFrame(
        [[p,
          f,
          reduce(lambda x, y: x.intersection(y), gb.get_group((p, f)).geometry)]
        for p, f in gb.groups.keys()],
        columns=['pathNumber', 'frameNumber', 'geometry']
    )

    gdf_frames['area_with_aoi'] = None
    gdf_frames['geometry_diff'] = None
    for i, row in gdf_frames.iterrows():
        # Drop the frames with frameNumber within frame_number_tol as they
        # are nearly identical to the current frame i
        idx_to_drop = gdf_frames.index[
            (gdf_frames.pathNumber == row['pathNumber']) &
            ((gdf_frames.frameNumber - row['frameNumber']).abs() <= frame_number_tol)
        ]

        # Add back the current frame i to the front of list
        idx = [i] + gdf_frames.index.drop(idx_to_drop).to_list()

        # Subtract other frames from current frame i and get remaining
        # intersected area with aoi
        geom = reduce(lambda x, y: x.difference(y), gdf_frames.geometry[idx])
        area = gdf_boundary.geometry[0].intersection(geom).area
        gdf_frames.loc[i, 'area_with_aoi'] = area
        # Assignment of multi-polygon needs special treatment.
        # https://github.com/geopandas/geopandas/issues/992
        gdf_frames.loc[[i], 'geometry_diff'] = gpd.GeoSeries([geom])

    print(f'\n{(gdf_frames.area_with_aoi == 0).sum()} frames are skimmed from the search results')

    gdf_granules = gdf_granules.merge(gdf_frames.loc[gdf_frames.area_with_aoi > 0, ['pathNumber', 'frameNumber']],
                                      on=['pathNumber', 'frameNumber'])

    geojson_skimmed_granules = geojson_granules.replace('s1_granules', 's1_skimmed_granules')
    geojson_frames = geojson_granules.replace('s1_granules', 's1_frames')
    with warnings.catch_warnings():
        # Ignore the FutureWarning, which will be fixed in the next release of geopandas
        warnings.simplefilter("ignore")
        gdf_granules.to_file(geojson_skimmed_granules, driver='GeoJSON')
        gdf_frames.drop(columns=['geometry_diff']).to_file(geojson_frames, driver='GeoJSON')
    print(f'\nMetadata of S1 skimmed granules saved to: {geojson_skimmed_granules}')
    print(f'Metadata of S1 frames saved to: {geojson_frames}')

    return gdf_granules, geojson_skimmed_granules


def search_granules(aoi_name,
                    aoi_boundary,
                    start,
                    end,
                    processingLevel=asf.PRODUCT_TYPE.GRD_HD,
                    beamMode=asf.BEAMMODE.IW,
                    polarization=asf.POLARIZATION.VV_VH,
                    flightDirection=None,
                    skim=True):

    # Get WKT of AOI boundary
    gdf_boundary = gpd.read_file(aoi_boundary).dissolve()
    aoi_wkt = gdf_boundary.simplify(0.1).geometry[0].wkt

    search_opts = {
        'start': start,
        'end': end,
        'platform': asf.PLATFORM.SENTINEL1,
        'processingLevel': processingLevel,
        'beamMode': beamMode,
        'polarization': polarization,
    }

    # Search granules
    if flightDirection is None:
        search_opts['flightDirection'] = asf.FLIGHT_DIRECTION.ASCENDING
        search_results_a = asf.geo_search(intersectsWith=aoi_wkt, **search_opts)
        print(f'{len(search_results_a)} granules found for ASCENDING orbits.')

        search_opts['flightDirection'] = asf.FLIGHT_DIRECTION.DESCENDING
        search_results_d = asf.geo_search(intersectsWith=aoi_wkt, **search_opts)
        print(f'{len(search_results_d)} granules found for DESCENDING orbits.')

        if len(search_results_a) >= len(search_results_d):
            search_results = search_results_a
            print('\nThe granules of ASCENDING orbits will be used.')
        else:
            search_results = search_results_d
            print('\nThe granules of DESCENDING orbits will be used.')
    elif flightDirection.lower() in ['a', 'ascending']:
        search_opts['flightDirection'] = asf.FLIGHT_DIRECTION.ASCENDING
        search_results = asf.geo_search(intersectsWith=aoi_wkt, **search_opts)
        print(f'{len(search_results)} granules found for ASCENDING orbits.')
    elif flightDirection.lower() in ['d', 'descending']:
        search_opts['flightDirection'] = asf.FLIGHT_DIRECTION.DESCENDING
        search_results = asf.geo_search(intersectsWith=aoi_wkt, **search_opts)
        print(f'{len(search_results)} granules found for DESCENDING orbits.')
    else:
        raise Exception(f'{flightDirection} is not a valid flightDirection')

    # Convert search results to a GeoDataFrame
    gdf_granules = gpd.GeoDataFrame.from_features(search_results.geojson()['features'])
    gdf_granules['browse'] = gdf_granules['browse'].apply(lambda x: x[0])
    gdf_granules['pathNumber'] = gdf_granules['pathNumber'].astype(int)
    gdf_granules['frameNumber'] = gdf_granules['frameNumber'].astype(int)

    # Save search results to a geojson
    out_dir = aoi_boundary.rstrip(aoi_boundary.split('/')[-1]).strip('/')
    geojson_granules = f'{out_dir}/{aoi_name}_s1_granules_{start.replace("-", "")}-{end.replace("-", "")}.geojson'
    with warnings.catch_warnings():
        # Ignore the FutureWarning, which will be fixed in the next release of geopandas
        warnings.simplefilter("ignore")
        gdf_granules.to_file(geojson_granules, driver='GeoJSON')
    print(f'\nMetadata of S1 granules saved to: {geojson_granules}')

    # Skim the unnecessary granules
    if skim:
        gdf_granules, geojson_granules = skim_granules(geojson_granules, aoi_boundary)

    return gdf_granules, geojson_granules


def main():
    parser = argparse.ArgumentParser(
        description='Search Sentinel-1 granules for an area of interest (AOI)'
    )
    parser.add_argument('aoi_name', type=str,
                        help='name of AOI')
    parser.add_argument('aoi_boundary', type=str,
                        help='boundary of AOI (shp/geojson)')
    parser.add_argument('start', type=str,
                        help='start date (YYYY-MM-DD)')
    parser.add_argument('end', type=str,
                        help='end date (YYYY-MM-DD)')
    args = parser.parse_args()

    search_granules(args.aoi_name, args.aoi_boundary, args.start, args.end)


if __name__ == "__main__":
    main()