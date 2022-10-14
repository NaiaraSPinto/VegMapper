#!/usr/bin/env python

import argparse

import ee
import geopandas as gpd

from vegmapper import pathurl


def export_modis_tc(sitename, tiles, res, year, gs=None):
    print(f'\nSubmitting GEE jobs for exporting MODIS tree cover ...')

    gdf_tiles = gpd.read_file(tiles)
    epsg = gdf_tiles.crs.to_epsg()

    ee.Initialize()

    modisTreeCover = ee.ImageCollection('MODIS/006/MOD44B').filterDate(f'{year}-01-01', f'{year}-12-31').select('Percent_Tree_Cover').first()

    # Export data for each tile
    task_list = []
    for i in gdf_tiles.index:
        h = gdf_tiles['h'][i]
        v = gdf_tiles['v'][i]
        m = gdf_tiles['mask'][i]
        g = gdf_tiles['geometry'][i]
        xmin = g.bounds[0]
        ymin = g.bounds[1]
        xmax = g.bounds[2]
        ymax = g.bounds[3]
        xdim = int((xmax - xmin) / res)
        ydim = int((ymax - ymin) / res)

        # Preferred crsTransform (pixel corner coordinates are multiples of res)
        ct = [res, 0, xmin, 0, -res, ymax]

        if m == 1:
            if gs is not None:
                gs = pathurl.PathURL(gs)
                if gs.storage != 'gs':
                    raise Exception('Currently GEE only supports exporting data to Google Storage buckets (gs://).')
                # Export data to Google Storage bucket
                task = ee.batch.Export.image.toCloudStorage(
                    bucket=gs.bucket,
                    fileNamePrefix=f'{gs.prefix}/modis_tc_{sitename}_{year}_h{h}v{v}',
                    image=modisTreeCover,
                    description=f'modis_tc_{sitename}_{year}_h{h}v{v}',
                    dimensions=f'{xdim}x{ydim}',
                    maxPixels=1e9,
                    crs=f'EPSG:{epsg}',
                    crsTransform=ct
                )
            else:
                task = ee.batch.Export.image.toDrive(
                    image=modisTreeCover,
                    description=f'modis_tc_{sitename}_{year}_h{h}v{v}',
                    dimensions=f'{xdim}x{ydim}',
                    maxPixels=1e9,
                    crs=f'EPSG:{epsg}',
                    crsTransform=ct
                )
            task.start()
            task_list.append(task)

            print(f'#{i+1}: h{h}v{v} started')
        else:
            print(f'#{i+1}: h{h}v{v} skipped')

    return task_list


def main():
    parser = argparse.ArgumentParser(
        description='submit GEE processing for MODIS tree cover'
    )
    parser.add_argument('sitename',
                        type=str,
                        help='site name')
    parser.add_argument('tiles',
                        type=str,
                        help=('shp/geojson file that contains tiles onto which '
                              'the output raster will be resampled'))
    parser.add_argument('res',
                        type=int,
                        help='resolution')
    parser.add_argument('year',
                        type=int,
                        help='year of dataset')
    # parser.add_argument('--gs_url',
    #                     type=str,
    #                     help=('Google Cloud Storage URL'))
    # parser.add_argument('--proj_dir',
    #                     type=str,
    #                     help=('project directory (s3:// or gs:// or local dirs)'))
    args = parser.parse_args()

    export_modis_tc(args.sitename, args.tiles, args.res, args.year)


if __name__ == '__main__':
    main()
