#!/usr/bin/env python

import argparse
import json
from pathlib import Path

import ee
import geopandas as gpd

from vegmapper import pathurl
from vegmapper.pathurl import ProjDir


# Function to mask clouds based on the pixel_qa band of Landsat 8 SR data.
# @param {ee.Image} image input Landsat 8 SR image
# @return {ee.Image} cloudmasked Landsat 8 image
def maskL8sr(image):
    # Bits 3 and 5 are cloud shadow and cloud, respectively.
    cloudShadowBitMask = (1 << 3)
    cloudsBitMask = (1 << 5)
    # Get the pixel QA band.
    qa = image.select('pixel_qa')
    # Both flags should be set to zero, indicating clear conditions.
    mask = qa.bitwiseAnd(cloudShadowBitMask).eq(0).And(qa.bitwiseAnd(cloudsBitMask).eq(0))
    return image.updateMask(mask)


# Function to add NDVI band
def addNDVI(image):
    ndvi = image.normalizedDifference(['B5', 'B4']).rename('NDVI')
    return image.addBands(ndvi)


def export_landsat_ndvi(proj_dir, sitename, tiles, res, year, gs=None):
    print(f'\nSubmitting GEE jobs for exporting Landsat NDVI ...')

    gdf_tiles = gpd.read_file(tiles)
    epsg = gdf_tiles.crs.to_epsg()
    gdf_wgs84 = gdf_tiles.to_crs('epsg:4326')

    ee.Initialize()

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

        # Native crsTransform of Landsat data (pixel center coordinates are multiples of res)
        ct_0 = [res, 0, xmin-res/2, 0, -res, ymax+res/2]

        # Preferred crsTransform (pixel corner coordinates are multiples of res)
        ct_1 = [res, 0, xmin, 0, -res, ymax]

        if m == 1:
            # Get cloud-masked SR median
            tile = ee.Geometry.Rectangle(gdf_wgs84['geometry'][i].bounds)
            sr = ee.ImageCollection('LANDSAT/LC08/C02/T1_L2').filterDate(f'{year}-01-01', f'{year}-12-31').map(maskL8sr).filterBounds(tile).median()

            # Set crs and crsTransform to the native ones and use bilinear interpolation when exported
            ndvi = addNDVI(sr).select('NDVI').reproject(**{'crs': f'EPSG:{epsg}', 'crsTransform': ct_0}).resample('bilinear')

            if gs is not None:
                gs = pathurl.PathURL(gs)
                if gs.storage != 'gs':
                    raise Exception('Currently GEE only supports exporting data to Google Storage buckets (gs://).')
                # Export data to Google Storage bucket
                task = ee.batch.Export.image.toCloudStorage(
                    bucket=gs.bucket,
                    fileNamePrefix=f'{gs.prefix}/landsat_ndvi_{sitename}_{year}_h{h}v{v}',
                    image=ndvi,
                    description=f'landsat_ndvi_{sitename}_{year}_h{h}v{v}',
                    dimensions=f'{xdim}x{ydim}',
                    maxPixels=1e9,
                    crs=f'EPSG:{epsg}',
                    crsTransform=ct_1
                )
            else:
                task = ee.batch.Export.image.toDrive(
                    image=ndvi,
                    description=f'landsat_ndvi_{sitename}_{year}_h{h}v{v}',
                    dimensions=f'{xdim}x{ydim}',
                    maxPixels=1e9,
                    crs=f'EPSG:{epsg}',
                    crsTransform=ct_1
                )
            task.start()
            task_list.append(task)

            print(f'#{i+1}: h{h}v{v} started')
        else:
            print(f'#{i+1}: h{h}v{v} skipped')

    # Save export destinations
    proj_dir = ProjDir(proj_dir)
    dst_dir = proj_dir / 'landsat' / f'{year}'
    export_dst = {task.config['description']: task.config['fileExportOptions'] for task in task_list}
    if dst_dir.is_local:
        export_dst_json = Path(f'{dst_dir}/export_dst.json')
        if not export_dst_json.parent.exists():
            export_dst_json.parent.mkdir(parents=True)
        with open(export_dst_json, 'w') as f:
            json.dump(export_dst, f)

    return task_list


def main():
    parser = argparse.ArgumentParser(
        description='submit GEE processing for Landsat NDVI'
    )
    parser.add_argument('sitename', metavar='sitename',
                        type=str,
                        help='site name')
    parser.add_argument('tiles', metavar='tiles',
                        type=str,
                        help=('shp/geojson file that contains tiles onto which '
                              'the output raster will be resampled'))
    parser.add_argument('res', metavar='res',
                        type=int,
                        help='resolution')
    parser.add_argument('year', metavar='year',
                        type=int,
                        help='year of dataset')
    args = parser.parse_args()

    export_landsat_ndvi(args.sitename, args.tiles, args.res, args.year)


if __name__ == '__main__':
    main()
