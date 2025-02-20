import argparse
import json
from pathlib import Path

import ee
import geopandas as gpd

from vegmapper import pathurl
from vegmapper.pathurl import ProjDir

# Function to apply cloud masking using the QA_PIXEL band
def maskClouds(image):
    bandList = ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']
    qa = image.select('QA_PIXEL')
    cloud_mask = qa.bitwiseAnd(1 << 3).eq(0).And(qa.bitwiseAnd(1 << 5).eq(0))
    return (image
            .updateMask(cloud_mask)
            .select(bandList)
            .copyProperties(image, ["system:time_start", "CLOUD_COVER"]))


# Function to apply scale factors
def rescaleBands(image):
    optical_bands = image.select('SR_B.').multiply(0.0000275).add(-0.2)
    thermal_bands = image.select('ST_B.*').multiply(0.00341802).add(149.0)
    return image.addBands(optical_bands, None, True).addBands(thermal_bands, None, True)

# Function for Spectral Mizture Analysis and calculating NDFI
def unmixAndNDFI(image, endmembers):
    unmixedImage = ee.Image(image).unmix(
        endmembers=[
            endmembers['gv'],
            endmembers['shade'],
            endmembers['npv'],
            endmembers['soil'],
            endmembers['cloud']
        ],
        sumToOne=True,
        nonNegative=True
    ).rename(['GV', 'Shade', 'NPV', 'Soil', 'Cloud'])
    
    # Calculate NDFI
    ndfi = unmixedImage.expression(
        '((GV / (1 - SHADE)) - (NPV + SOIL)) / ((GV / (1 - SHADE)) + NPV + SOIL)',
        {
            'GV':    unmixedImage.select('GV'),
            'SHADE': unmixedImage.select('Shade'),
            'NPV':   unmixedImage.select('NPV'),
            'SOIL':  unmixedImage.select('Soil')
        }
    ).rename('NDFI')
    
    return image.addBands([unmixedImage, ndfi])


def export_landsat_ndfi(proj_dir, sitename, tiles, res, year, sma_endmembers=None, gs=None):
    print(f'\nSubmitting GEE jobs for exporting Landsat NDVI ...')

    gdf_tiles = gpd.read_file(tiles)
    epsg = gdf_tiles.crs.to_epsg()
    gdf_wgs84 = gdf_tiles.to_crs('epsg:4326')

    # Define endmembers
    if sma_endmembers is not None:
        endmembers = sma_endmembers
    else:
        endmembers = {
            'gv':    [0.05, 0.09, 0.04, 0.61, 0.3, 0.1],
            'shade': [0.0,  0.0,  0.0,  0.0,  0.0, 0.0],
            'npv':   [0.14, 0.17, 0.22, 0.3,  0.55, 0.3],
            'soil':  [0.2,  0.3,  0.34, 0.58, 0.6,  0.58],
            'cloud': [0.9,  0.96, 0.8,  0.78, 0.72, 0.65],
        }

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
            # Get cloud-masked SR
            tile = ee.Geometry.Rectangle(gdf_wgs84['geometry'][i].bounds)
            landsat = (ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
                       .merge(ee.ImageCollection('LANDSAT/LC09/C02/T1_L2'))
                       .filterDate(f'{year}-01-01', f'{year}-12-31')
                       .filterBounds(tile)
                       .map(maskClouds)
                       .map(rescaleBands))
            
            # Apply the SMA and NDFI calculation to the Landsat collection
            landsatNDFI = landsat.map(lambda img: unmixAndNDFI(img, endmembers))

            ndfiMosaic = landsatNDFI.select('NDFI').median().reproject(**{'crs': f'EPSG:{epsg}', 'crsTransform': ct_0}).resample('bilinear')
        
        #Export NDFI Bands
        if gs is not None:
            gs = pathurl.PathURL(gs)
            if gs.storage != 'gs':
                raise Exception('Currently only exporting to Google Storage (gs://).')
            task = ee.batch.Export.image.toCloudStorage(
                bucket=gs.bucket,
                fileNamePrefix=f'{gs.prefix}/{f'landsat_coded_distFlag_{sitename}_{year}_h{h}v{v}'}',
                image=ndfiMosaic,
                description=f'landsat_coded_distFlag_{sitename}_{year}_h{h}v{v}',
                dimensions=f'{xdim}x{ydim}',
                maxPixels=1e9,
                crs=f'EPSG:{epsg}',
                crsTransform=ct_1
            )
        else:
            task = ee.batch.Export.image.toDrive(
                image=ndfiMosaic,
                description=f'landsat_coded_distFlag_{sitename}_{year}_h{h}v{v}',
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
                        help='Year of ndfi mosaic calculation')
    parser.add_argument('sma_endmembers', metavar='sma_endmembers',
                        type=list,
                        help='endmembers for Spectral Mixture Analysis')
    args = parser.parse_args()

    export_landsat_ndfi(args.sitename, args.tiles, args.res, args.year)

if __name__ == '__main__':
    main()

