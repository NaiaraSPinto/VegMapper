import argparse

import ee
import geopandas as gpd

res = 30

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('state', help='state name')
parser.add_argument('year', type=int, help='year (1900 - 2100)')
parser.add_argument('-h', '--help', action='help', help='submit GEE processing for Landsat NDVI')
args = parser.parse_args()

state = args.state.lower()
year = args.year

if year < 1900 or year > 2100:
    raise Exception('year must be a 4-digit number between 1900 and 2100')

print(f'state: {state}')
print(f'year: {year}')

gdf_tiles = gpd.read_file('../AOI/{state}/{state}_tiles.geojson')
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
        # Export data to Google Drive
        task = ee.batch.Export.image.toDrive(image=modisTreeCover,
                                             description=f'modis_tc_{state}_{year}_h{h}v{v}',
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
