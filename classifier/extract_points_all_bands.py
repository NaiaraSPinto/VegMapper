import geopandas as gpd
import rasterio

src_file = '/Users/nspinto/Documents/sw/VegMapper/classifier/para/para_sample_fin.geojson'
dst_file = '/Users/nspinto/Documents/sw/VegMapper/classifier/para/para_sample_fin_rs_pred.csv'

vrt = 'servir-stacks/para/2020/all-bands/para_virtual_stack_2020.vrt'

gdf = gpd.read_file(src_file)

gdf['coords.x1'] = float('nan')
gdf['coords.x2'] = float('nan')
gdf['c_vv'] = float('nan')
gdf['c_vh'] = float('nan')
gdf['c_inc'] = float('nan')
gdf['l_hh'] = float('nan')
gdf['l_hv'] = float('nan')
gdf['l_inc'] = float('nan')
gdf['ndvi'] = float('nan')
gdf['tree_cover'] = float('nan')
gdf['prodes'] = float('nan')

with rasterio.open(f'/vsis3/{vrt}') as dset:
    gdf_utm = gdf.to_crs(dset.crs)
    xy = [(g.x, g.y) for g in gdf_utm.geometry]
    for i, values in enumerate(dset.sample(xy)):
        print(f'Extracting band values for points: {i}/{len(xy)}')
        gdf.loc[i, 'coords.x1'] = xy[i][0]
        gdf.loc[i, 'coords.x2'] = xy[i][1]
        gdf.loc[i, 'c_vv'] = values[0]
        gdf.loc[i, 'c_vh'] = values[1]
        gdf.loc[i, 'c_inc'] = values[2]
        gdf.loc[i, 'l_hh'] = values[3]
        gdf.loc[i, 'l_hv'] = values[4]
        gdf.loc[i, 'l_inc'] = values[5]
        gdf.loc[i, 'ndvi'] = values[6]
        gdf.loc[i, 'tree_cover'] = values[7]
        gdf.loc[i, 'prodes'] = values[8]

gdf.to_csv(dst_file)
