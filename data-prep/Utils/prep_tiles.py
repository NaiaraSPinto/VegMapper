import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon
from unidecode import unidecode

# User input
state = 'Para'      # State name
epsg = 32722        # Target projection EPSG
t_size = 150000     # Tile size in meters
adm1_shp = 'bra_adm_ibge_2020_shp/bra_admbnda_adm1_ibge_2020.shp'   # Shapefile of subnational administrative boundaries 
adm1_col = 'ADM1_PT'                                                # Column name of states (state is usually adm1)

gdf = gpd.read_file(adm1_shp)
gdf.crs = 'epsg:4326'

# Extract state boundary
state = state.lower()
for i in gdf.index:
    gdf.loc[i, adm1_col] = unidecode(gdf.loc[i, adm1_col]).lower()
idx = gdf.index[gdf.loc[:, adm1_col] == state]
gdf = gdf.iloc[idx, :].reset_index(drop=True)
out_boundary = f'{state}_boundary.geojson'
gdf.to_file(out_boundary, driver='GeoJSON')
print(f'Boundary for {state.capitalize()}: {out_boundary}')

# Get UTM tiles
crs = f'epsg:{epsg}'
gdf = gdf.to_crs(crs)
t_xmin = np.floor(gdf.bounds.minx[0] / t_size) * t_size
t_ymin = np.floor(gdf.bounds.miny[0] / t_size) * t_size
t_xmax = np.ceil(gdf.bounds.maxx[0] / t_size) * t_size
t_ymax = np.ceil(gdf.bounds.maxy[0] / t_size) * t_size
t_xs = np.arange(t_xmin, t_xmax, t_size)
t_ys = np.arange(t_ymax, t_ymin, -t_size)

state_boundary = gdf.geometry[0]

hs = []
vs = []
mask = []
polygons = []
for h, x in enumerate(t_xs):
    for v, y in enumerate(t_ys):
        xs = [x, x+t_size, x+t_size, x, x]
        ys = [y, y, y-t_size, y-t_size, y]
        p = Polygon(zip(xs, ys))
        m = state_boundary.intersects(p)
        hs.append(h)
        vs.append(v)
        if m:
            mask.append(1)
        else:
            mask.append(0)
        polygons.append(p)
gdf2 = gpd.GeoDataFrame({'h': hs,
                         'v': vs,
                         'mask': mask},
                        crs=crs,
                        geometry=polygons)
out_tiles = f'{state}_tiles.geojson'
gdf2.to_file(out_tiles, driver='GeoJSON')
print(f'Tiles for {state.capitalize()}: {out_tiles}')

print(f"{gdf2['mask'].sum()} out of {len(gdf2['mask'])} tiles intersecting {state.capitalize()}")
