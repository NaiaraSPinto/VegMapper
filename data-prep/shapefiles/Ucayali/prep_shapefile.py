import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon

state = 'Ucayali'
adm1_shp = 'per_adm_ign_20200714_shp/per_admbnda_adm1_ign_20200714.shp'

gdf = gpd.read_file(adm1_shp)
gdf.crs = 'epsg:4326'

# State boundary
idx = gdf.index[gdf.ADM1_ES == state]
gdf = gdf.iloc[idx, :].reset_index(drop=True)
gdf.to_file(f'{state}.shp')

# UTM tiles
crs = 'epsg:32718'
gdf = gdf.to_crs(crs)
t_size = 300000
t_xmin = np.floor(gdf.bounds.minx[0] / t_size) * t_size
t_ymin = np.floor(gdf.bounds.miny[0] / t_size) * t_size
t_xmax = np.ceil(gdf.bounds.maxx[0] / t_size) * t_size
t_ymax = np.ceil(gdf.bounds.maxy[0] / t_size) * t_size
t_xs = np.arange(t_xmin, t_xmax, t_size)
t_ys = np.arange(t_ymax, t_ymin, -t_size)

print(f't_xmin = {t_xmin}')
print(f't_ymin = {t_ymin}')
print(f't_xmax = {t_xmax}')
print(f't_ymax = {t_ymax}')

hs = []
vs = []
polygons = []
for h, x in enumerate(t_xs):
    for v, y in enumerate(t_ys):
        xs = [x, x+t_size, x+t_size, x, x]
        ys = [y, y, y-t_size, y-t_size, y]
        p = Polygon(zip(xs, ys))
        hs.append(h)
        vs.append(v)
        polygons.append(p)
gdf2 = gpd.GeoDataFrame({'h': hs,
                         'v': vs},
                        crs=crs,
                        geometry=polygons)
gdf2.to_file(f'{state}_tiles_utm_18s.shp')
