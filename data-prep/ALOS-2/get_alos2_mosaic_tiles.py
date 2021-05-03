site = 'para_ne'

lat_min = -4
lat_max = 0
lon_min = -51
lon_max = -44

tiles = []
for lat in range(lat_max, lat_min, -1):
    for lon in range(lon_min, lon_max):
        if lat >= 0:
            ns = 'N'
        else:
            ns = 'S'
        if lon >= 0:
            ew = 'E'
        else:
            ew = 'W'
        tile = f'{ns}{abs(lat):02}{ew}{abs(lon):03}'
        tiles.append(tile)

with open(f'alos2_mosaic_list_{site}.txt', 'w') as f:
    f.write('\n'.join(tiles))
