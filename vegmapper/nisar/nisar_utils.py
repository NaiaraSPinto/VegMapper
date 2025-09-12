import h5py
import requests
import io
import xarray as xr
import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_origin
import rioxarray
from rasterio.enums import Resampling

def nisar_tmean(df: pd.DataFrame, which_pol: str = "HHHH", pixel_size: float = 30.0) -> pd.DataFrame:
    out_paths = {}

    for granule_id, group in df.groupby("granule_id"):
        urls = group["link"].tolist()
        times = group["acq_datetime"].tolist()

        sigma_list = []
        x_coords = None
        y_coords = None
        epsg = None

        print(f"\nProcessing granule: {granule_id}")

        for url in urls:
            print(f"Opening {url}")
            resp = requests.get(url)
            resp.raise_for_status()

            with h5py.File(io.BytesIO(resp.content), "r") as f:
                if x_coords is None:
                    x_coords = f["science"]["LSAR"]["GCOV"]["grids"]["frequencyA"]["xCoordinates"][()]
                    y_coords = f["science"]["LSAR"]["GCOV"]["grids"]["frequencyA"]["yCoordinates"][()]
                    epsg = f["science"]["LSAR"]["GCOV"]["grids"]["frequencyA"]["projection"][()].item()

                sigma = f["science"]["LSAR"]["GCOV"]["grids"]["frequencyA"][which_pol][()]
                sigma_list.append(sigma)

        # Stack into xarray
        sigma_stack = np.stack(sigma_list)

        da = xr.DataArray(
            sigma_stack,
            dims=("time", "y", "x"),
            coords={
                "time": times,
                "x": x_coords,
                "y": y_coords,
            },
            name=f"sigma0_{which_pol}"
        )

        # Temporal mean
        da_mean = da.mean(dim="time", skipna=True)

        # Assign CRS and spatial reference for reprojection
        da_mean = da_mean.rio.write_crs(f"EPSG:{epsg}", inplace=True)

        # Reproject/resample to define pixel posting
        da_resampled = da_mean.rio.reproject(
            da_mean.rio.crs,
            resolution=pixel_size,
            resampling=Resampling.bilinear   # or Resampling.nearest
        )


        # Time range
        first_dt = min(times).strftime("%Y%m%dT%H%M%S")
        last_dt = max(times).strftime("%Y%m%dT%H%M%S")

        # Output filename
        out_name = f"NISAR_{granule_id}_{which_pol}_tmean_{first_dt}_{last_dt}.tif"
        print(f"Exporting {out_name}")

        # Save to GeoTIFF 
        da_resampled.rio.to_raster(out_name)

### Pseudo code that in the future will map NISAR frames into the predefined tiles. 
import geopandas as gpd
import warnings

def map_nisar2tile(reference_tiles, nisar_gdf, output_dir):
    """
    Maps NISAR frames (from a GeoDataFrame) to their corresponding reference tiles.

    Parameters
    ----------
    reference_tiles : str
        Path to the reference tiles GeoJSON.
    nisar_gdf : GeoDataFrame
        GeoDataFrame containing columns: acq_datetime, granule_id, link, geometry.
    output_dir : str
        Directory where the CSV mapping will be saved.

    Returns
    -------
    tile_gdf : GeoDataFrame
        Reference tiles GeoDataFrame with a new column 'overlapping_frames'.
    """

    # 1. Load reference tiles as GeoDataFrame
    tile_gdf = gpd.read_file(reference_tiles)

    # 2. Copy NISAR GeoDataFrame
    frames_gdf = nisar_gdf.copy()

    # 3. Standardize CRS
    tile_crs = tile_gdf.crs
    tile_epsg_number = tile_crs.to_string().split(':')[-1]

    # Assume NISAR geometries are in WGS84 (EPSG:4326)
    frames_gdf.set_crs(epsg=4326, inplace=True)

    # Reproject NISAR frames to match tiles CRS
    frames_gdf = frames_gdf.to_crs(epsg=tile_epsg_number)
    tile_gdf = tile_gdf.to_crs(epsg=tile_epsg_number)

    # 4. Spatial join to find overlapping frames
    # Use 'granule_id' as the identifier
    overlapping_gdf = gpd.sjoin(frames_gdf, tile_gdf, how="inner", predicate="intersects")

    # 5. Group overlaps by tile index
    overlap_dict = overlapping_gdf.groupby('index_right')['granule_id'].apply(list).to_dict()

    # 6. Add overlapping frames to tile GeoDataFrame
    tile_gdf['overlapping_frames'] = tile_gdf.index.map(overlap_dict)

    # 7. Prepare for CSV export
    def join_lists(cell):
        if isinstance(cell, list):
            return ','.join(cell)
        return cell

    gdf2export = tile_gdf.copy()
    gdf2export['overlapping_frames'] = gdf2export['overlapping_frames'].apply(join_lists)

    # Remove invalid geometries
    gdf2export = gdf2export[gdf2export['geometry'].notnull()]
    gdf2export = gdf2export[gdf2export['geometry'].apply(lambda x: hasattr(x, "wkt"))]

    # Convert geometry to WKT for CSV
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        gdf2export['geometry'] = gdf2export['geometry'].apply(lambda x: x.wkt)

    # 8. Export to CSV
    gdf2export.to_csv(f"{output_dir}/nisar_frames_to_tile_map.csv", index=False)

    # 9. Return tile GeoDataFrame with overlap info
    return tile_gdf