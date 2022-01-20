#!/usr/bin/env python

import argparse

import rasterio
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT


def main():
    parser = argparse.ArgumentParser(
        description='Append a raster to data stack'
    )
    parser.add_argument('in_raster', metavar='in_raster',
                        type=str,
                        help=('input raster'))
    parser.add_argument('in_stack', metavar='in_stack',
                        type=str,
                        help=('input stack'))
    parser.add_argument('out_stack', metavar='out_stack',
                        type=str,
                        help=('output stack'))
    parser.add_argument('-b', metavar='band',
                        type=int,
                        default=1,
                        help='band of <in_raster> to append to <out_stack>')
    parser.add_argument('-n', metavar='name',
                        type=str,
                        default='',
                        help='band name of <in_raster> to be in <out_stack>')
    parser.add_argument('-r', metavar='resampling_method',
                        type=str,
                        default='near',
                        choices=[
                            'nearest', 'bilinear', 'cubic', 'cubicspline',
                            'lanczos', 'average', 'rms', 'mode', 'max', 'min',
                            'med', 'q1', 'q3', 'sum'
                        ],
                        help='resampling method for warping <in_raster>')

    args = parser.parse_args()
    band = args.b
    name = args.n
    resampling_method = args.r

    with rasterio.open(args.in_stack) as src_stack:
        # Read stack and metadata of in_stack
        stack = src_stack.read()
        profile = src_stack.profile
        band_count = src_stack.count
        band_names = list(src_stack.descriptions)

        # Warp in_raster onto the grid of in_stack
        vrt_options = {
            'resampling': Resampling[resampling_method],
            'crs': src_stack.crs,
            'transform': src_stack.transform,
            'height': src_stack.height,
            'width': src_stack.width,
        }
        with rasterio.open(args.in_raster) as src_raster:
            with WarpedVRT(src_raster, **vrt_options) as vrt:
                data = vrt.read(band).astype(profile['dtype'])
                mask = vrt.read_masks(band)
                data[mask == 0] = profile['nodata']

        # Append data to stack and write to out_stack
        profile.update(count=band_count+1, compress='lzw')
        band_names.append(name)
        with rasterio.open(args.out_stack, 'w', **profile) as dst_stack:
            for b in range(band_count):
                dst_stack.write(stack[b, :, :], b+1)
            dst_stack.write(data, band_count+1)
            dst_stack.descriptions = band_names

if __name__ == '__main__':
    main()