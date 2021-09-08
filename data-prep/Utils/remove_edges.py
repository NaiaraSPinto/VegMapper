#!/usr/bin/env python

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import rasterio


def extract_inner_edges(mask):
    """
    Extract inner edges of an image mask.
    An inner edge pixel is defined as:
        pixel value = 1 & at least one adjacent (rook's case) pixel is 0

    Parameters
    ----------
    mask: uint8 np.array
        Image mask.

    Returns
    -------
    edge: uint8 np.array
        Inner edges.
    """
    if mask.dtype != np.uint8:
        raise TypeError('Input mask is not of type np.uint8')

    # Make non-zero values be 1
    mask[mask > 0] = 1

    # Row and column of ones
    height, width = mask.shape
    row_ones = np.ones((1, width), np.uint8)
    col_ones = np.ones((height, 1), np.uint8)

    # Summing center and four neighbors of each pixel
    asum = np.copy(mask)
    asum += np.concatenate((mask[:, 1:], col_ones), axis=1)
    asum += np.concatenate((col_ones, mask[:, :-1]), axis=1)
    asum += np.concatenate((mask[1:, :], row_ones), axis=0)
    asum += np.concatenate((row_ones, mask[:-1, :]), axis=0)

    # Inner edges
    edge = np.uint8((mask == 1) & (asum < 5))

    return edge


def identify_side_edges(mask, left_and_right=True):
    """
    Identify side (left/right or top/bottom) inner edges for an image mask that
    is somewhat rectangular (e.g., a mask for a radar swath).

    Parameters
    ----------
    mask: uint8 np.array
        Image mask.

    left_and_right: bool
        True to return left/right edges; False to return top/bottom edges.

    Returns
    -------
    side_edge: uint8 np.array
        Side edges.
    """
    # Extract edges of mask
    edge = extract_inner_edges(mask)

    # Row/column indices of edge pixels
    row, col = np.where(edge == 1)

    # Get row/col indices of corner pixels. Note that there can be more than one
    # pixels with row=min(row) for "top", same for "bottom", "left", "right"
    row_top = row[np.argmin(row)]
    # col_top = col[np.argmin(row)]
    row_bottom = row[np.argmax(row)]
    # col_bottom = col[np.argmax(row)]
    row_left = row[np.argmin(col)]
    col_left = col[np.argmin(col)]
    row_right = row[np.argmax(col)]
    col_right = col[np.argmax(col)]

    # Get upper left (ul), upper right (ur), lower right (ur), lower left (ll)
    if row_left > row_right:    # top = upper left
        row_ul = row_top
        col_ul = col[row == row_ul][0]
        row_lr = row_bottom
        col_lr = col[row == row_lr][-1]
        col_ll = col_left
        row_ll = row[col == col_ll][-1]
        col_ur = col_right
        row_ur = row[col == col_ur][0]
    else:                       # top = upper right
        row_ur = row_top
        col_ur = col[row == row_ur][-1]
        row_ll = row_bottom
        col_ll = col[row == row_ll][0]
        col_ul = col_left
        row_ul = row[col == col_ul][0]
        col_lr = col_right
        row_lr = row[col == col_lr][-1]

    side_edge = np.zeros(edge.shape, np.uint8)
    if left_and_right:
        # Right edge
        idx = np.where((row > np.min([row_ur, row_lr])) &
                       (row < np.max([row_ur, row_lr])) &
                       (col >= np.min([col_ur, col_lr])) &
                       (col <= np.max([col_ur, col_lr])))
        side_edge[row[idx], col[idx]] = 1

        # Left edge
        idx = np.where((row > np.min([row_ul, row_ll])) &
                       (row < np.max([row_ul, row_ll])) &
                       (col >= np.min([col_ul, col_ll])) &
                       (col <= np.max([col_ul, col_ll])))
        side_edge[row[idx], col[idx]] = 1
    else:
        # Top edge
        idx = np.where((row >= np.min([row_ul, row_ur])) &
                       (row <= np.max([row_ul, row_ur])) &
                       (col > np.min([col_ul, col_ur])) &
                       (col < np.max([col_ul, col_ur])))
        side_edge[row[idx], col[idx]] = 1
        # Bottom edge
        idx = np.where((row >= np.min([row_ll, row_lr])) &
                       (row <= np.max([row_ll, row_lr])) &
                       (col > np.min([col_ll, col_lr])) &
                       (col < np.max([col_ll, col_lr])))
        side_edge[row[idx], col[idx]] = 1

    side_edge[row_ur, col_ur] = 2
    side_edge[row_lr, col_lr] = 2
    side_edge[row_ll, col_ll] = 2
    side_edge[row_ul, col_ul] = 2

    return side_edge


def main():
    parser = argparse.ArgumentParser(
        description='remove edges of a raster image')
    parser.add_argument('srcfile', metavar='srcfile',
                        type=Path,
                        help='source raster file')
    parser.add_argument('dstfile', metavar='dstfile',
                        type=Path,
                        help='destination raster file')
    parser.add_argument('--edge_depth', metavar='edge_depth', dest='edge_depth',
                        type=int,
                        default=1,
                        help=('edge depth to be removed'))
    parser.add_argument('--maskfile', metavar='maskfile',
                        type=Path,
                        help='mask raster file to replace the mask of srcfile')
    parser.add_argument('--edgefile', metavar='edgefile',
                        type=Path,
                        help='output edge raster file')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--lr_only',
                       action="store_true",
                       help=('remove only left and right edges'))
    group.add_argument('--tb_only',
                       action="store_true",
                       help=('remove only top and bottom edges'))
    args = parser.parse_args()

    with rasterio.open(args.srcfile) as src:
        data = src.read(1)
        src_mask = src.read_masks(1)
        profile = src.profile
        nodata = src.nodata

    if args.maskfile:
        with rasterio.open(args.maskfile) as dset:
            mask = dset.read_masks(1)
            mask[(src_mask == 255) & (mask == 0)] = 255
    else:
        mask = src_mask

    edge = np.zeros(mask.shape, np.uint8)
    for i in range(args.edge_depth):
        if args.lr_only:
            side_edge = identify_side_edges(mask, left_and_right=True)
        elif args.tb_only:
            side_edge = identify_side_edges(mask, left_and_right=False)
        else:
            side_edge = extract_inner_edges(mask)
        mask[side_edge > 0] = 0
        edge[side_edge > 0] = side_edge[side_edge > 0]

    if nodata is None:
        raise Exception('No nodata value set for srcfile. '
                        'Set a nodata value first')
    else:
        data[edge > 0] = nodata
    with rasterio.open(args.dstfile, 'w', **profile) as dst:
        dst.write(data, 1)

    profile.update(dtype=np.uint8, nodata=0)
    if args.edgefile:
        with rasterio.open(args.edgefile, 'w', **profile) as dst:
            dst.write(edge, 1)


if __name__ == '__main__':
    main()
