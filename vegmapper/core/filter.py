import cv2 as cv
import numpy as np

def enhanced_lee(img, win_size, num_looks=1, nodata=None):
    src_dtype = img.dtype
    img = img.astype(np.float64)

    # Get image mask (0: nodata; 1: data)
    mask = np.ones(img.shape)
    mask[img == nodata] = 0
    mask[np.isnan(img)] = 0     # in case there are pixels of NaNs

    # Change nodata pixels to 0 so they don't contribute to the sums
    img[mask == 0] = 0

    # Kernel size
    ksize = (win_size, win_size)

    # Window sum of image values
    img_sum = cv.boxFilter(img, -1, ksize,
                           normalize=False, borderType=cv.BORDER_ISOLATED)
    # Window sum of image values squared
    img2_sum = cv.boxFilter(img**2, -1, ksize,
                            normalize=False, borderType=cv.BORDER_ISOLATED)
    # Pixel number within window
    pix_num = cv.boxFilter(mask, -1, ksize,
                           normalize=False, borderType=cv.BORDER_ISOLATED)

    # There might be a loss of accuracy as how boxFilter handles floating point
    # number subtractions/additions, causing a window of all zeros to have
    # non-zero sum, hence correction here using np.isclose.
    img_sum[np.isclose(img_sum, 0)] = 0
    img2_sum[np.isclose(img2_sum, 0)] = 0

    # Get image mean and std within window
    img_mean = np.full(img.shape, np.nan, dtype=np.float64)     # E[X]
    img2_mean = np.full(img.shape, np.nan, dtype=np.float64)    # E[X^2]
    img_mean2 = np.full(img.shape, np.nan, dtype=np.float64)    # (E[X])^2
    img_std = np.full(img.shape, 0, dtype=np.float64)           # sqrt(E[X^2] - (E[X])^2)

    idx = np.where(pix_num != 0)                # Avoid division by zero
    img_mean[idx] = img_sum[idx]/pix_num[idx]
    img2_mean[idx] = img2_sum[idx]/pix_num[idx]
    img_mean2 = img_mean**2

    idx = np.where(~np.isclose(img2_mean, img_mean2))           # E[X^2] and (E[X])^2 are close
    img_std[idx] = np.sqrt(img2_mean[idx] - img_mean2[idx])

    # Get weighting function
    k = 1
    cu = 0.523/np.sqrt(num_looks)
    cmax = np.sqrt(1 + 2/num_looks)
    ci = img_std / img_mean         # it's fine that img_mean could be zero here
    w_t = np.zeros(img.shape)
    w_t[ci <= cu] = 1
    idx = np.where((cu < ci) & (ci < cmax))
    w_t[idx] = np.exp((-k * (ci[idx] - cu)) / (cmax - ci[idx]))

    # Apply weighting function
    img_filtered = (img_mean * w_t) + (img * (1 - w_t))

    # Assign nodata value
    img_filtered[pix_num == 0] = nodata

    return img_filtered.astype(src_dtype)