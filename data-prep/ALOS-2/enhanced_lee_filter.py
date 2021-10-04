import cv2 as cv
import numpy as np

def enhanced_lee_filter(img, win_size, num_looks=1, nodata=np.nan):
    # Get image mask (0: no data; 1: data)
    mask = np.ones(img.shape)
    if np.isnan(nodata):
        mask[np.isnan(mask)] = 0
    else:
        mask[mask == nodata] = 1

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

    # Get image mean and std within window
    img_mean = np.ones(img.shape)*nodata
    img_std = np.ones(img.shape)*nodata
    idx = np.where(pix_num != 0)
    img_mean[idx] = img_sum[idx] / pix_num[idx]
    img_std[idx] = np.sqrt(img2_sum[idx] / pix_num[idx] - img_mean[idx]**2)

    # Get weighting function
    k = 1
    cu = 0.523/np.sqrt(num_looks)
    cmax = np.sqrt(1 + 2/num_looks)
    ci = img_std / img_mean
    w_t = np.zeros(img.shape)
    w_t[ci <= cu] = 1
    idx = np.where((cu < ci) & (ci < cmax))
    w_t[idx] = np.exp((-k * (ci[idx] - cu)) / (cmax - ci[idx]))

    # Apply weighting function
    img_filtered = (img_mean * w_t) + (img * (1 - w_t))

    return img_filtered.astype(img.dtype)