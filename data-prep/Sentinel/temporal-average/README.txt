Richard's Code
cal_temp_ave.py -- calculating temporal average
proc_ami_filter.py -- using AMI_FILTER_GEO of GAMMA software to perform temporal filter (GAMMA software required)
cal_ave_std.py -- calculating temporal average and standard deviation of filtered images
make_vrt.py -- example of the commend I used to generate VRT
make_rgb.py -- example of the commend I used to generate RGB false color composite
As an example, I put all 30 Sentinel-1 Hyp3 images for path-frame:171-617 of 2020 in the folder named "171-617". Then call make_vrt.py and cal_temp_ave.py to generate temporal average raster.
build_vrt.py -- builds a vrt of all the files in a specified aws bucket
