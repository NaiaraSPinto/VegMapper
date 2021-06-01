# VegMapper
Land cover classification using remote sensing observations

## EC2 Set Up ##

To bring up conda:
```
/opt/miniconda3/bin/conda init bash
source ~/.bashrc
```

Now you should see (base) for the base env in front:
```
(base) [username@ip-xxx-xxx-xxx-xxx ~]$
```

Activate the "data-prep" env:
```
(base) [username@ip-xxx-xxx-xxx-xxx ~]$ conda activate data-prep
(data-prep) [username@ip-xxx-xxx-xxx-xxx ~]$
```

Clone VegMapper to your home directory:
```
(data-prep) [username@ip-xxx-xxx-xxx-xxx ~]$ git clone https://github.com/NaiaraSPinto/VegMapper.git
```

## Sentinel-1 ##
### Submit Hyp3 processing jobs and compute temporal average ###
```
(data-prep) [username@ip-xxx-xxx-xxx-xxx ~]$ cd VegMapper/data-prep/Sentinel/automate
```
Edit "config.ini" and upload the granules csv to this directory as needed
```
(data-prep) [username@ip-xxx-xxx-xxx-xxx automate]$ python automation.py config.ini
```

### Remove S1 right/left edges ###
```
(data-prep) [username@ip-xxx-xxx-xxx-xxx automate]$ cd ..
(data-prep) [username@ip-xxx-xxx-xxx-xxx Sentinel]$ python remove_s1_edges.py country state year
```
Supported **(country, state)** are (peru, ucayali) and (brazil, para) now.

## ALOS-2 ##
```
(data-prep) [username@ip-xxx-xxx-xxx-xxx Sentinel]$ cd ../ALOS-2
(data-prep) [username@ip-xxx-xxx-xxx-xxx ALOS-2]$ python download_alos2_mosaic.py country state year
(data-prep) [username@ip-xxx-xxx-xxx-xxx ALOS-2]$ python filter_alos2_mosaic.py country state year
(data-prep) [username@ip-xxx-xxx-xxx-xxx ALOS-2]$ python proc_alos2_tiles.py country state year
```

## Landsat NDVI & MODIS TC (GEE) ##
These don't need to be run on EC2, but they require using a Google Earth Engine account and enough Google Drive space.

For Landsat:
```
python gee_export_landsat.py state year

For MODIS:
```
python gee_export_modis.py state year
```

After all processed tiles are loaded to your Google Drive, download them locally and upload to S3 bucket.
