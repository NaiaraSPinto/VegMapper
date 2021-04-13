This folder contains code to automate the processing of sentinel granules using the Hyp3 API, the copying of said granules to an s3 bucket, the creation of virtual rasters for each year/path/frame, and temporal average for each year/path/frame.

Create a conda virtual environment with environment.yml to install the dependencies the script needs to run. 

Run using ``` python automation.py <config file> ```.

```<config file>``` should be a .ini file structured like so:

```
[HyP3]
username = <username>
password = <password>

[aws]
aws_access_key_id = <aws_access_key_id>
aws_secret_access_key = <aws_secret_access_key>
dest_bucket = <dest_bucket>
prefix_str = <prefix_str>

[csv]
csv = <csv>

[misc]
max_threads = 4

[switches]
submit_granules = true
copy_processed_granules_to_bucket = true
calculate_temporal_average = true

```
