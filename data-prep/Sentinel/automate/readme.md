This folder contains code to automate the processing of sentinel granules using the Hyp3 API and the copying of said granules to an s3 bucket.

[test_automation.py](test_automation.py) should be run in the same directory as a config.ini file, which should be structured as such:

```
[HyP3]
username = <username>
password = <password>

[s3]
prefix_str = <prefix_str>
dest_bucket = <dest_bucket>

[csv]
csv = <csv>

[misc]
max_threads = <max_threads>

```
