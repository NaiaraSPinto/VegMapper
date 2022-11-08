import json

from vegmapper import pathurl
from vegmapper.pathurl import PathURL

# TO DO:
#   1. Handle cloud data_dir

def download_files(data_dir, tasks):
    data_dir = PathURL(data_dir)

    if isinstance(tasks, list):
        for task in tasks:
            filename = task.config['description']
            export_opts = task.config['fileExportOptions']
            if 'gcsDestination' in export_opts.keys():
                dst_key = 'gcsDestination'
            elif 'cloudStorageDestination' in export_opts.keys():
                dst_key = 'cloudStorageDestination'
            gcs_bucket = export_opts[dst_key]['bucket']
            gcs_prefix = export_opts[dst_key]['filenamePrefix']
            gcs_url = f'gs://{gcs_bucket}/{gcs_prefix}.tif'
            dst_dir = data_dir
            print(f'Downloading {filename}')
            pathurl.copy(gcs_url, dst_dir)
    else:
        with open(tasks) as f:
            export_dst = json.load(f)
        for filename, export_opts in export_dst.items():
            if 'gcsDestination' in export_opts.keys():
                dst_key = 'gcsDestination'
            elif 'cloudStorageDestination' in export_opts.keys():
                dst_key = 'cloudStorageDestination'
            gcs_bucket = export_opts[dst_key]['bucket']
            gcs_prefix = export_opts[dst_key]['filenamePrefix']
            gcs_url = f'gs://{gcs_bucket}/{gcs_prefix}.tif'
            dst_dir = data_dir
            print(f'Downloading {filename}')
            pathurl.copy(gcs_url, dst_dir, overwrite=True)
