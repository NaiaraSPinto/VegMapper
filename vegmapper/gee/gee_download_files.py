import subprocess

from vegmapper import pathurl

# TO DO:
#   1. Handle cloud proj_dir

def download_files(proj_dir:pathurl.ProjDir, task_list):
    for task in task_list:
        status = task.status()
        filename = status['description']
        export_opts = task.config['fileExportOptions']
        gcs_bucket = export_opts['gcsDestination']['bucket']
        gcs_prefix = export_opts['gcsDestination']['filenamePrefix']
        gcs_url = f'gs://{gcs_bucket}/{gcs_prefix}'
        dst_dir = proj_dir / filename.split('_')[0]

        print(f'Downloading {filename}')
        pathurl.copy(gcs_url, dst_dir)
