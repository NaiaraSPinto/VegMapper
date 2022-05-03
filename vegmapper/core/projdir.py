#!/usr/bin/env python

import subprocess
from pathlib import Path
from urllib.parse import urlparse

supported_cloud_storage = ['s3', 'gs']

class ProjDir(object):
    def __init__(self, proj_dir):
        u = urlparse(proj_dir)
        if u.scheme == '':
            # proj_dir is a local path
            self.is_cloud = False
            self.proj_dir = Path(proj_dir).resolve()
            if self.proj_dir.exists():
                if not self.proj_dir.is_dir():
                    raise Exception(f'{self.proj_dir} is not a directory path')
            else:
                self.proj_dir.mkdir()
        elif u.scheme in supported_cloud_storage:
            # proj_dir is a cloud storage url
            self.is_cloud = True
            self.storage = u.scheme
            self.bucket = u.netloc
            self.prefix = u.path.strip('/')
            self.proj_dir = f'{self.storage}://{self.bucket}/{self.prefix}'
            subprocess.check_call(
                f'gsutil ls {self.storage}://{self.bucket}',
                stdout=subprocess.DEVNULL,
                shell=True
            )
        else:
            raise Exception(f'{u.scheme} is not a supported cloud storage')