#!/usr/bin/env python

import shutil
import subprocess
import sys
from pathlib import Path
from typing import Union
from urllib.parse import urlparse

supported_cloud_storage = ['s3', 'gs']

class PathURL(object):
    def __init__(self, path_or_url: Union[str, Path]):
        path_or_url = str(path_or_url)
        u = urlparse(path_or_url)
        if u.scheme == '':
            # path_or_url is a local path
            self.is_local = True
            self.is_cloud = False
            self.storage = ''
            self.bucket = ''
            self.prefix = ''
            self.path = Path(path_or_url).resolve()
            self.parent = self.path.parent
        elif u.scheme in supported_cloud_storage:
            # proj_dir is a cloud storage url
            self.is_local = False
            self.is_cloud = True
            self.storage = u.scheme
            self.bucket = u.netloc
            self.prefix = u.path.strip('/')
            self.path = f'{self.storage}://{self.bucket}/{self.prefix}'.strip('/')
            self.parent = f"{self.storage}://{self.bucket}/{'/'.join(self.prefix.split('/')[0:-1])}".strip('/')
        else:
            raise Exception(f'{u.scheme} is not a supported cloud storage')

    def __fspath__(self):
        return f'{self.path}'

    def __repr__(self):
        return f'{self.path}'

    def __str__(self):
        return f'{self.path}'

    def __truediv__(self, other):
        if isinstance(other, str):
            return PathURL(f'{self}/{other}')

    def exists(self):
        if self.is_local:
            return self.path.exists()
        else:
            ls_cmd = f'gsutil ls {self}'
            if subprocess.call(ls_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True) == 0:
                return True
            else:
                return False

    def is_dir(self):
        if self.is_local:
            return self.path.is_dir()
        else:
            # Try listing the url
            ls_cmd = f'gsutil ls {self}'
            try:
                ls_output = subprocess.check_output(ls_cmd, shell=True).decode(sys.stdout.encoding).splitlines()
            except:
                # The url matched no objects
                return False

            # Check ls output
            if len(ls_output) == 0:
                # This happens when the url points to an empty bucket
                return True
            elif len(ls_output) == 1:
                # If the url points to a file, ls will return the url exactly
                if ls_output[0] == self.path:
                    return False
                else:
                    return True
            else:
                # There are multiple objects under the url, so it's a directory
                return True

    def is_file(self):
        if self.is_local:
            return self.path.is_file()
        else:
            # Try listing the url
            ls_cmd = f'gsutil ls {self}'
            try:
                ls_output = subprocess.check_output(ls_cmd, shell=True).decode(sys.stdout.encoding).splitlines()
            except:
                # The url matched no objects
                return False

            # Check ls output
            if len(ls_output) == 0:
                # This happens when the url points to an empty bucket
                return False
            elif len(ls_output) == 1:
                # If the url points to a file, ls will return the url exactly
                if ls_output[0] == self.path:
                    return True
                else:
                    return False
            else:
                # There are multiple objects under the url, so it's a directory
                return False


class ProjDir(PathURL):
    def __init__(self, path_or_url):
        super().__init__(path_or_url)
        self.proj_dir = self.path
        if not self.is_cloud:
            if self.proj_dir.exists():
                if not self.proj_dir.is_dir():
                    raise Exception(f'{self.proj_dir} is not a directory path')
            else:
                self.proj_dir.mkdir()
        else:
            subprocess.check_call(
                f'gsutil ls {self.storage}://{self.bucket}',
                stdout=subprocess.DEVNULL,
                shell=True
            )


def copy(src: Union[str, Path, PathURL], dst: Union[str, Path, PathURL], overwrite=False):
    # Convert both src and dst to PathURL
    if isinstance(src, (str, Path)):
        src = PathURL(src)
    if isinstance(dst, (str, Path)):
        dst = PathURL(dst)

    if src.is_local and dst.is_local:
        # Both src and dst are local paths
        if src.is_dir():
            if overwrite:
                shutil.copytree(src.path, dst.path, dirs_exist_ok=True)
            else:
                shutil.copytree(src.path, dst.path, dirs_exist_ok=False)
        else:
            if dst.path.exists() and dst.path.is_file() and not overwrite:
                raise Exception(f'{dst} exists and overwrite is set to False.')
            shutil.copy2(src.path, dst.path)
    else:
        # At least one of src and dst is a cloud storage url (s3:// or gs://)
        if src.is_dir():
            cp_cmd = f'gsutil cp -r {src.path} {dst.path}'
        else:
            cp_cmd = f'gsutil cp {src.path} {dst.path}'

        if dst.exists() and not overwrite:
            raise Exception(f'{dst} exists and overwrite is set to False.')
        else:
            subprocess.call(cp_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)
