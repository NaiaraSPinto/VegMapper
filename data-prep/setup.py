import os
import stat
from pathlib import Path
from shutil import copyfile


env = 'data-prep'
if os.getenv('CONDA_DEFAULT_ENV') != env:
    raise Exception(f'Current conda environment is not "{env}", please switch.')
env_bin_path = Path(os.getenv('CONDA_PREFIX')) / 'bin'

with open('source.txt') as f:
    src_scripts = f.read().splitlines()

src_scripts = [Path(src) for src in src_scripts]

for src in src_scripts:
    print(src)
    if not src.exists():
        raise Exception(f'Cannot find {src}!')
    dst = env_bin_path / src.name
    copyfile(src, dst)
    dst.chmod(dst.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
