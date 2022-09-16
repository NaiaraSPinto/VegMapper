# VegMapper

Land cover classification using remote sensing observations.

## Prerequisites

The VegMapper software is intended to be used on a cloud-computing platform (e.g., [AWS EC2](https://aws.amazon.com/ec2/)) as the volume of remote sensing data is very large normally. However, VegMapper can still be used on a local machine but only Linux and macOS platforms have been tested. To get started, the following softwares need to be first installed:

- [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) (for cloning this repository to the machine used)

- [Miniconda](https://docs.conda.io/en/latest/miniconda.html) (for installing other required software packages)

After Miniconda is istalled, install [jupyterlab](https://jupyterlab.readthedocs.io/en/stable/), [mamba](https://github.com/mamba-org/mamba), and [kernda](https://github.com/vericast/kernda) into the base environment by running:

```
conda install -n base -c conda-forge jupyterlab mamba kernda
```

For system admin of a shared cloud-computing platform, see [Setup.md](./Setup.md) for setting up the system to be used by multiple users.

## Installation

2. [Prerequisites.ipynb](./Prerequisites.ipynb)
