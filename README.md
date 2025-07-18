# VegMapper

Land cover classification using remote sensing observations.

## Prerequisites

If you own a cloud environment, follow steps 1-5 below. If you are using OpenScienceLab, go straight to step 4.

The VegMapper software is intended to be used on a cloud-computing platform (e.g., [AWS EC2](https://aws.amazon.com/ec2/)) as the volume of remote sensing data is very large normally. However, VegMapper can still be used on a local machine but only Linux and macOS platforms have been tested.

## 1. Obtain an AWS account and EC2 instance

Follow steps on [CLOUD.md](./CLOUD.md).

## 2. Install Git and Conda

- [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) (for cloning this repository to the machine used)

- [Miniconda](https://docs.conda.io/en/latest/miniconda.html) (for installing other required Python packages)

After Miniconda is istalled, install [jupyterlab](https://jupyterlab.readthedocs.io/en/stable/), [mamba](https://github.com/mamba-org/mamba), and [kernda](https://github.com/vericast/kernda) into the base environment by running:

`````
conda install -n base -c conda-forge jupyterlab mamba kernda
`````

## 3. Running JupyterLab on a remote server (EC2)

- From local machine, run the following command:
    ```
    ssh -L 8080:localhost:8080 <EC2_username>@<EC2_address>
    ```
    to connect to EC2 with the port forwarding.

- After logging into EC2, run the following command:
    ```
    jupyter lab --no-browser --port=8080
    ```
    to launch JupyterLab.

- After launching JupyterLab, in the output message it will provide URLs for accessing the Jupyter server. Copy and paste one of them (for example, http://localhost:8080/lab?token=your_token) in your browser to open the Jupyter Lab web interface.

## 4. Clone VegMapper Repository

Open a terminal, navigate to where you want the repository to be cloned to, and do
```
git clone https://github.com/NaiaraSPinto/VegMapper.git
```

## 5. Installation

To create a conda environment and install VegMapper software, navigate to the VegMapper folder and run this Jypyter notebook: [INSTALL.ipynb](./INSTALL.ipynb). This notebook also includes instructions for obtaining credentials to download imagery from NASA and JAXA.
