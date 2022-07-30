# Setup

## EC2 setup

## Installing softwares

- [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

- [Miniconda](https://docs.conda.io/en/latest/miniconda.html) (select the latest Python version)

## Conda setup

- To allow multiple users in a group to access conda, install packages, and create environments, do the following:
    ```
    sudo group add mygroup
    sudo chgrp -R mygroup <conda_installation_path>
    sudo chmod 770 -R <conda_installation_path>
    sudo adduser username mygroup
    ```

- Install [jupyterlab](https://jupyterlab.readthedocs.io/en/stable/), [mamba](https://github.com/mamba-org/mamba), and [kernda](https://github.com/vericast/kernda) into the base environment by running:
    ```
    conda install -n base -c conda-forge jupyterlab mamba kernda
    ```

## Running JupyterLab on a remote server (EC2)

1. From local machine, run the following command:
    ```
    ssh -L 8080:localhost:8080 <EC2_username>@<EC2_address>
    ```
    to connect to EC2 with the port forwarding.

2. After logging into EC2, run the following command:
    ```
    jupyter lab --no-browser --port=8080
    ```
    to launch JupyterLab.

3. After launching JupyterLab, in the output message it will provide URLs for accessing the Jupyter server. Copy and paste one of them (for example, http://localhost:8080/lab?token=your_token) in your browser to open the Jupyter Lab web interface.
