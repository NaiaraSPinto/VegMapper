## Prerequisites

### Setup conda environment

1. Download and install conda
    - [Anaconda](https://www.anaconda.com/download/)
    - [Miniconda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/download.html) (slim version of Anaconda, preferred if storage space is limited)
2. Install Git
    - [https://git-scm.com/book/en/v2/Getting-Started-Installing-Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
3. Copy/clone/download VegMapper repository

    To clone:
    ```
    (base)      $ git clone https://github.com/NaiaraSPinto/VegMapper.git
    ```
4. Navigate to VegMapper repository directory
    ```
    (base)      $ cd VegMapper
    ```
5. Create VegMapper environment and install required packages
    ```
    (base)      $ conda env create -f environment.yml
    ```
6. Activate VegMapper environment
    ```
    (base)      $ conda activate vegmapper
    (vegmapper) $
    ```

### Acquire credentials for data
1. NASA Earthdata & ASF Vertax
    - Register an account at [https://urs.earthdata.nasa.gov](https://urs.earthdata.nasa.gov)
    - After registration, sign into [ASF Vertax](https://search.asf.alaska.edu) and sign the data use agreement
2. JAXA EORC
    - Register an account at [https://www.eorc.jaxa.jp/ALOS/en/palsar_fnf/registration.htm](https://www.eorc.jaxa.jp/ALOS/en/palsar_fnf/registration.htm)
3. Google Earth Engine
    - First, you need a Google account
    - To set up GEE Python API:
        ```
        (vegmapper) $ python
        >>> import ee
        >>> ee.Authenticate()
        ```
        A browser window will be prompted.

        Follow the steps to complete the authentication for your Google account, then
        ```
        >>> ee.Initialize()
        ```

### Connect a Jupyter notebook on EC2
1. In a terminal window, connect to EC2 with this command:
    ```
    ssh -L 8889:localhost:8889 EC2_username@EC2_ip_address
    ```
    This connects your local port to the remote port on EC2. You should now see the EC2 prompt as normal.
2. Activate the conda environment you want to use. That is,
    ```
    (base)      $ conda activate vegmapper
    (vegmapper) $
    ```
3. Run the command
    ```
    jupyter lab --no-browser --port=8889
    ```
    The output message will provide an URL like following:
    ```
    Or copy and paste one of these URLs:
        http://localhost:8889/lab?token=your_token
    ```
4. Paste the URL (http://localhost:8889/lab?token=your_token) in your browser and it will open Jupyter Lab, but the notebook is running on EC2.
