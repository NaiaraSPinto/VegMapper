# VegMapper
Land cover classification using remote sensing observations

## EC2 Set Up ##

To bring up conda:
```
/opt/miniconda3/bin/conda init bash
source ~/.bashrc
```

Now you should see (base) for the base env in front:
```
(base) [username@ip-xxx-xxx-xxx-xxx ~]$
```

Activate the "data-prep" env:
```
(base) [username@ip-xxx-xxx-xxx-xxx ~]$ conda activate data-prep
(data-prep) [username@ip-xxx-xxx-xxx-xxx ~]$
```

Clone VegMapper to your home directory:
```
(data-prep) [username@ip-xxx-xxx-xxx-xxx ~]$ git clone https://github.com/NaiaraSPinto/VegMapper.git
```
