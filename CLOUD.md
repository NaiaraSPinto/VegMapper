## EC2 Setup

To be edited by Naiara.

## Conda Setup for Multiple Users

To allow multiple users in a group to access conda, install packages, and create environments, do the following:

```
sudo group add mygroup
sudo chgrp -R mygroup <conda_installation_path>
sudo chmod 770 -R <conda_installation_path>
sudo adduser username mygroup
```
