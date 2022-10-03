## AWS Setup

- AWS services such as EC2 and S3 come with a cost. For price calculation, visit: https://calculator.aws
- For EC2 instance types, go here: https://aws.amazon.com/ec2/instance-types/
- First, navigate to S3 and create a bucket
- Next, navigate to EC2 --> AMI
- Chose Region West-2, CentOS-7, x86_64
- Then click on "launch instance from AMI"
- Select an instance type. Routines have been tested with an i3.xlarge instance with 12 GB storage volume
- Create a key pair and download it; keep this file safe
- Go to instances --> "start instance" and write down the IP address for the server
- Use your key pair and SSH into the server
- Follow these instructions to create account for new users: https://aws.amazon.com/premiumsupport/knowledge-center/new-user-accounts-linux-instance/
- Follow these instructions to append an IAM policy to the EC2 instance so it can read from and write to the S3 bucket:
https://aws.amazon.com/premiumsupport/knowledge-center/ec2-instance-access-s3-bucket/
- Remember to stop the instance by going to instances --> instance state --> stop instance
- It is good practice to provide one key per user; always avoid sharing credentials

## Conda Setup for Multiple Users

To allow multiple users in a group to access conda, install packages, and create environments, do the following:

```
sudo group add <group_name>
sudo chgrp -R <group_name> <conda_installation_path>
sudo chmod 770 -R <conda_installation_path>
sudo adduser <username> <group_name>
```
