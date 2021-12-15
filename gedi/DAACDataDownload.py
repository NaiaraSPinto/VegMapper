"""
---------------------------------------------------------------------------------------------------
 How to Access the LP DAAC Data Pool with Python
  The following Python code example demonstrates how to configure a connection to download data from
   an Earthdata Login enabled server, specifically the LP DAAC's Data Pool.
   ---------------------------------------------------------------------------------------------------
    Author: Cole Krehbiel
     Last Updated: 05/14/2020
     ---------------------------------------------------------------------------------------------------
     """
# Load necessary packages into Python
from subprocess import Popen
from getpass import getpass
from netrc import netrc
import argparse
import time
import os
import requests

# ----------------------------------USER-DEFINED VARIABLES--------------------------------------- #
# Set up command line arguments
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-dir', '--directory', required=True, help='Specify directory to save files to')
parser.add_argument('-f', '--files', required=True, help='A single granule URL, or the location of csv or textfile containing granule URLs')
args = parser.parse_args()

saveDir = args.directory  # Set local directory to download to
files = args.files        # Define file(s) to download from the LP DAAC Data Pool
prompts = ['Enter NASA Earthdata Login Username \n(or create an account at urs.earthdata.nasa.gov): ',
           'Enter NASA Earthdata Login Password: ']
print("USER-DEFINED VARIABLES")
# ---------------------------------SET UP WORKSPACE---------------------------------------------- #
# Create a list of files to download based on input type of files above
if files.endswith('.txt') or files.endswith('.csv'):
   fileList = open(files, 'r').readlines()  # If input is textfile w file URLs
elif isinstance(files, str):
   fileList = [files]                       # If input is a single file

# Generalize download directory
if saveDir[-1] != '/' and saveDir[-1] != '\\':
       saveDir = saveDir.strip("'").strip('"') + os.sep
       urs = 'urs.earthdata.nasa.gov'    # Address to call for authentication
print("SET UP WORKSPACE")
# --------------------------------AUTHENTICATION CONFIGURATION----------------------------------- #
# Determine if netrc file exists, and if so, if it includes NASA Earthdata Login Credentials
try:
   netrcDir = os.path.expanduser("/data3/GEDI_H5/.netrc")
   netrc(netrcDir).authenticators(urs)[0]
   print("FILE EXISTS")

# Below, create a netrc file and prompt user for NASA Earthdata Login Username and Password
except FileNotFoundError:
   print("FILE NOT FOUND")
   homeDir = os.path.expanduser("~")
   Popen('touch {0}.netrc | chmod og-rw {0}.netrc | echo machine {1} >> {0}.netrc'.format(homeDir + os.sep, urs), shell=True)
   Popen('echo login {} >> {}.netrc'.format(getpass(prompt=prompts[0]), homeDir + os.sep), shell=True)
   Popen('echo password {} >> {}.netrc'.format(getpass(prompt=prompts[1]), homeDir + os.sep), shell=True)

# Determine OS and edit netrc file if it exists but is not set up for NASA Earthdata Login
except TypeError:
   print("INVALID NETRC")
   homeDir = os.path.expanduser("~")
   Popen('echo machine {1} >> {0}.netrc'.format(homeDir + os.sep, urs), shell=True)
   Popen('echo login {} >> {}.netrc'.format(getpass(prompt=prompts[0]), homeDir + os.sep), shell=True)
   Popen('echo password {} >> {}.netrc'.format(getpass(prompt=prompts[1]), homeDir + os.sep), shell=True)

# Delay for up to 1 minute to allow user to submit username and password before continuing
tries = 0
print("ABOUT TO TRY")
print(netrc(netrcDir).authenticators(urs)[1])
print(netrc(netrcDir).authenticators(urs)[2])
#while tries < 30:
 #  try:
  #    netrc(netrcDir).authenticators(urs)[2]
  # except:
  #    time.sleep(2.0)
  #    print(netrcDir)
  #    print(tries)
  #    tries += 1
print("AUTHENTICATED")
# -----------------------------------------DOWNLOAD FILE(S)-------------------------------------- #
# Loop through and download all files to the directory specified above, and keeping same filenames
for f in fileList:
   print("F: " + f)
   if not os.path.exists(saveDir):
      os.makedirs(saveDir)
      saveName = os.path.join(saveDir, f.split('/')[-1].strip())
      print("FILENAME: " + saveName)
   # Create and submit request and download file
   with requests.get(f.strip(), verify=False, stream=True, auth=("isabella.rahs@gmail.com", "v9j^8VMml34")) as response:
      print("OPENED REQUEST")
      if response.status_code != 200:
         print(response.status_code)
         print("{} not downloaded. Verify that your username and password are correct in {}".format(f.split('/')[-1].strip(), netrcDir))
      else:
         print("RESPONSE 200")
         response.raw.decode_content = True
         content = response.raw
         with open(saveName, 'wb') as d:
            print("OPENED FILE")
            while True:
               chunk = content.read(16 * 1024)
               if not chunk:
                  break
               d.write(chunk)
         print('Downloaded file: {}'.format(saveName))
