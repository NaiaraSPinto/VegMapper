import os
import numpy as np
import pandas as pd

csv_files = []

f = open("csvFiles.txt", "r")
for file in f.readlines():
    cpS3url = "aws s3 cp s3://servir-public/gedi/peru/" + file.strip() +  " " + file.strip()
    print(cpS3url)
    os.system(cpS3url)
    csv_df =  pd.read_csv(file.strip())
    csv_files.append(csv_df)
    os.remove(file.strip())
df = pd.concat(csv_files)
df.to_csv("filteredShots.csv", index = False)
