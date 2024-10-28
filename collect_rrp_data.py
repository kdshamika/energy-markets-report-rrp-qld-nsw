"""
Objective: Retrieve the most recent 24-hour Regional Reference Price (RRP) data from the AEMO website (link: https://nemweb.com.au/Reports/Current/Public_Prices/).

This script carries out the following key steps:

1. Fetch the latest 24-hour data from the specified link.
2. Extract relevant columns (REGIONID, SETTLEMENTDATE, RRP) from the zipped CSV file.
3. Clean the dataset by eliminating duplicates and removing rows with all NaN values.
4. Save the processed data as separate CSV files for the selected regions (QLD and NSW) in a 'processed_data' folder.

The data is obtained from the NEM website and prepared for further use in a Power BI dashboard.

GitHub: https://github.com/kdshamika/energy-markets-report-rrp-qld-nsw

"""

import urllib.request
from datetime import date, timedelta
import re
from io import BytesIO
import pandas as pd
import os
import requests

# 1. Fetch the latest 24-hour data from the specified link.

# recent 24hr date
yesterday = (date.today()- timedelta(days=1))
yesterday_str = yesterday.strftime('%Y%m%d')
print(yesterday)

base_url = "https://nemweb.com.au/Reports/Current/Public_Prices/"
f = urllib.request.urlopen(base_url)
file = f.read()
# print(file)

# decode html content, converting byte format content to string format
html = file.decode('utf-8')
# use regex to create file name
pattern = r"PUBLIC_PRICES_" + yesterday_str + "\d{4}_\d{14}\.zip(?!\")"
filename = re.search(pattern, html).group()
print(filename)
print("---")

#create exact url of the zip file
url = base_url + filename
print(url)
print("---")

# 2. Extract relevant columns (REGIONID, SETTLEMENTDATE, RRP) from the zipped CSV file.

# data columns are inconsistent throughout, with some rows containing 120 columns while others have 130 columns
# get the data in zip file and crete df
r = requests.get(url)
# parse data, select only required columns 
df = pd.read_csv(BytesIO(r.content), compression='zip', delimiter=',', header= 1, on_bad_lines='skip', usecols=["REGIONID", "SETTLEMENTDATE", "RRP"])
print(df.head())
print("---")

print(len(df))
print("---")

# 3. Clean the dataset by eliminating duplicates and removing rows with all NaN values.

# clean data
df.drop_duplicates(inplace=True)
# delete rows which has all values as NaN
df.dropna(axis = 0, how = 'all', inplace = True)
# drop last row
df.drop(index=df.index[-1],axis=0,inplace=True)

print(len(df))
print("---")

# 4. Save the processed data as separate CSV files for the selected regions (QLD and NSW) in a 'processed_data' folder.

# save final csv files       
# select regions 
states=['QLD1', 'NSW1']

# create 'results' folder if it doesn't already exist
os.makedirs('processed_data', exist_ok=True)
# save processed data in csv
for s in states:
    df_= df.loc[df['REGIONID'] == s]
    df_.to_csv(f"processed_data/{s}.csv", sep=',', index=False)
