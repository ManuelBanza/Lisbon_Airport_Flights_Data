import string
import requests
import json
import pandas as pd
import numpy as np

# 1. Obter Dados

## 1.1. Arrivals - Chegadas
timeframe = [1,2,3,4,5,6,7,8]

df_arrivals = pd.DataFrame()

for x in timeframe:
    url = "https://www.lisbonairport.eu/ajax/loadcontrol.ashx?cid=3&ts={}".format(x)
    resp = requests.get(url)
    txt = resp.json()
    results = pd.DataFrame(txt['data'])
    # Rename column
    results = results.rename(columns={"a": "Airliner", "b": "Flight", "c": "Origin", "d" : "Status", "e" : "Schedule"})
    results['Type'] = 'Arrival'
    results['Order'] = x
    # Fix schedule columns
    results['firts_2str'] = results['Schedule'].str[:2]
    results['last_str'] = results['Schedule'].str[-3:]
    def schedule_fix(x):
        if x['Order'] == 1  and x['firts_2str'] == '12' : return '00' + x['last_str']
        else: return x['Schedule']

    results['Schedule_Fix'] = results.apply(schedule_fix, axis=1)
    # Delete column / drop column
    results = results.drop(columns=['firts_2str', 'last_str'])
    df_arrivals = df_arrivals.append(results, ignore_index=True)



## 1.2. Departures - Chegadas

df_departures = pd.DataFrame()

for x in timeframe:
    url = "https://www.lisbonairport.eu/ajax/loadcontrol.ashx?cid=4&ts={}".format(x)
    resp = requests.get(url)
    txt = resp.json()
    results = pd.DataFrame(txt['data'])
    # Rename column
    results = results.rename(columns={"a": "Airliner", "b": "Flight", "c": "Destination", "d" : "Status", "e" : "Schedule"})
    results['Type'] = 'Departures'
    results['Order'] = x
    # Fix schedule columns
    results['firts_2str'] = results['Schedule'].str[:2]
    results['last_str'] = results['Schedule'].str[-3:]
    def schedule_fix(x):
        if x['Order'] == 1  and x['firts_2str'] == '12' : return '00' + x['last_str']
        else: return x['Schedule']

    results['Schedule_Fix'] = results.apply(schedule_fix, axis=1)
    # Delete column / drop column
    results = results.drop(columns=['firts_2str', 'last_str'])
    df_departures = df_departures.append(results, ignore_index=True)

# Clean NaNs
df = df_arrivals.append(df_departures, ignore_index=True)
df = df.reset_index()
df['Origin'] = df['Origin'].replace(np.nan, 'Lisbon (LIS)')
df['Destination'] = df['Destination'].replace(np.nan, 'Lisbon (LIS)')
# Delete column / drop column
df = df.drop(columns=['index'])


# Get snapshot date
df['Snapshot'] = pd.to_datetime('today')
df['Snapshot'] = pd.to_datetime(df['Snapshot'], format='%Y-%m-%d')
df["Snapshot"] = df.Snapshot.dt.date


# Get today date now to file name when export to csv or excel with encoding utf8
from datetime import datetime
df.to_csv(datetime.now().strftime('data_sources/data_transformed/Lisbon_Airport_Flights-%Y-%m-%d-%H-%M-%S.csv'), encoding='utf8', index=False)
