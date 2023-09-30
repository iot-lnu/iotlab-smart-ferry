#This script saves dataset to file 

import pandas as pd
import os
import yaml
import requests
import time

#Set root to run locally
#if os.getcwd()[0:1]== '/': #You are on linux, eg. at work
#    working_path = '/home/sm/yggio-api-python-examples'
#elif os.getcwd()[0:1]== 'C': #You are on Window, eg. at home
#    working_path = 'C:/Users/sara/Documents/yggio-api-python-examples'
#os.chdir(working_path)

#Own modules
import yggio_API


###########################################################################################
# Decide which data to load
###########################################################################################

#Load config and its parameters
with open("config.yaml", "r") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
server = config["yggio_account"]["server"]
username= config["yggio_account"]["username"]
password = config["yggio_account"]["password"]

#Specify series

# You can check in the "UI: Devices:Some specific device: Data: All" for the _id 
# and which measurements that has availible data in the "UI: Devices: Some specific device: Charts"

# It is possible to get this info through the API by I find it useful to first check if a specific 
# measurement actually contains data

series=[#{"_id": "645ba89bce62983a5d9c4267", "measurement": "vibrationLevel"}, #vibration machine room
        #{"_id": "645902a0d2407e1f6db348b3", "measurement": "temperature"},  #weather station
        #{"_id": "645b989543c7d8da134bce8c", "measurement": "humidityRelative"},   # humidity cafe particle sensor
        #{"_id": "6455008f40e77ffc26553b25", "measurement": "noiseLevelAverage"},   # cafe noise level sensor
       {"_id": "64efb2383c5bd2832951016b", "measurement": "longitude"}, # gps sensor
       {"_id": "64efb2383c5bd2832951016b", "measurement": "latitude"} # gps sensor
        ]

###########################################################################################
# Load data
###########################################################################################

#Authorize
my_headers = yggio_API.authorize(server, username, password)
my_session = requests.Session()
my_session.headers.update(my_headers)

#Decide timeperiod to load data for, below takes all current-availible data
starttime = 0
endtime = int(round(time.time() * 1000))

#Load data and save to separate csv-files
for idx in range(len(series)):
    
    #Load
    df = yggio_API.collectOnePeriodOneNode(series[idx]['_id'], series[idx]['measurement'], starttime, endtime, server, username, password, my_session)
    
    #Rename
    four_last_char_in_id = series[idx]['_id'][len(series[idx]['_id'])-4:len(series[idx]['_id'])]
    df.rename(columns={'value': series[idx]['measurement'] + '_' + four_last_char_in_id}, inplace=True)
    
    #Save to csv
    df.to_csv('data_' + series[idx]['measurement'] + '_' + four_last_char_in_id +'.csv')
