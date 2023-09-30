"""
 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0. If a copy of the MPL was not distributed with this
 file, You can obtain one at https://mozilla.org/MPL/2.0/.
"""

import pandas as pd
import requests
import time


def api_get(call, server, username, password, my_session):
    while True:
        response = my_session.get(call)
        if response.status_code == 200:
            return response
        elif response.status_code == 401:
            my_headers = authorize(server, username, password)
            my_session.headers.update(my_headers)
            print("Updated headers!")
        else:
            print("No response: ", response)
            time.sleep(60)
 
    
def collectOnePeriodOneNode(nodeId, measurement, starttime, endtime, server, username, password, my_session):
    
    
    # Initial call 
    response = api_get(server + '/iotnodes/' + nodeId + '/stats?measurement=' + measurement +
    '&end=' + str(endtime), server, username, password, my_session)   
    jsonResponse = response.json()
    df = pd.json_normalize(jsonResponse)
    startTimeLastCall = pd.to_datetime(min(df.time)) 
    endTimeLastCall = pd.to_datetime(max(df.time)) 
    startTimeLastCallUnix = int(startTimeLastCall.timestamp()*1000)
    
    
    maxIter = 100
    maxCounter = 0
    while (startTimeLastCallUnix > starttime) and (maxCounter < maxIter):
        response = api_get(server + '/iotnodes/' + nodeId + '/stats?measurement=' + measurement +
        '&end=' + str(startTimeLastCallUnix), server, username, password, my_session)  
        jsonResponse = response.json()
        if jsonResponse:        # Catches if return is succesfull but an empty list
            dfLastCall = pd.json_normalize(jsonResponse)
            startTimeLastCall = pd.to_datetime(min(dfLastCall.time)) 
            endTimeLastCall = pd.to_datetime(max(dfLastCall.time)) 
            startTimeLastCallUnix = int(startTimeLastCall.timestamp()*1000)
            df = pd.concat([df, dfLastCall])                  
        else:
            break
        maxCounter = maxCounter+1
    
    # Clean    
    df.drop_duplicates(inplace=True)
    print(df)
    df.time = pd.to_datetime(df.time, format='mixed')
    df.set_index('time', inplace=True)
    
    # Removes timepoints outside requested period
    df = df[df.index >= pd.to_datetime(starttime, origin='unix', unit='ms', utc=True)]
    df = df[df.index <= pd.to_datetime(endtime, origin='unix', unit='ms', utc=True)]
    
    return df


def authorize(server, username, password): 
    
    try:
        response = requests.post(server +'/auth/local', json={"username": username,"password": password})
        authorization = response.json()
        token = authorization['token']
        headers = {'Authorization' : 'Bearer ' + token + ''}
    except Exception:
        print("Cannot authorize")
        headers = {'Authorization' : 'Bearer '+'***no token since cannot authorize***'+''}
                
    return headers
    
   
def get_all_node_ids(server, username, password, my_session):
    
    try:
        response = api_get(server +'/iotnodes', server, username, password, my_session) 
        jsonResponse = response.json()
        df = pd.json_normalize(jsonResponse)
    except Exception:
        print("Cannot get list of IDs")
        df = pd.DataFrame()
 
    return df
