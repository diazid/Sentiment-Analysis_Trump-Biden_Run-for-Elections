import pandas as pd
import numpy as np
import json
import os
import glob
from datetime import datetime
import time

## defining functions
def now(folder='data/reshaped_data'):
    now = datetime.now()
    date_string = now.strftime("%Y%m%d-%H%M%S")
    folder = folder
    filename = f'{folder}/chunck_tweets_{date_string}.json'
    return filename

def preprocess_json(file, destination_folder):
    #open File
    with open(file, 'r') as f:
        text = f.readlines()
    
    # processing
    rebuild_json(text, file_name=now(folder=destination_folder))
    print(file, 'Processed')

def rebuild_json(text, file_name):
    for t in text:
        res = json.loads(t)
        ##build element
        element = {'created_at': res['data']['created_at'], 
                   'id': res['data']['id'], 
                   'retweet_count': res['data']['public_metrics']['retweet_count'], 
                   'like_count': res['data']['public_metrics']['like_count'], 
                   'quote_count': res['data']['public_metrics']['quote_count'], 
                   'impression_count': res['data']['public_metrics']['impression_count'], 
                   'text': res['data']['text']}
        
        ## appending to dictionary
        with open(file_name, 'a') as f:
                json.dump(element, f)
                f.write('\n')

## Using glob to open the json files
FOLDER = 'data/reshaped_data/'
q = FOLDER + "*.json"
print(q)

reshaped_files = sorted(glob.glob(q, recursive=True))
reshaped_files

## Run function
for file in reshaped_files:
    try:
        preprocess_json(file, destination_folder='data/processed')
        time.sleep(2)
    except:
        print(f'Null condition found at file {file}' )
        time.sleet(2)