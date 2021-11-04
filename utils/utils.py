# imports
import requests
import json
import pandas as pd
import urllib3
import certifi
from urllib3 import request
   
def fetch_chembl_data(starting_url, method='GET', record_path=None):
    """
    
    Method to retrieve data from ChemBL Web Data Services. This takes a starting URL as input and expolit the pagination 
    Meta Data and Pagination feature of ChemBL web api in order to download large data.  
    
    Parameters
    -----------
    
    starting_url:  (Str) Starting url to be used, e.g. /chembl/api/data/activity.json?limit=1000&offset=1&_=18635916
    
    method: str, GET (default) or POST.
        method for starting URL. 
    
    record_path : str or list of str, default None
        Path in each object to list of records. If not passed, data will be
        assumed to be an array of records. 
        
    Returns
    -------
    frame : DataFrame
    Normalize semi-structured JSON data into a flat table.
    
    Examples
    --------
    >>> starting_activity_url = '/chembl/api/data/activity.json?limit=1000&offset=1&_=18635916'
    >>> df_activity = fetch_chembl_data(starting_activity_url, method='GET', record_path='activities')
    >>> df_activity
    
    
    """
    
    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
       
    response = http.request('GET', starting_url)
    data = json.loads(response.data.decode('utf-8'))
    df = pd.json_normalize(data, record_path=record_path)
    url_df = pd.json_normalize(data)
    base = 'https://www.ebi.ac.uk'
    next_url_exist = url_df['page_meta.next'][0]
    print("Starting URL", starting_url)

    while(next_url_exist):
        next_url = base + str(next_url_exist)
        response = http.request('GET', next_url)
        data = json.loads(response.data.decode('utf-8'))
        df_next = pd.json_normalize(data, record_path=record_path)
        next_url_df = pd.json_normalize(data)
        next_url_exist = next_url_df['page_meta.next'][0]
        df = df.append(df_next)
        print("shape of new df: ", df.shape)
        print ("Will use this url next: ", next_url)
    
    return df 
