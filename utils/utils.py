# imports
import requests
import json
import pandas as pd
import urllib3
import certifi
import datetime
from urllib3 import request
   
def fetch_chembl_data(starting_url, compound_ids=None, method='GET', record_path=None):
    """
    
    Method to retrieve ChemBL activity data from ChemBL Web Data Services for a given list of molecule_chembl_id. 
    This takes a starting URL as input and expolit the Meta Data and Pagination feature of ChemBL web api in order 
    to download large data.  
    
    Parameters
    -----------
    
    starting_url:  (Str) Starting url to be used, e.g. https://www.ebi.ac.uk/chembl/api/data/activity.json?limit=1000&offset=1&_=18635916
    
    compound_id: (pandas series) pass a list of molecule chembl IDs for which the records need to be retreived.
    Default None, all the records will be retreived. 
    
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
    starting_time = datetime.datetime.now()
    #print("Starting time:{} ".format(str(starting_time)))
    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
       
    response = http.request('GET', starting_url)
    data = json.loads(response.data.decode('utf-8'))
    df = pd.json_normalize(data, record_path=record_path)
    
    #--------------------------------------
    # if list of compound ids are provided, then select rows with those compound ids 
    #--------------------------------------
    #print(len(compound_ids))
    if(compound_ids is not None):
        df_initial_length = len(df)
        df = df[df['molecule_chembl_id'].isin(compound_ids)] ## select only rows with the compound_id
        df_length_after = len(df)
        
        ##------------------------------------
        # Check how many rows are selected
        #--------------------------------------
        if(df_initial_length == df_length_after):
            print("None found!")
        else:
            print("{} rows selected out of {}".format(df_length_after, df_initial_length))
        
    url_df = pd.json_normalize(data)
    base = 'https://www.ebi.ac.uk'
    next_url_exist = url_df['page_meta.next'][0]
    print("Starting URL", starting_url)
    
    
    end_time = datetime.datetime.now()
    print("Last fetch took : {} seconds".format(end_time-starting_time))
    
    while(next_url_exist):
        starting_time = datetime.datetime.now()
        next_url = base + str(next_url_exist)
        response = http.request('GET', next_url)
        data = json.loads(response.data.decode('utf-8'))
        df_next = pd.json_normalize(data, record_path=record_path)
        
        #----------------------------------------
        # if list of compound ids are provided, then select rows with those compound ids 
        #----------------------------------------
        if(compound_ids is not None):
            df_next_initial_length = len(df_next)
            df_next = df_next[df_next['molecule_chembl_id'].isin(compound_ids)] ## select only rows with the compound_id
            df_next_length_after = len(df_next)
        
            
            ##------------------------------------
            # Check how many rows are selected
            #--------------------------------------
            
            if(df_next_initial_length == df_next_length_after):
                print("None found!")
            else:
                print("{} rows selected out of {}".format(df_next_length_after, 
                                                      df_next_initial_length))
        
        next_url_df = pd.json_normalize(data)
        next_url_exist = next_url_df['page_meta.next'][0]
        df = df.append(df_next)
        print("shape of new df: ", df.shape)
        print ("Will use this url next: ", next_url)
        end_time = datetime.datetime.now()
        print("Last fetch took : {} seconds".format(end_time-starting_time))
        # display df, for debug purpose
        #print(df['molecule_chembl_id'])
    
    return df
