#!/usr/bin/env python3

"""This script contains functions for retrieving data from various sources, including:
- Inference
- Dataloggers
- Weather stations
- Metadata files
"""
import os
import numpy as np
import pandas as pd
import datetime as dt
import fnmatch
import chorus_qc_data as qdata
import chorus_utils as chutils

def get_inference(folder_path, location_id, date_ini, date_fin,raw=False):
    """Function to obtain inferences from the inference files of the machine learning models.
    
    Args:
        folder_path (str): path to the folder containing the inference files.
        location_id (str): location identifier.
        date_ini (str): start date in YYYY-MM-DD format.
        date_fin (str): end date in YYYY-MM-DD format.
        raw (boolean): flag that indicates if you want to obtain the raw data.
        
    Returns:
       df_sel (pandas DataFrame): DataFrame that contains the inferences on the requested dates.
       df_raw (pandas DataFrame): DataFrame that contains the raw data (if required).
    """
    
    dis = date_ini.split('-')
    dfs = date_fin.split('-')
    
    try:
        date_ini_dt = dt.datetime(int(dis[0]), int(dis[1]), int(dis[2]))
    except:
        print('Wrong Start Date')
        df = pd.DataFrame()
        if (raw):
            return df, df
        else:
            return df
    
    try:
        date_fin_dt = dt.datetime(int(dfs[0]), int(dfs[1]), int(dfs[2]))
    except:
        print('Wrong End Date')
        df = pd.DataFrame()
        if (raw):
            return df, df
        else:
            return df
    
    ##find files
    pattern = location_id+'_inference'+'*.gzip'
    find_files = []
    for dirpath, dirs, files in os.walk(folder_path):
        for filename in fnmatch.filter(files, pattern):
            find_files.append(os.path.join(dirpath, filename))
    
    ##open files and load them into a dataframe
    if len(find_files) == 0:
        print('No records found.')
        df = pd.DataFrame()
        if (raw):
            return df, df
        else:
            return df
    else:
        df = []
        for i in range(len(find_files)):
            file_path = find_files[i]
            df.append(pd.read_parquet(file_path))
                
        df_raw = pd.DataFrame()
        for i in range(len(df)):
            df_proc = df[i].copy()
            df_raw = pd.concat([df_raw,df_proc])
        
        df_raw = df_raw.sort_values(['date','min'])
        df_raw = df_raw.reset_index(drop=True)
        
        ##copy enabled columns and set data types
        inference_labels = chutils.get_inference_labels()
        df_sel_cols = pd.DataFrame()
    
        raw_col_names = df_raw.columns
    
        for label in inference_labels:
            if (label.enable and label.ori_name in raw_col_names):
                df_sel_cols[label.new_name] = df_raw[label.ori_name].values
                if (label.new_name == 'time'):
                    df_sel_cols['time'] = df_sel_cols['date'].dt.time
                else:
                    df_sel_cols[label.new_name] = df_sel_cols[label.new_name].astype(label.new_dtype)
    
        ##select data according to date                
        df_sel_cols = df_sel_cols.sort_values(['date','min'])
        df_sel_cols = df_sel_cols.reset_index(drop=True)
        
        di = df_sel_cols['date'][0]
        df_date_ini = dt.datetime(di.year, di.month, di.day)
        
        df = df_sel_cols['date'][len(df_sel_cols['date'])-1]
        df_date_fin = dt.datetime(df.year, df.month, df.day)
        
        if (df_date_ini <= date_ini_dt):
            print('There are records SINCE the requested date.')
        else:
            date_ini_dt = di
            print('There are not records SINCE the requested date.')
            print('There are only records SINCE : ' + date_ini_dt.strftime("%Y-%m-%d"))
        
        if (df_date_fin >= date_fin_dt):
            print('There are records UP TO the requested date.')
        else:
            date_fin_dt = df_date_fin
            print('There are not records UP TO the requested date.')
            print('There are only records UP TO : '+ date_fin_dt.strftime("%Y-%m-%d"))
        
        df_sel = df_sel_cols.loc[(df_sel_cols['date'] >= date_ini_dt) & (df_sel_cols['date'] <= date_fin_dt)]
    
        df_sel = df_sel.sort_values(['date','min'])
        df_sel = df_sel.reset_index(drop=True)

        ##basic quality test
        qdata.evaluate_nulls(df_sel)
        qdata.evaluate_duplicates(df_sel)
        
        if (raw):
            return df_sel, df_raw
        else:
            return df_sel

def get_datalogger(folder_path, location_id, date_ini, date_fin,raw=False):
    """Function to obtain the climatic variables from the datalogger files.
    
    Args:
        folder_path (str): path to the folder containing the datalogger files.
        location_id (str): location identifier.
        date_ini (str): start date in YYYY-MM-DD format.
        date_fin (str): end date in YYYY-MM-DD format.
        raw (boolean): flag that indicates if you want to obtain the raw data.
        
    Returns:
        df_new (pandas DataFrame): DataFrame that contains the climatic variables of the datalogger on the requested dates.
        df_raw (pandas DataFrame): DataFrame that contains the raw data (if required).
    """

    dis = date_ini.split('-')
    dfs = date_fin.split('-')
    
    dis = date_ini.split('-')
    dfs = date_fin.split('-')
    
    try:
        date_ini_dt = dt.datetime(int(dis[0]), int(dis[1]), int(dis[2]))
    except:
        print('Wrong Start Date')
        df = pd.DataFrame()
        #return df
    
    try:
        date_fin_dt = dt.datetime(int(dfs[0]), int(dfs[1]), int(dfs[2]))
    except:
        print('Wrong End Date')
        df = pd.DataFrame()
        #return df
    
    ##find files
    pattern = location_id + '_datalogger'+'*.xlsx'
    find_files = []
    for dirpath, dirs, files in os.walk(folder_path):
        for filename in fnmatch.filter(files, pattern):
            find_files.append(os.path.join(dirpath, filename))
    
    ##copy enabled columns and set data types
    if len(find_files) == 0:
        print('No records found.')
    else:
        df_raw = pd.DataFrame()
        list_df = []
        for i in range(len(find_files)):
            file_path = find_files[i]
            df_proc = pd.read_excel(file_path,engine='openpyxl',index_col=False)
            df_proc.columns = df_proc.loc[2].values
            df_proc = df_proc.drop(labels=range(0,3), axis=0)
            df_proc = df_proc.reset_index(drop=True)
            df_raw = pd.concat([df_raw,df_proc])
        df_raw = df_raw.reset_index(drop=True)
    
        datalogger_labels = chutils.get_datalogger_labels()
        df_sel_cols = pd.DataFrame()
    
        raw_col_names = df_raw.columns
    
        for label in datalogger_labels:
            if (label.enable and label.ori_name in raw_col_names):
                df_sel_cols[label.new_name] = df_raw[label.ori_name].values
                if (label.new_name != 'time'):
                    df_sel_cols[label.new_name] = df_sel_cols[label.new_name].astype(label.new_dtype)
                   
         #select data according to date                
        df_sel_cols = df_sel_cols.sort_values(by='date')
        df_sel_cols = df_sel_cols.reset_index(drop=True)
    
        di = df_sel_cols['date'][0]
        df_date_ini = dt.datetime(di.year, di.month, di.day)
        
        df = df_sel_cols['date'][len(df_sel_cols['date'])-1]
        df_date_fin = dt.datetime(df.year, df.month, df.day)
        
        if (df_date_ini <= date_ini_dt):
            print('There are records SINCE the requested date.')
        else:
            date_ini_dt = di
            print('There are not records SINCE the requested date.')
            print('There are only records SINCE : ' + date_ini_dt.strftime("%Y-%m-%d"))
        
        if (df_date_fin >= date_fin_dt):
            print('There are records UP TO the requested date.')
        else:
            date_fin_dt = df_date_fin
            print('There are not records UP TO the requested date.')
            print('There are only records UP TO : '+ date_fin_dt.strftime("%Y-%m-%d"))
        
        df_sel = df_sel_cols.loc[(df_sel_cols['date'] >= date_ini_dt) & (df_sel_cols['date'] <= date_fin_dt)]
        df_sel = df_sel.reset_index(drop=True)

        ##basic quality test
        qdata.evaluate_nulls(df_sel)
        qdata.evaluate_duplicates(df_sel)
        
        if (raw):
            return df_sel, df_raw
        else:
            return df_sel
               
def get_wstation(folder_path, location_id, date_ini, date_fin, raw=False):
    """Function to obtain climatic variables from the wheater station files.
    
    Args:
        folder_path (str): path to the folder containing the weather station files.
        location_id (str): location identifier.
        date_ini (str): start date in YYYY-MM-DD format.
        date_fin (str): end date in YYYY-MM-DD format.
        raw (boolean): flag that indicates if you want to obtain the raw data.

    Returns:
       df_new (pandas DataFrame): DataFrame that contains the climatic variables of the weather station on the requested dates.
        df_raw (pandas DataFrame): DataFrame that contains the raw data (if required).
    """

    dis = date_ini.split('-')
    dfs = date_fin.split('-')
    
    try:
        date_ini_dt = dt.datetime(int(dis[0]), int(dis[1]), int(dis[2]))
    except:
        print('Wrong Start Date')
        df = pd.DataFrame()
        #return df
    
    try:
        date_fin_dt = dt.datetime(int(dfs[0]), int(dfs[1]), int(dfs[2]))
    except:
        print('Wrong End Date')
        df = pd.DataFrame()
        #return df
    
    #find files
    pattern = location_id + '_wstation'+'*.xlsx'
    find_files = []
    for dirpath, dirs, files in os.walk(folder_path):
        for filename in fnmatch.filter(files, pattern):
            find_files.append(os.path.join(dirpath, filename))
    
    if len(find_files) == 0:
        print('No records found.')
    else:
        df_raw = pd.DataFrame()
        for i in range(len(find_files)):
            file_path = find_files[i]
            df_proc = pd.read_excel(file_path,engine='openpyxl',index_col=False)
            df_raw = pd.concat([df_raw,df_proc])
    
        #copy enabled columns and set data types
        wstation_labels = chutils.get_wstation_labels()
        df_sel_cols = pd.DataFrame()

        raw_col_names = df_raw.columns
        
        for label in wstation_labels:
                if (label.enable and label.ori_name in raw_col_names):
                    df_sel_cols[label.new_name] = df_raw[label.ori_name].values
                    if (label.new_name != 'time'):
                        df_sel_cols[label.new_name] = df_sel_cols[label.new_name].replace('-',np.nan)
                        df_sel_cols[label.new_name] = df_sel_cols[label.new_name].astype(label.new_dtype)
    
        #Preproccessing: converting UTC Hour to Local Hour                    
        df_tot = df_sel_cols.copy()
        dif_utc_hour = -3 
        date_ini = df_tot['date'].dt.date.values[0]
        time = []
        corr = [21,22,23]
        for i in range(len(df_tot)):
            aux = int((df_tot['time'].iloc[i]/100)+dif_utc_hour)
            if (aux < 0):
                d = date_ini - dt.timedelta(days=1)
                aux = corr[aux]
            aux = dt.time(aux)
            time.append(aux)
    
        dif_utc_hour = -3
        delta = abs(dif_utc_hour)
        date = []
        date_ini = df_tot['date'].dt.date.values[0]
        for i in range(delta):
            aux = int((df_tot['time'].iloc[i]/100)+dif_utc_hour)
            if (aux < 0):
                d = date_ini - dt.timedelta(days=1)
            else:
                d = date_ini
            date.append(d)
            
        for i in range(len(df_tot)-delta):
            d = df_tot['date'].dt.date.values[i]
            date.append(d)
    
        df_tot['date'] = date
        df_tot['date'] = pd.to_datetime(df_tot['date'])
        df_tot['time'] = time
    
        ##select data according to date
        df_sel_cols = pd.DataFrame()
        df_sel_cols = df_tot.copy()
        df_sel_cols = df_sel_cols.sort_values(by='date')
        df_sel_cols = df_sel_cols.reset_index(drop=True)
        
        di = df_sel_cols['date'][0]
        df_date_ini = dt.datetime(di.year, di.month, di.day)
        
        df = df_sel_cols['date'][len(df_sel_cols['date'])-1]
        df_date_fin = dt.datetime(df.year, df.month, df.day)
        
        if (df_date_ini <= date_ini_dt):
            print('There are records SINCE the requested date.')
        else:
            date_ini_dt = di
            print('There are not records SINCE the requested date.')
            print('There are only records SINCE : ' + date_ini_dt.strftime("%Y-%m-%d"))
        
        if (df_date_fin >= date_fin_dt):
            print('There are records UP TO the requested date.')
        else:
            date_fin_dt = df_date_fin
            print('There are not records UP TO the requested date.')
            print('There are only records UP TO : '+ date_fin_dt.strftime("%Y-%m-%d"))
        
        df_sel = df_sel_cols.loc[(df_sel_cols['date'] >= date_ini_dt) & (df_sel_cols['date'] <= date_fin_dt)]
        df_sel = df_sel.reset_index(drop=True)

        ##basic quality test
        qdata.evaluate_nulls(df_sel)
        qdata.evaluate_duplicates(df_sel)
        
        if (raw):
            return df_sel, df_raw
        else:
            return df_sel


def get_metadata(folder_path, file_name, location_id):
    """Function to obtain basic metadata of the location from a metadata file.
    
    Args:
        folder_path (str): path to the folder containing the metadata file.
        file_name (str): name of the metadata file.
        
    Returns:
       df_sel (pandas DataFrame): DataFrame that contains the metadata information.
    """
    
    find_files = []
    for dirpath, dirs, files in os.walk(folder_path):
        for filename in fnmatch.filter(files, file_name):
            find_files.append(os.path.join(dirpath, filename))

    if len(find_files) == 0:
        print('No file found.')
        df_sel = pd.DataFrame()
    else:

        df = []
        for i in range(len(find_files)):
            file_path = find_files[i]
            df.append(pd.read_excel(file_path,engine='openpyxl',index_col=False))

        df_raw = pd.DataFrame()
        for i in range(len(df)):
            df_proc = df[i].copy()
            df_raw = pd.concat([df_raw,df_proc])

        locations_labels = chutils.get_locations_labels()
        df_sel_cols = pd.DataFrame()

        for label in locations_labels:
            if label.enable:
                df_sel_cols[label.new_name] = df_proc[label.ori_name].values

        df_sel = df_sel_cols.loc[df_sel_cols['location_ID']==location_id]
        df_sel = df_sel.reset_index(drop=True)
    
    return df_sel