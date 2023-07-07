#!/usr/bin/env python3

"""This script contains functions to create and read EBV-ready datasets in the Apache Parquet format.
"""

import os
import numpy as np
import pandas as pd
import datetime as dt
import fnmatch
import chorus_qc_data as qdata
import chorus_utils as chutils

def ebv_rd_create(df_inf,df_dlog,folder_path,ebv_rd_name, file_name, location_id):
    """
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
            df.append(pd.read_csv(file_path))
    
        df_raw = pd.DataFrame()
        for i in range(len(df)):
            df_proc = df[i].copy()
            df_raw = pd.concat([df_raw,df_proc])

        df_raw['site'] = df_raw['site'].replace(['INCT20'], 'INCT20955')

        df = df_raw[df_raw.site==location_id]
        df = df[df.EBV_ready_dataset==1]

        ##create dataframe to store the EBV-ready dataset
        df_ebv_rd = pd.DataFrame()

        df_ebv_rd['time'] = df_inf.time
        df_ebv_rd['date'] = df_inf.date
        df_ebv_rd['hour'] = df_inf.hour

        for specie in df.Species:
            df_ebv_rd[specie] = df_inf[specie]

        climatic_variables = list(df_dlog.columns[3:])

        for clim_var in climatic_variables:
            df_ebv_rd[clim_var] = df_dlog[clim_var]

        df_ebv_rd.to_parquet(folder_path+'/'+ebv_rd_name,engine='auto',compression='gzip')
        

def ebv_rd_read(folder_path, file_name):
    """
    """

    find_files = []
    for dirpath, dirs, files in os.walk(folder_path):
        for filename in fnmatch.filter(files, file_name):
            find_files.append(os.path.join(dirpath, filename))
    
    ##open files and load them into a dataframe
    if len(find_files) == 0:
        print('No records found.')
        df = pd.DataFrame()
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

        return df_raw