"""TThis module contains functions for retrieving data from various sources, including:
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
import chorus_qc_data as qc

def get_datalogger(folder_path, location_id, date_ini, date_fin):
    """Function to obtain the climatic variables from the datalogger files.
    
    Args:
        folder_path (str): path to the folder containing the datalogger files.
        location_id (str): location identifier.
        date_ini (str): start date in YYYY-MM-DD format.
        date_fin (str): end date in YYYY-MM-DD format.
        
    Returns:
        df_new (pandas DataFrame): DataFrame that contains the climatic variables of the datalogger on the requested dates.
    """
    
    dis = date_ini.split('-')
    dfs = date_fin.split('-')
    
    try:
        date_ini_dt = dt.datetime(int(dis[0]), int(dis[1]), int(dis[2]))
    except:
        print('Wrong Start Date.')
        df = pd.DataFrame()
        return df
    
    try:
        date_fin_dt = dt.datetime(int(dfs[0]), int(dfs[1]), int(dfs[2]))
    except:
        print('Wrong End Date.')
        df = pd.DataFrame()
        return df
    
    pattern = location_id + '_datalogger'+'*.xlsx'
    # Step 2: Find files
    find_files = []
    for dirpath, dirs, files in os.walk(folder_path):
        for filename in fnmatch.filter(files, pattern):
            find_files.append(os.path.join(dirpath, filename))

    if len(find_files) == 0:
        print('No records found.')
    else:
        #print('Records found.')
        # Step 3: Open the files and load them into a dataframe
        df = []
        for i in range(len(find_files)):
            file_path = find_files[i]
            df.append(pd.read_excel(file_path,engine='openpyxl',index_col=False))

        # Step 4: Process the dataframes and concatenate the data into a new dataframe
        column_names = ['SN','Date','Time','T(C)_DL','RH(%)_DL','DP(C)_DL']
        df_tot = pd.DataFrame(columns = column_names)
        for i in range(len(df)):
            df_proc = df[i].copy()
            df_proc = df_proc.drop(labels=range(0,3), axis=0)
            df_proc.columns = column_names
            df_proc.reset_index(drop=True)
            df_tot = pd.concat([df_tot,df_proc])
            
        df_tot = df_tot.drop('SN',axis=1)

        df_tot['Date'] = pd.to_datetime(df_tot['Date'])
        df_tot.sort_values(by='Date')
        df_tot = df_tot.reset_index(drop=True)
    
        # Step 5: Select a subset based on dates
        if (df_tot['Date'][0] <= date_ini_dt):
            print('There are records SINCE the requested date.')
        else:
            date_ini_dt = df_tot['Date'][0]
            print('There are not records SINCE the requested date.')
            print('There are only records SINCE : ' + date_ini_dt.strftime("%Y-%m-%d"))
        if (df_tot['Date'][len(df_tot['Date'])-1] >= date_fin_dt):
            print('There are records UP TO the requested date.')
        else:
            date_fin_dt = df_tot['Date'][len(df_tot['Date'])-1]
            print('There are not records UP TO the requested date.')
            print('There are only records UP TO : '+ date_fin_dt.strftime("%Y-%m-%d"))
            
        df_sel = df_tot.loc[(df_tot['Date'] >= date_ini_dt) & (df_tot['Date'] <= date_fin_dt)]
        df_sel = df_sel.reset_index(drop=True)
                
        column_types_dict = {'T(C)_DL': float,
                             'RH(%)_DL': float,
                             'DP(C)_DL': float
                             }
        
        df_sel = df_sel.astype(column_types_dict)
        
        qc.evaluate_nulls(df_sel)
        qc.evaluate_duplicates(df_sel)
        
        return df_sel
               
def get_wstation(folder_path, location_id, date_ini, date_fin):
    """Function to obtain climatic variables from the wheater station files.
    
    Args:
        folder_path (str): path to the folder containing the weather station files.
        location_id (str): location identifier.
        date_ini (str): start date in YYYY-MM-DD format.
        date_fin (str): end date in YYYY-MM-DD format.
        
    Returns:
       df_new (pandas DataFrame): DataFrame that contains the climatic variables of the weather station on the requested dates.
    """
    
    dis = date_ini.split('-')
    dfs = date_fin.split('-')

    try:
        date_ini_dt = dt.datetime(int(dis[0]), int(dis[1]), int(dis[2]))
    except:
        print('Error in Start Date.')
        df = pd.DataFrame()
        return df

    try:
        date_fin_dt = dt.datetime(int(dfs[0]), int(dfs[1]), int(dfs[2]))
    except:
        print('Error in End Date.')
        df = pd.DataFrame()
        return df

    pattern = location_id+'_wstation'+'*.xlsx'
    # Step 2: Find files
    find_files = []
    for dirpath, dirs, files in os.walk(folder_path):
        for filename in fnmatch.filter(files, pattern):
            find_files.append(os.path.join(dirpath, filename))

    if len(find_files) == 0:
        print('No records found.')
        df = pd.DataFrame()
        return df
    else:
        df = []
        for i in range(len(find_files)):
            file_path = find_files[i]
            df.append(pd.read_excel(file_path,engine='openpyxl',index_col=False))

        df_tot = pd.DataFrame()
        for i in range(len(df)):
            df_proc = df[i].copy()
            df_tot = pd.concat([df_tot,df_proc])
            
        #Step
        column_names_dict = {'Date':'Date',
                             'Time':'Time',
                             'Temp. Ins. (C)':'T(C)_WS',
                             'Temp. Max. (C)':'T_max(C)_WS',
                             'Temp. Min. (C)':'T_min(C)_WS',
                             'Umi. Ins. (%)': 'RH(%)_WS',
                             'Umi. Max. (%)': 'RH_max(%)_WS',
                             'Umi. Min. (%)': 'RH_min(%)_WS',
                             'Pto Orvalho Ins. (C)': 'DP(C)_WS',
                             'Pto Orvalho Max. (C)': 'DP_max(C)_WS',
                             'Pto Orvalho Min. (C)': 'DP_min(C)_WS',
                             'Pressao Ins. (hPa)': 'ATM(hPa)_WS',
                             'Pressao Max. (hPa)': 'ATM_max(hPa)_WS',
                             'Pressao Min. (hPa)': 'ATM_min(hPa)_WS',
                             'Vel. Vento (m/s)': 'WND(m/s)_WS',
                             'Radiacao (KJ/m²)': 'Radiant(KJ/m²)_WS',
                             'Chuva (mm)': 'Rainfall(mm)_WS'
                             }

        column_types_dict = {'T(C)_WS':float,
                     'T_max(C)_WS':float,
                     'T_min(C)_WS':float,
                     'RH(%)_WS':float,
                     'RH_max(%)_WS':float,
                     'RH_min(%)_WS':float,
                     'DP(C)_WS':float,
                     'DP_max(C)_WS':float,
                     'DP_min(C)_WS':float,
                     'ATM(hPa)_WS':float,
                     'ATM_max(hPa)_WS':float,
                     'ATM_min(hPa)_WS':float,
                     'WND(m/s)_WS':float,
                     'Radiant(KJ/m²)_WS':float,
                     'Rainfall(mm)_WS':float
                    }

        #Preproccessing: converting UTC Hour to Local Hour
        dif_utc_hour = -3 
        date_ini = df_tot['Date'].dt.date.values[0]
        time = []
        corr = [21,22,23]
        for i in range(len(df_tot)):
            aux = int((df_tot['Hora (UTC)'].iloc[i]/100)+dif_utc_hour)
            if (aux < 0):
                d = date_ini - dt.timedelta(days=1)
                aux = corr[aux]
            aux = dt.time(aux)
            time.append(aux)

        dif_utc_hour = -3
        delta = abs(dif_utc_hour)
        date = []
        date_ini = df_tot['Date'].dt.date.values[0]
        for i in range(delta):
            aux = int((df_tot['Hora (UTC)'].iloc[i]/100)+dif_utc_hour)
            if (aux < 0):
                d = date_ini - dt.timedelta(days=1)
            else:
                d = date_ini
            date.append(d)

        for i in range(len(df_tot)-delta):
            d = df_tot['Date'].dt.date.values[i]
            date.append(d)

        df_tot['Date'] = date
        df_tot.insert(loc=1, column='Time', value=time)

        df_tot['Date'] = pd.to_datetime(df_tot['Date'])
        df_tot.sort_values(by='Date')
        df_tot = df_tot.reset_index(drop=True)

        # Step 5: Select a subset based on dates
        if (df_tot['Date'][0] <= date_ini_dt):
            print('There are records SINCE the requested date.')
        else:
            date_ini_dt = df_tot['Date'][0]
            print('There are not records SINCE the requested date.')
            print('There are only records SINCE : ' + date_ini_dt.strftime("%Y-%m-%d"))
        if (df_tot['Date'][len(df_tot['Date'])-1] >= date_fin_dt):
            print('There are records UP TO the requested date.')
        else:
            date_ini_dt = df_tot['Date'][len(df_tot['Date'])-1]
            print('There are not records UP TO the requested date.')
            print('There are only records UP TO : '+ date_fin_dt.strftime("%Y-%m-%d"))

        df_sel = df_tot.loc[(df_tot['Date'] >= date_ini_dt) & (df_tot['Date'] <= date_fin_dt)]
        df_sel = df_sel.reset_index(drop=True)

        df_sel = df_sel.drop('Hora (UTC)',axis=1)
   
    df_sel.rename(columns = column_names_dict, inplace=True)

    df_new = pd.DataFrame(columns = [col_name for col_name in column_names_dict.values()])

    for col_name in column_names_dict.values():
        df_new[col_name] = df_sel[col_name]

    for col,typ in column_types_dict.items():
        df_new[col] = df_new[col].replace('-',np.nan)
        df_new[col] = df_new[col].astype(typ)

    qc.evaluate_nulls(df_new)
    qc.evaluate_duplicates(df_new)
    
    return df_new

def get_inference(folder_path, location_id, date_ini, date_fin):
    """Function to obtain inferences from the inference files of the machine learning models .
    
    Args:
        folder_path (str): path to the folder containing the inference files.
        location_id (str): location identifier.
        date_ini (str): start date in YYYY-MM-DD format.
        date_fintr): end date in YYYY-MM-DD format.
        
    Returns:
       df_sort (pandas DataFrame): DataFrame that contains the inferences on the requested dates.
    """
    
    dis = date_ini.split('-')
    dfs = date_fin.split('-')

    try:
        date_ini_dt = dt.datetime(int(dis[0]), int(dis[1]), int(dis[2]))
    except:
        print('Error in Start Date.')
        df = pd.DataFrame()
        return df

    try:
        date_fin_dt = dt.datetime(int(dfs[0]), int(dfs[1]), int(dfs[2]))
    except:
        print('Error in End Date.')
        df = pd.DataFrame()
        return df

    pattern = location_id+'_inference'+'*.csv'
    # Step 2: Find files
    find_files = []
    for dirpath, dirs, files in os.walk(folder_path):
        for filename in fnmatch.filter(files, pattern):
            find_files.append(os.path.join(dirpath, filename))

    if len(find_files) == 0:
        print('No records found.')
        df = pd.DataFrame()
        return df
    else:
        df = []
        for i in range(len(find_files)):
            file_path = find_files[i]
            df.append(pd.read_csv(file_path,index_col=False))

        df_new = pd.DataFrame()
        for i in range(len(df)):
            df_proc = df[i].copy()
            df_new = pd.concat([df_new,df_proc])

        #
        column_names_dict = {'date':'Date',
                             'time':'Time',
                             'fname':'file_name',
                             'min':'min',
                             'max':'max',
                             'inference_cnn':'inference_cnn'
                             }

        column_types_dict = {'file_name':'string',
                             'min':float,
                             'max':float,
                             'inference_cnn':float
                             }

        df_new.rename(columns = column_names_dict, inplace=True)

        df_tot = pd.DataFrame(columns = [col_name for col_name in column_names_dict.values()])

        for col_name in column_names_dict.values():
            df_tot[col_name] = df_new[col_name]

        df_tot['Date'] = pd.to_datetime(df_tot['Date'])
        df_tot['Time'] = df_tot['Date'].dt.time
        for col,typ in column_types_dict.items():
            df_tot[col] = df_tot[col].astype(typ)

        df_tot['inference_cnn'] = df_tot['inference_cnn'].replace(to_replace=[0,1],value=[1,0])
        df_tot = df_tot.sort_values(by='Date')
        df_tot = df_tot.reset_index(drop=True)

        # Step 5: Select a subset based on dates
        if (df_tot['Date'][0] <= date_ini_dt):
            print('There are records SINCE the requested date.')
        else:
            date_ini_dt = df_tot['Date'][0]
            print('There are not records SINCE the requested date.')
            print('There are only records SINCE : ' + date_ini_dt.strftime("%Y-%m-%d"))
        if (df_tot['Date'][len(df_tot['Date'])-1] >= date_fin_dt):
            print('There are records UP TO the requested date.')
        else:
            date_ini_dt = df_tot['Date'][len(df_tot['Date'])-1]
            print('There are not records UP TO the requested date.')
            print('There are only records UP TO : '+ date_fin_dt.strftime("%Y-%m-%d"))

        df_sel = df_tot.loc[(df_tot['Date'] >= date_ini_dt) & (df_tot['Date'] <= date_fin_dt)]
        df_sel = df_sel.reset_index(drop=True)

        df_sort = pd.DataFrame(columns = df_sel.columns)
        vc = df_sel.Date.value_counts()
        a = vc.index.sort_values()
        for i in range(len(a)):
            df_aux = df_sel[df_sel.Date == a[i]]
            df_aux = df_aux.sort_values('min')
            df_aux = df_aux.reset_index(drop=True)
            df_sort = pd.concat([df_sort,df_aux])

        df_sort = df_sort.reset_index(drop=True)

        qc.evaluate_nulls(df_sort)
        qc.evaluate_duplicates(df_sort)
        
        return df_sort

def get_metadata(folder_path, file_name, location_id):
    """Function to obtain metadata from a metadata file.
    
    Args:
        folder_path (str): path to the folder containing the metadata file.
        file_name (str): name of the metadata file.
        location_id (str): location identifier.
        
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

        df_tot = pd.DataFrame()
        for i in range(len(df)):
            df_proc = df[i].copy()
            df_tot = pd.concat([df_tot,df_proc])

        df_sel = df_tot.loc[df_tot['location_ID']==location_id]
        df_sel = df_sel.reset_index(drop=True)
        
        df_sel['location_ID'] = df_sel['location_ID'].astype('string')
        df_sel['name_ID'] = df_sel['name_ID'].astype('string')
    
    return df_sel