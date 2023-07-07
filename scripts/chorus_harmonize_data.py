#!/usr/bin/env python3

"""This script contains functions to harmonize the data from inferences and climate variables using the temporal variable with a resolution of 15 minutes.
"""
import numpy as np
import pandas as pd
import datetime as dt
import chorus_utils as chutils
import chorus_qc_data as qdata

def harmonize_inference(df_inf,time_list_np64,date_list,hour_list):
    
    time_cols = ['time','date','hour']
    eva_cols = df_inf.columns[4:].values.tolist()
    column_names = time_cols + eva_cols
    df_inf_h = pd.DataFrame(columns = column_names,)
    
    df_inf_h['time'] = time_list_np64
    df_inf_h['date'] = date_list
    df_inf_h['hour'] = hour_list
    df_aux = df_inf.groupby("date")[eva_cols].max()
    for i in range(len(df_inf_h)):
        values = df_aux[df_aux.index.values == df_inf_h.time.values[i]]
        if len(values) != 0:
            df_inf_h.iloc[i,3:] = values.values[0]
    
    labels = chutils.get_inference_labels()
    for label in labels:
        if (label.new_name in column_names):
            if (label.new_name != 'date' and label.new_name != 'time'):
                df_inf_h[label.new_name] = df_inf_h[label.new_name].astype(label.new_dtype)
    
    print("Harmonized Inference Data")
    
    #qdata.evaluate_nulls(df_inf_h)
    #qdata.evaluate_duplicates(df_inf_h)
    
    return df_inf_h

def interpolation(X,Y,value):
    # calculate the mean of X and Y using the numpy's built-in method mean()
    mean_x = np.mean(X)
    mean_y = np.mean(Y)
    # total number of input values
    m = len(X)
    # using the formula to calculate m and b
    numer = 0
    denom = 0
    for i in range(m):
      numer += (X[i] - mean_x) * (Y[i] - mean_y)
      denom += (X[i] - mean_x) ** 2
    m = numer / denom
    b = mean_y - (m * mean_x)
    
    pred = b + m * value
    
    return round(pred,1)

def median(Y):
    return np.median(Y)

def harmonize_datalogger(df,time_list_np64,date_list,hour_list,T_sample):
    
    time_cols = ['time','date','hour']
    
    climatic_cols = list(df.columns[2:])
    column_names = time_cols + climatic_cols
    df_h = pd.DataFrame(columns = column_names)
    
    df_h['time'] = time_list_np64
    df_h['date'] = date_list
    df_h['hour'] = hour_list
    
    lim_inf = 0
    
    for i in range (len(time_list_np64)):
    
        idx = []
        th = T_sample
        df_aux = df[df['date'] == np.datetime64(date_list[i])]
        if (df_aux.shape[0] != 0):
            df_aux = df_aux.sort_values(by='time')
            df_aux = df_aux.reset_index(drop=True)
            t1 = hour_list[i]
            for k in range (lim_inf,df_aux.shape[0]):
                t2 = df_aux.time[k]
                datetime1 = dt.datetime.combine(date_list[i], t1)
                datetime2 = dt.datetime.combine(date_list[i], t2)
                d = datetime2 - datetime1
                d_min = d.total_seconds()/60
                if (abs(d_min)<=th):
                        idx.append(k)
                if d_min > th:
                    break
    
            if (len(idx) != 0):
                xs = []
                ys = []
                for j in range(3,len(column_names)):
                    for k in idx:
                        xs.append(df_aux.iloc[k]['time'].hour*60+df_aux.iloc[k]['time'].minute)
                        ys.append(df_aux.iloc[k][j-1])
                    x_interp = hour_list[i].hour*60+hour_list[i].minute
                    y_interp = interpolation(xs, ys, x_interp)
                    df_h.iloc[i,j] = y_interp
            
    print("\nHarmonized Climatic Variables from Datalogger\n")
    #qdata.evaluate_nulls(df_h)
    #qdata.evaluate_duplicates(df_h)
        
    return df_h

def harmonize_wstation(df,time_list_np64,date_list,hour_list,T_sample):
            
    time_cols = ['time','date','hour']

    climatic_cols = list(df.columns[2:])
    column_names = time_cols + climatic_cols
    df_h = pd.DataFrame(columns = column_names)
    
    df_h['time'] = time_list_np64
    df_h['date'] = date_list
    df_h['hour'] = hour_list

    type_df = str(type(df.date.values[0]))

    if type_df != "<class 'numpy.datetime64'>":
        for i in range (len(time_list_np64)):
            df_aux = df[df['date'] == np.datetime64(date_list[i])]
            df_aux = df_aux.sort_values(by='time')
            df_aux = df_aux.reset_index(drop=True)
            df_sel = df_aux[df_aux['time']==hour_list[i]]
            if (df_sel.shape[0] != 0):
                for j in range (3,df_sel.shape[1]):
                        df_h.at[i,column_names[j]] = df_sel.iloc[0,j-1]
    else:
        for i in range (len(time_list_np64)):
            df_sel = df[df['date'] == time_list_np64[i]]
            if (df_sel.shape[0] != 0):
                for j in range (3,df_sel.shape[1]+1):
                    df_h.at[i,column_names[j]] = df_sel.iloc[0,j-1]
                
    print("Harmonized Climatic Variables from Weather Station")
    #qdata.evaluate_nulls(df_h)
    #qdata.evaluate_duplicates(df_h)
    
    return df_h

def harmonize3(df_inf,df_dlog,df_wst,T_sample = 15):
    """
    Function to harmonize information from inferences, dataloggers and weather stations.
    
    Args:
        df_inf (pandas DataFrame): DataFrame that contains the information of the inferences.
        df_dlog (pandas DataFrame): DataFrame that contains the information of the climatic variables of the dataloggers.
        df_wst (pandas DataFrame): DataFrame that contains the information of the climatic variables of the weather stations.
        
    Returns:
       df_inf_h (pandas DataFrame): DataFrame that contains the harmonized information of the inferences.
       df_dlog_h (pandas DataFrame): DataFrame that contains the harmonized information of the datalogger.
       df_wst_h (pandas DataFrame): DataFrame that contains the harmonized information of the weather station.
    """
    
    dini = df_inf.date[0]
    dfin = df_inf.date[len(df_inf)-1]
    freq = str(T_sample)+"min"
    time_list_ts = pd.date_range(dini, dfin, freq=freq)
    time_list_np64 = np.array(time_list_ts,dtype=np.dtype('datetime64[ns]'))
    date_list = []
    hour_list = []
    for ts in time_list_ts:
        date_list.append(ts.date())
        hour_list.append(ts.time())

    df_inf_h = harmonize_inference(df_inf,time_list_np64,date_list,hour_list)
    df_dlog_h = harmonize_datalogger(df_dlog,time_list_np64,date_list,hour_list,T_sample)
    df_wst_h = harmonize_wstation(df_wst,time_list_np64,date_list,hour_list,T_sample)

    return df_inf_h,df_dlog_h,df_wst_h

def harmonize2(df_inf,df_dlog,T_sample = 15):
    """
    Function to harmonize information from inferences, dataloggers and weather stations.
    
    Args:
        df_inf (pandas DataFrame): DataFrame that contains the information of the inferences.
        df_dlog (pandas DataFrame): DataFrame that contains the information of the climatic variables of the dataloggers.
        df_wst (pandas DataFrame): DataFrame that contains the information of the climatic variables of the weather stations.
        
    Returns:
       df_inf_h (pandas DataFrame): DataFrame that contains the harmonized information of the inferences.
       df_dlog_h (pandas DataFrame): DataFrame that contains the harmonized information of the datalogger.
       df_wst_h (pandas DataFrame): DataFrame that contains the harmonized information of the weather station.
    """
    
    dini = df_inf.date[0]
    dfin = df_inf.date[len(df_inf)-1]
    freq = str(T_sample)+"min"
    time_list_ts = pd.date_range(dini, dfin, freq=freq)
    time_list_np64 = np.array(time_list_ts,dtype=np.dtype('datetime64[ns]'))
    date_list = []
    hour_list = []
    for ts in time_list_ts:
        date_list.append(ts.date())
        hour_list.append(ts.time())

    df_inf_h = harmonize_inference(df_inf,time_list_np64,date_list,hour_list)
    df_dlog_h = harmonize_datalogger(df_dlog,time_list_np64,date_list,hour_list,T_sample)
    
    return df_inf_h,df_dlog_h

def combine_climvar(df_dlog, df_wst):
    
    time_cols = ['time','date','hour']
    climatic_cols = ['T(C)','RH(%)','DP(C)','Rainfall(mm)']
    column_names = time_cols + climatic_cols
    df_climvar = pd.DataFrame(columns = column_names)
    
    df_climvar.time = df_dlog.time
    df_climvar.date = df_dlog.date
    df_climvar.hour = df_dlog.hour
    
    for i in range(df_climvar.shape[0]):
        #temperature
        if (np.isnan(df_dlog['T(C)_DL'][i])!=1):
            df_climvar.at[i,'T(C)'] = df_dlog.loc[i,'T(C)_DL']
        elif (np.isnan(df_wst['T_max(C)_WS'][i])!=1 and np.isnan(df_wst['T_min(C)_WS'][i])!=1):
            temp = (df_wst['T_max(C)_WS'][i]+df_wst['T_min(C)_WS'][i])/2
            df_climvar.at[i,'T(C)'] = temp
        elif (np.isnan(df_wst['T_max(C)_WS'][i])!=1):
            df_climvar.at[i,'T(C)'] = df_dlog.loc[i,'T_max(C)_WS']
        elif (np.isnan(df_wst['T_max(C)_WS'][i])!=1):
            df_climvar.at[i,'T(C)'] = df_dlog.loc[i,'T_max(C)_WS']
    
    df_climvar['RH(%)'] = df_dlog['RH(%)_DL']
    df_climvar['DP(C)'] = df_dlog['DP(C)_DL']
    df_climvar['Rainfall(mm)'] = df_wst['Rainfall(mm)_WS']

    return df_climvar