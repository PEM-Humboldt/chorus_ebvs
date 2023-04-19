"""This module contains functions to harmonize the data of the inferences and the climatic variables:
"""
import numpy as np
import pandas as pd
import datetime as dt

def harmonize3(df_inf,df_dlog,df_wst):
    """Function to harmonize information from inferences, dataloggers and weather stations.
    
    Args:
        df_inf (pandas DataFrame): DataFrame that contains the information of the inferences.
        df_dlog (pandas DataFrame): DataFrame that contains the information of the climatic variables of the dataloggers.
        df_wst (pandas DataFrame): DataFrame that contains the information of the climatic variables of the weather stations.
        
    Returns:
       df_inf_h (pandas DataFrame): DataFrame that contains the harmonized information of the inferences.
       df_dlog_h (pandas DataFrame): DataFrame that contains the harmonized information of the datalogger.
       df_wst_h (pandas DataFrame): DataFrame that contains the harmonized information of the weather station.
    """
    
    df_inf_r = resampling_inference(df_inf)
    
    vc = df_inf_r.Date.value_counts()
    a = vc.index.sort_values()
    d_ini = pd.to_datetime(a[0].strftime("%Y-%m-%d"))
    d_fin = pd.to_datetime(a[len(a)-1].strftime("%Y-%m-%d"))
    date_list = pd.date_range(d_ini, d_fin)
    t_ini = df_inf_r.Time[0]
    times_list= []
    flag = 1
    dates_list = []
    second = t_ini.second
    delta_t = 15

    for j in range(len(date_list)):

        if (flag == 1):
            ini_h = t_ini.hour
        else:
            ini_h = 0

        for hour in range(ini_h,24):
            if (flag == 1):
                ini_t = t_ini.minute
                flag = 0
            else:
                ini_t = 0

            for minute in range(ini_t, 60, delta_t):

                dates_list.append(date_list[j])
                time_str = '{:02d}:{:02d}:{:02d}'.format(hour, minute,second)
                time_object = dt.datetime.strptime(time_str, '%H:%M:%S').time()
                times_list.append(time_object)

    column_names = ['Date','Time','File_Name','Absences','Presences']
    type_dict = {'File_Name': 'string',
                 'Absences': float,
                 'Presences': float
                 }
    df_inf_h = harmonize_inference(df_inf_r,column_names,type_dict,dates_list,times_list)
    
    column_names = ['Date',
                'Time',
                'T(C)_WS',
                'T_max(C)_WS',
                'T_min(C)_WS',
                'RH(%)_WS',
                'RH_max(%)_WS',
                'RH_min(%)_WS',
                'DP(C)_WS',
                'DP_max(C)_WS',
                'DP_min(C)_WS',
                'ATM(hPa)_WS',
                'ATM_max(hPa)_WS',
                'ATM_min(hPa)_WS',
                'WND(m/s)_WS',
                'Radiant(KJ/m²)_WS',
                'Rainfall(mm)_WS'
                ]
    type_dict = {'T(C)_WS':float,
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
    df_wst_h = harmonize_wstation(df_wst,column_names,type_dict,dates_list,times_list)
    
    column_names = ['Date','Time','T(C)_DL','RH(%)_DL','DP(C)_DL']
    type_dict = {'T(C)_DL': float,
                 'RH(%)_DL': float,
                 'DP(C)_DL': float
                }
    df_dlog_h = harmonize_dlogger(df_dlog, column_names, type_dict, dates_list, times_list)
    
    return df_inf_h,df_dlog_h,df_wst_h

def harmonize2(df_inf,df_dlog):
    
    df_inf_r = resampling_inference(df_inf)
    
    vc = df_inf_r.Date.value_counts()
    a = vc.index.sort_values()
    d_ini = pd.to_datetime(a[0].strftime("%Y-%m-%d"))
    d_fin = pd.to_datetime(a[len(a)-1].strftime("%Y-%m-%d"))
    date_list = pd.date_range(d_ini, d_fin)
    t_ini = df_inf_r.Time[0]
    times_list= []
    flag = 1
    dates_list = []
    second = t_ini.second
    delta_t = 15

    for j in range(len(date_list)):

        if (flag == 1):
            ini_h = t_ini.hour
        else:
            ini_h = 0

        for hour in range(ini_h,24):
            if (flag == 1):
                ini_t = t_ini.minute
                flag = 0
            else:
                ini_t = 0

            for minute in range(ini_t, 60, delta_t):

                dates_list.append(date_list[j])
                time_str = '{:02d}:{:02d}:{:02d}'.format(hour, minute,second)
                time_object = dt.datetime.strptime(time_str, '%H:%M:%S').time()
                times_list.append(time_object)

    column_names = ['Date','Time','File_Name','Absences','Presences']
    type_dict = {'File_Name': 'string',
                 'Absences': float,
                 'Presences': float
                 }
    df_inf_h = harmonize_inference(df_inf_r,column_names,type_dict,dates_list,times_list)
        
    column_names = ['Date','Time','T(C)_DL','RH(%)_DL','DP(C)_DL']
    type_dict = {'T(C)_DL': float,
                 'RH(%)_DL': float,
                 'DP(C)_DL': float
                }
    df_dlog_h = harmonize_dlogger(df_dlog, column_names, type_dict, dates_list, times_list)
    
    return df_inf_h,df_dlog_h

def resampling_inference(df):
    column_names = ['Date','Time','File_Name','Absences','Presences']
    df_resamp = pd.DataFrame(columns = column_names)
    vc = df.Date.value_counts()
    a = vc.index.sort_values()
    for i in range(len(a)):
        df_aux = df[df.Date == a[i]]
        n_presences = df_aux['inference_cnn'].sum()
        n_absences = df_aux.shape[0]-n_presences
        d = {'Date':a[i].strftime("%Y-%m-%d"),
         'Time': df.Time[i*df_aux.shape[0]],
         'File_Name': df.file_name[i*df_aux.shape[0]],
         'Absences': n_absences,
         'Presences': n_presences}
        df_aux = pd.DataFrame(d, index=[0])
        df_aux = df_aux.reset_index(drop=True)
        df_resamp = pd.concat([df_resamp,df_aux])

    df_resamp['Date'] = pd.to_datetime(df_resamp['Date'])
    df_resamp['File_Name'] = df_resamp['File_Name'].astype('string')
    df_resamp = df_resamp.reset_index(drop=True)
    
    return df_resamp

def resampling_datalogger(df, temp_res_min = 15):
    idx = []
    i=0
    s_minute = df.Time[i].minute
    s_hour = df.Time[i].hour
    idx.append(i)
    tol_minute = 3

    for i in range(1,df.shape[0]):
        c_hour = df.Time[i].hour
        c_minute = df.Time[i].minute

        delta_hour = c_hour - s_hour

        if (delta_hour == 0):
            delta_minute = c_minute - s_minute
        else:
            offset_minute = 60 - s_minute
            delta_minute = c_minute + offset_minute        

        dif_minute = abs(temp_res_min - delta_minute)

        if(dif_minute <= tol_minute):
            idx.append(i)
            s_minute = df.Time[i].minute
            s_hour = df.Time[i].hour

    df_resamp = df.loc[idx]
    df_resamp = df_resamp.reset_index(drop=True)
    
    return df_resamp

def harmonize_inference(df, column_names, type_dict,dates_list, times_list):
    
    df_h = pd.DataFrame(columns = column_names)
    df_h['Date'] = dates_list
    df_h['Time'] = times_list

    for i in range (len(dates_list)):
        df_aux = df[df['Date'] == dates_list[i]]
        #df_sel = df_aux[df_aux['Time'].astype('string')==times_list[i]]
        df_sel = df_aux[df_aux['Time']==times_list[i]]
        if (df_sel.shape[0] != 0):
            for j in range (2,len(column_names)):
                df_h.at[i,column_names[j]] = df_sel.iloc[0,j]

    df_h = df_h.astype(type_dict)
    
    return df_h

def harmonize_wstation(df, column_names, type_dict,dates_list, times_list):
    
    df_h = pd.DataFrame(columns = column_names)
    df_h['Date'] = dates_list
    df_h['Time'] = times_list

    for i in range (len(dates_list)):
        df_aux = df[df['Date'] == dates_list[i]]
        df_sel = df_aux[df_aux['Time']==times_list[i]]
        if (df_sel.shape[0] != 0):
            for j in range (2,len(column_names)):
                df_h.at[i,column_names[j]] = df_sel.iloc[0,j]

    df_h = df_h.astype(type_dict)
    
    return df_h

def harmonize_dlogger(df, column_names, type_dict, dates_list, times_list):
    
    df_h = pd.DataFrame(columns = column_names)
    df_h['Date'] = dates_list
    df_h['Time'] = times_list

    for i in range (len(dates_list)):

        idx = []
        th = 10

        df_aux = df[df['Date'] == dates_list[i]]
        t1 = dt.datetime.strptime(times_list[i].strftime('%H:%M:%S'), "%H:%M:%S")
        for k in range (df_aux.shape[0]):
            t2 = dt.datetime.strptime(df_aux.iloc[k]['Time'].strftime('%H:%M:%S'), "%H:%M:%S")
            d = t2 - t1
            d_min = d.total_seconds()/60
            if (abs(d_min)<=10):
                idx.append(k)
            if d_min > 10:
                break
            #print(d_min)
        xs = []
        temps = []
        rhs = []
        dps = []
        for k in idx:
            xs.append(df_aux.iloc[k]['Time'].hour*60+df_aux.iloc[k]['Time'].minute)
            temps.append(df_aux.iloc[k]['T(C)_DL'])
            rhs.append(df_aux.iloc[k]['RH(%)_DL'])
            dps.append(df_aux.iloc[k]['DP(C)_DL'])

        value = times_list[i].hour*60+times_list[i].minute
        temp_int = interpolation(xs, temps, value)
        rh_int = interpolation(xs, rhs, value)
        dp_int = interpolation(xs, dps, value)

        df_h.at[i,'T(C)_DL'] = temp_int
        df_h.at[i,'RH(%)_DL'] = rh_int
        df_h.at[i,'DP(C)_DL'] = dp_int

    df_h = df_h.astype(type_dict)
    
    return df_h

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