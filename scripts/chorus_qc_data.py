import numpy as np
import pandas as pd

def evaluate_nulls(df):
    print('\nNumber of Nulls in the dataframe:')
    nan = df.isna().sum()
    for i in range(nan.shape[0]):
        print('Column: {} - Nulls: {} - Percentage: {:.2f}%'.format(df.columns[i],nan[i],100*(nan[i]/df.shape[0])))
    
def evaluate_duplicates(df):
    duplicates = df.duplicated()
    n_duplicates = duplicates.sum()
    print('\nNumber of Duplicates in the dataframe: {} - Percentage: {:.2f}%'.format(n_duplicates,100*(n_duplicates/df.shape[0])))
    
def find_outliers(arr):
    # 1st quartil
    q1 = np.quantile(arr, 0.25)
    # 3rd quartil
    q3 = np.quantile(arr, 0.75)
    # 2nd quartil (median)
    med = np.median(arr)
    # inter quartil range (iqr)
    iqr = q3-q1
    # find "wiskers"
    upper_bound = q3+(1.5*iqr)
    lower_bound = q1-(1.5*iqr)
    
    outs = arr[(arr < lower_bound) | (arr > upper_bound)]
    return outs

def plot_outliers(df,date):
    d = s_date.split('-')
    date = dt.datetime(int(d[0]), int(d[1]), int(d[2]))
    df_aux = df[df.Date == date]

    column_names = df_aux.columns
    n_rows = 0
    for n in column_names:
        if (df_aux[n].dtype == float):
            n_rows = n_rows + 1

    fig, ax = plt.subplots(n_rows,2,figsize=(14,10))
    i = 0
    for n in column_names:
        if (df_aux[n].dtype == float):
            v = df_aux[n].values
            t = df_aux['Time'].values
            t2 = np.linspace(0, 24, len(t))

            ax[i,0].boxplot(v,0,'b')
            ax[i,0].set_title("Boxplot Variable {} on the date {}".format(n, s_date))

            ax[i,1].scatter(t2,v,marker='.',color = 'r')
            ax[i,1].set_xlabel('Time')
            ax[i,1].set_ylabel(n)
            #plt.xticks(rotation=90)
            ax[i,1].set_xticks(np.arange(0,24,step=1))
            ax[i,1].xaxis.set_tick_params(labelsize=8)
            #ax[i,1].set_xticklabels(np.arange(0,24,step=1),fontsize = 8)
            ax[i,1].grid()
            ax[i,1].set_title("Variable {} on the date {}".format(n, s_date))
            i = i + 1

    fig.subplots_adjust(hspace=0.4)
    plt.show()

def evaluate_outliers(df):
    outliers = []
    vc = df.Date.value_counts()
    a = vc.index.sort_values()
    i = 0
    for d in a:
        df_aux = df[df.Date == d]
        column_names = df_aux.columns
        i = i + 1
        print('Day {}, date {}'.format(i,d.strftime("%Y-%m-%d")))
        for n in column_names:
            #print(df_aux[n].dtype == np.float)
            if (df_aux[n].dtype == float):
                temps = df_aux[n].values
                outs = find_outliers(temps)
                n_outs = len(outs)
                outliers.append([n,d,n_outs])
                print('The variable {} has {} outliers '.format(n,n_outs))
        print('\n')
        
    r = len(outliers)
    if (r == 0):
        print('There is no outliers to show')
    else:
        column_names = []
        for i in range(r):
            #print(outliers[i][0])
            column_names.append(outliers[i][0])
        column_names = np.unique(column_names)
        #print(column_names)
        n_colnam = len(column_names)
        for i in range(n_colnam):
            x = []
            y = []
            for j in range(int(r/n_colnam)):
                x.append(outliers[(j*n_colnam)+i][1])
                y.append(outliers[(j*n_colnam)+i][2])

            figure = plt.figure
            ax = plt.gca()
            ax.scatter(x,y)
            ax.set_xlabel('Date')
            ax.set_ylabel('Number of outliers')
            plt.xticks(rotation=90)
            ax.set_title("Variable {}".format(column_names[i]))
            plt.scatter(x,y)
            plt.show()
    
    return outliers