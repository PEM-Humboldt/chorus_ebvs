#!/usr/bin/env python3

from collections import namedtuple
import datetime as dt

def get_inference_labels():
    
    Label = namedtuple( 'Label' , [
    'ori_name',
    'new_name',
    'new_dtype',
    'enable',
    ])
    
    labels = [
        #   name new_name new_dtype enable
        Label('date','date','datetime64[ns]', True),
        Label('time','time',dt.time, True),
        Label('path_audio','path_audio',str, False),
        Label('fname','file_name',str, False),
        Label('sample_rate','sample_rate',float, False),
        Label('channels','channels',float, False),
        Label('bits','bits',float, False),
        Label('samples','samples',float, False),
        Label('length','length',float, False),
        Label('fsize','fsize',float, False),
        Label('sensor_name','sensor_name',str, False),
        Label('min','min',int, True),
        Label('max','max',int, True),
        Label('SPHSUR','SPHSUR',float, True),
        Label('BOABIS','BOABIS',float, True),
        Label('SCIPER','SCIPER',float, True),
        Label('DENNAH','DENNAH',float, True),
        Label('LEPLAT','LEPLAT',float, True),
        Label('RHIICT','RHIICT',float, True),
        Label('BOALEP','BOALEP',float, True),
        Label('BOAFAB','BOAFAB',float, True),
        Label('PHYCUV','PHYCUV',float, True),
        Label('DENMIN','DENMIN',float, True),
        Label('ELABIC','ELABIC',float, True),
        Label('BOAPRA','BOAPRA',float, True),
        Label('DENCRU','DENCRU',float, True),
        Label('BOALUN','BOALUN',float, True),
        Label('BOAALB','BOAALB',float, True),
        Label('PHYMAR','PHYMAR',float, True),
        Label('PITAZU','PITAZU',float, True),
        Label('PHYSAU','PHYSAU',float, True),
        Label('LEPFUS','LEPFUS',float, True),
        Label('DENNAN','DENNAN',float, True),
        Label('PHYALB','PHYALB',float, True),
        Label('LEPLAB','LEPLAB',float, True),
        Label('SCIFUS','SCIFUS',float, True),
        Label('BOARAN','BOARAN',float, True),
        Label('SCIFUV','SCIFUV',float, True),
        Label('AMEPIC','AMEPIC',float, True),
        Label('LEPPOD','LEPPOD',float, True),
        Label('ADEDIP','ADEDIP',float, True),
        Label('ELAMAT','ELAMAT',float, True),
        Label('PHYNAT','PHYNAT',float, True),
        Label('LEPELE','LEPELE',float, True),
        Label('RHISCI','RHISCI',float, True),
        Label('SCINAS','SCINAS',float, True),
        Label('LEPNOT','LEPNOT',float, True),
        Label('ADEMAR','ADEMAR',float, True),
        Label('BOAALM','BOAALM',float, True),
        Label('PHYDIS','PHYDIS',float, True),
        Label('RHIORN','RHIORN',float, True),
        Label('LEPFLA','LEPFLA',float, True),
        Label('SCIRIZ','SCIRIZ',float, True),
        Label('DENELE','DENELE',float, True),
        Label('SCIALT','SCIALT',float, True),
        Label('visita','visita',str, False),
        Label('site','site',str, False),
        Label('day','day',str, False)
    ]
    
    return labels

def get_datalogger_labels():
    
    Label = namedtuple( 'Label' , [
    'ori_name',
    'new_name',
    'new_dtype',
    'enable',
    ])
    
    labels = [
        #   name new_name new_dtype enable
        Label('SN','SN',int, False),
        Label('DATE','date','datetime64[ns]', True),
        Label('TIME','time',dt.time, True),
        Label('\toC','T(C)_DL',float, True),
        Label('\t%RH','RH(%)_DL',float, True),
        Label('\tDP','DP(C)_DL',float, True),
    ]
    
    return labels

def get_wstation_labels():
    
    Label = namedtuple( 'Label' , [
    'ori_name',
    'new_name',
    'new_dtype',
    'enable',
    ])

    labels = [
        #   name new_name new_dtype enable
        Label('Date','date','datetime64[ns]', True),
        Label('Hora (UTC)','time',dt.time, True),
        Label('Temp. Ins. (C)','T(C)_WS',float, True),
        Label('Temp. Max. (C)','T_max(C)_WS',float, True),
        Label('Temp. Min. (C)','T_min(C)_WS',float, True),
        Label('Umi. Ins. (%)','RH(%)_WS',float, True),
        Label('Umi. Max. (%)','RH_max(%)_WS',float, True),
        Label('Umi. Min. (%)','RH_min(%)_WS',float, True),
        Label('Pto Orvalho Ins. (C)','DP(C)_WS',float, True),
        Label('Pto Orvalho Max. (C)','DP_max(C)_WS',float, True),
        Label('Pto Orvalho Min. (C)','DP_min(C)_WS',float, True),
        Label('Pressao Ins. (hPa)','ATM(hPa)_WS',float, True),
        Label('Pressao Max. (hPa)','ATM_max(hPa)_WS',float, True),
        Label('Pressao Min. (hPa)','ATM_min(hPa)_WS',float, True),
        Label('Vel. Vento (m/s)','WND(m/s)_WS',float, True),
        Label('Dir. Vento (m/s)','WND2(m/s)_WS',float, False),
        Label('Raj. Vento (m/s)','WND3(m/s)_WS',float, False),
        Label('Radiacao (KJ/m²)','Radiant(KJ/m²)_WS',float, True),
        Label('Chuva (mm)','Rainfall(mm)_WS',float, True),
    ]

    return labels

def get_locations_labels():
    
    Label = namedtuple( 'Label' , [
    'ori_name',
    'new_name',
    'new_dtype',
    'enable',
    ])

    labels = [
        #   name new_name new_dtype enable
        Label('location_ID','location_ID',str, True),
        Label('name_ID','name_ID',str, True),
        Label('lat_DL','lat_DL',float, True),
        Label('lon_DL','lon_DL',float, True),
        Label('WStation','WStation',bool, True),
        Label('lat_WS','lat_WS',float, True),
        Label('lon_WS','lon_WS',float, True),
    ]

    return labels