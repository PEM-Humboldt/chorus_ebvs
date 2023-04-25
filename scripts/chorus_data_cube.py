#!/usr/bin/env python3

"""This module contains functions for building and reading EBV-ready datasets in the NetCDF format
"""
import pandas as pd
import netCDF4 as nc

def build_ebv(ebvs_file_name, ebvs_metadata_file, df_inf_h, df_dlog_h, df_meta):
    """
    Args:
        ebvs_file_name (str): name of the NetCDF file containing the EBV-ready dataset
        ebvs_metadata_file (str):
        df_inf_h (pandas DataFrame):
        df_dlog_h (pandas DataFrame):
        df_meta (pandas DataFrame):
    """
    
    # Step 0: create the NetCDF file
    ds = nc.Dataset(ebvs_file_name,'w',format='NETCDF4')

    # Step 1: create dimensions
    # time dimension
    time_dim = df_inf_h.shape[0]
    time = ds.createDimension('time', time_dim)
    # latitud and longitud dimensions
    lat_dim = df_meta.shape[0]
    lon_dim = df_meta.shape[0]
    lat = ds.createDimension('lat',lat_dim)
    lon = ds.createDimension('lon',lon_dim)
    # number of species dimension = estimated vocal activity dimension
    n_species = df_inf_h.columns.str.startswith('EVA_').sum()
    eva = ds.createDimension('eva', n_species)

    # Step 2: create variables
    times = ds.createVariable('time','f4',('time',))
    lats = ds.createVariable('lat','f4',('lat',))
    lons = ds.createVariable('lon','f4',('lon',))
    evas = ds.createVariable('eva','f4',('time','eva'))
    temps = ds.createVariable('temp','f4',('time','lat','lon',))
    temps.units = 'degree_C'
    rhs = ds.createVariable('rh','f4',('time','lat','lon',))
    dps = ds.createVariable('dp','f4',('time','lat','lon',))

    # Step 4: assign  values to variables
    times = df_inf_h['Date'].values
    lats = df_meta.loc[0,'lat_DL']
    lons = df_meta.loc[0,'long_DL']
    evas = df_inf_h['EVA_BOAFAB'].values
    temps = df_dlog_h['T(C)_DL'].values
    rhs = df_dlog_h['RH(%)_DL'].values
    dp = df_dlog_h['DP(C)_DL'].values

    ds.close()
    
def read_ebv(ebvs_file_name):
    """
    Args:
        ebvs_file_name (str): name of the NetCDF file containing the EBV-ready dataset
        
    Returns:
        ds (netCDF4 DataStructure): DataSructure that contains the EBV-ready dataset

    """
    ds = nc.Dataset(ebvs_file_name)
    
    return ds