#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd
import numpy as np
import getpass

import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


def initialize_conn(dict_conn=None):
    """
    Initializes connection to Snowflake

    Parameters
    ----------
    str_query : str
        String query to run via Snowflake connector

    dict_conn : dict
        Dictionary of Snowflake connection parameters.
        Default values are:
            account: xj24206.us-east-1
            warehouse: BI_XSMALL
            database: LOAD_PROD
            schema: PUBLIC

    Returns
    -------
    Snowflake Connection
        Connection to Snowflake
    """
    ### Get username and password via inputs
    sf_username = input("Username:")
    sf_password = getpass.getpass(prompt="Password:")

    ### Set the connection parameters
    if dict_conn:
        ctx = snowflake.connector.connect(
            user=sf_username,
            account=dict_conn["account"],
            warehouse=dict_conn["warehouse"],
            password=sf_password,
            database=dict_conn["database"],
            schema=dict_conn["schema"],
            )
    else:
        ctx = snowflake.connector.connect(
            user=sf_username,
            account='xj24206.us-east-1',
            warehouse="BI_XSMALL",
            password=sf_password,
            database="LOAD_PROD",
            schema="PUBLIC",
            )

    cs = ctx.cursor()
    del sf_password, sf_username
    return cs

def query_snowflake(str_query, cs):
    """
    Queries snowflake and returns a dataframe.

    Parameters
    ----------
    str_query : str
        String query to run via Snowflake connector

    Returns
    -------
    DataFrame
        Pandas dataframe with the results of the query
    """

    ### Query Snowflake and return a dataframe
    cur = cs.execute(str_query)
    df_out = pd.DataFrame\
                   .from_records(iter(cur),
                                 columns=[x[0] for x in cur.description])
    df_out.columns = [x.lower() for x in list(df_out.columns)]
    return df_out

def dict_conn_build():
    """
    
    """
    account_build = input('Account')
    warehouse_build = input('Warehouse')
    database_build = input('Database')
    schema_build = input('Schema')
    if account_build == '':
        account_build = 'xj24206.us-east-1'
    if warehouse_build == '':
        warehouse_build = 'BI_XSMALL'
    if database_build == '':
        database_build = 'LOAD_PROD'
    if schema_build == '':
        schema_build = 'PUBLIC'
    dict_conn = {
        "account": account_build,
        "warehouse": warehouse_build,
        "database": database_build,
        "schema": schema_build
    }
    return dict_conn

# function to recursively find the best combination of fields to make primary key
def find_primary(frame, required_cols=None, exclude_cols=None, threshold=10, improve=0.1):
    """
    Takes a dataframe and finds the ideal primary key built of columns using a recursive method.

    Parameters
    ----------
    frame: dataframe
        Dataframe to recursively generate primary keys from.
    required_cols: list
        List of strings to indicate a column must be included in the primary key generation. Default: all columns in frame
    exclude_cols: list
        List of strings to indicate a column must be excluded in the primary key generation. Default: no columns to be excluded
    threshold: int
        Threshold to accept the value as a primary key. Default: 10
    improve: percent (decimal form)
        Threshold for the amount of improvement an index must achieve to be kept. Default: 0.1 i.e 10% smaller index

    Returns
    -------
    DataFrame
        Pandas dataframe with the best possible primary key build
    """
    uniqueInd = False # boolean check for unique index
    if required_cols:
        frame = frame.set_index(required_cols) # set the index to required columns if passed
    ### find number of unique values per column
    dict_ucols = {}
    for col in frame.columns.values:
        dict_ucols[col] = frame[col].value_counts().max()
    if exclude_cols:
        #frame = frame.drop(columns=exclude_cols)
        for col in exclude_cols:
            del dict_ucols[col]
    ### if no required cols, set smallest repeated val col as index
    if not required_cols:
        minCol = min(dict_ucols, key=dict_ucols.get)
        frame = frame.set_index(min(dict_ucols, key=dict_ucols.get))
        del dict_ucols[minCol]
    if len(frame[frame.index.duplicated()]) < threshold:
        uniqueInd = True
    ### recursively set next lowest column as index
    while not uniqueInd and (len(dict_ucols) > 0):
        minCol = min(dict_ucols, key=dict_ucols.get)
        df_improve = frame.set_index(min(dict_ucols, key=dict_ucols.get), append=True)
        ## note: need to improve upon this metric evaluation
        if len(frame[frame.index.duplicated()]) * (1-improve) > len(df_improve[df_improve.index.duplicated()]):
            frame = df_improve
        del dict_ucols[minCol]
        if len(frame[frame.index.duplicated()]) < threshold:
            uniqueInd = True
    return frame

def unique_counts(frame):
    """
    Takes a dataframe and finds the count of unique values for each column.

    Parameters
    ----------
    frame: dataframe
        Dataframe to find unique column counts.
        
    Returns
    -------
    Dictionary
        Dictionary of the following format: {'column name': unique_count}
    """
    dict_unique_cols = {}
    for col in frame.columns.values:
        dict_unique_cols[col] = frame[col].value_counts().max()
    return dict_unique_cols