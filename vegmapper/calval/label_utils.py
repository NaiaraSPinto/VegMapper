import pandas as pd
import numpy as np
import re
import random

#from google.colab import drive
from datetime import datetime as dt
from pylab import rcParams

import warnings
import os



# read datasets into list, keep the most important columns, and do some renaming

def load_csv(csv_path):
    """
    Load a single csv file into a pandas dataframe.
    """
    csv_data = pd.read_csv(csv_path, index_col=False)
    
    return csv_data


def subset_cols(df, col_list):
    """
    Keep the selected columns only.
    -args:
    df: a pandas dataframe
    col_list: list of selected columns
    return: a pandas dataframe with only selected columns
    """
    
    df = df.copy()

    return df.loc[:, col_list]



def rename_cols(df, update_dict):
    """
    Update a pandas dataframe's column names use a dictionary. The function will
    raise a ValueError if the user asks to change a column name that does not exist.
    -args:
    df: a pandas dataframe  
    update_dict: a dictionary with {old_name: new_name}
    return: a pandas dataframe with updated column names
    """
    df = df.copy()
    
    if not set(update_dict.keys()).issubset(df.columns): 
        raise ValueError("One or multiple old name(s) do not exist in the dataframe.")
    
    df.rename(columns=update_dict, inplace=True)
    
    return df


def check_exclusive(df,csv_path):
    """
    This function valids the label entry in the samples. For a single labeler, for a data point,
    only one of the four (Young","Mature","Not" (Not Oil Palm), and "NotSure") can be labeled as
    true.
    E.g., if the "Young" is labeled as 100 (true), then the other three columns have to be 0. 
    This function will print a Warning if the four classes are not mutually exclude in any entry.
    This function will use hard-code column names "Young","Mature","Not", and "NotSure". Also, 'True'
    is hard-coded as 100, and 'False' is hard-coded as 0.
    -args: 
    df: a pandas dataframe
    No return 
    """
    
    df = df.copy()
        
    # sum of each row should equals 100
    check_sum = df[["Young","Mature","Not","NotSure"]].sum(axis=1, skipna=False)
    if check_sum.isin([100]).all():
        print("The labeled classes are mutually exclusive.")
    else:
        warnings.warn('Found at least one entry(s) that does not have mutually exclusive labels.\n\
        >>>file: {}<<<\n\
        Check your columns "Young","Mature","Not", and "NotSure".\n\
        (1)Make sure no empty entry in those columns.\n\
        (2)Make sure there is one and only one column is labeled as 100.'.format(csv_path))

def recode(df, recode_dict, label_name):
    """
    Create a new column called label. Fill this class column based on labels
    0: Not
    1: Young
    2: Mature
    3: NotSure
    *Use check_exclusive() first to make sure there is one and only one column = 100.
    
    -args:
    df: a pandas dataframe
    recode_dict: a dictionary with {col1:[old_value,new_value], col2:[old_value, new_value]}
    
    return: a pandas dataframe with recode values
    """
    df = df.copy()
    
    # collapse sparse matrix into a list. For each row, the label with 100 will be selected.
    df_densemat = df[["Young","Mature","Not","NotSure"]].idxmax(axis=1)
    
    # create a column called "label", fill with the label list.
    df = df.assign(**{label_name:df_densemat})
    
    df.replace({label_name: recode_dict},inplace=True)
    
    return df

def combine_labelers(pd_list,by=["Point_ID","Clust"], label_name="label"):
    """
    user 1's label will be like "label_1"; 
    user 2 is "label_2" etc...
    """
    base = pd_list[0]
    
    # user 2's suffix is 2 (by setting enumerate idx start=2) 
    if len(pd_list)>1:
        for idx, i in enumerate(pd_list[1:], start=2):
            base = pd.merge(base, i[[*by, label_name]], how = 'left',on=by, suffixes =(None, '_'+str(idx)))

    # let the first user to be "_1"
    base = rename_cols(base, {label_name:label_name + '_1'})
    return base

def find_mode(df):
    """
    Create a concensus label called "mode_label" based on the most freqent label (mode) across labelers.
    When there is a complete disagreement among labelers, give -9999
    """
    
    df = df.copy()
    labels = df.filter(like='label')
    mode_label = labels.mode(axis=1)
    agreement_flag = mode_label.isnull().any(axis=1)
    
    df = df.assign(mode_label = -9999)
    df.mode_label.mask(agreement_flag, mode_label[0], axis=0, inplace=True)

    return df

# Function to get mode and occurrence by row (credit to ChatGPT)
def get_mode_and_occurence(row):
    """
    Get modal and the occurance of modal (a ratio) for each row.
    This function will be applied to each row of a pandas dataframe.
    -agrs:
    row: a row that only includes labeler's labels
    return two value
    """
    mode = row.mode().values[0]
    occurrence = row.value_counts()[mode]/row.count()
      
    return mode, occurrence
