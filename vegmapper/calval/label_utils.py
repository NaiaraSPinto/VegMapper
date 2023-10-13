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

def check_exclusive(df, csv_path, new_col_names):
    
    """
    This function valids the label entry in the samples. For a single labeler, for a data point,
    only one of the label columns (e.g. presence, absence, and unsure) can be labeled as
    true. Then, if one is labeled as 100 (true), the other columns have to be 0. 
    This function will print a Warning if the classes are not mutually exclude in any entry.
    This function uses 'True' as hard-coded 100, and 'False' as hard-coded  0.
    -args: 
    df: a pandas dataframe
    csv_path: path to the CSVs
    new_col_names: a list of desired column names

    No return 
    """
    
    df = df.copy()
        
    # sum of each row should equals 100
    check_sum = df[new_col_names].sum(axis=1, skipna=False)
    if check_sum.isin([100]).all():
        print("The labeled classes are mutually exclusive.")
    else:
        warnings.warn('Found at least one entry(s) that does not have mutually exclusive labels.\n\
        >>>file: {}<<<\n\
        Check your columns values.\n\
        (1)Make sure no empty entry in those columns.\n\
        (2)Make sure there is one and only one column is labeled as 100.'.format(csv_path))

def recode(df, recode_dict, label_name, new_col_names):
    
    """
    Create a new column called label. Fill this class column based on labels
    *Use check_exclusive() first to make sure there is one and only one column = 100.
    
    -args:
    df: a pandas dataframe
    recode_dict: a dictionary with {col1:[old_value,new_value], col2:[old_value, new_value]}
    label_name: a list of labels
    new_col_names:a list of desired column names
    return: a pandas dataframe with recode values
    """
    df = df.copy()
    
    # collapse sparse matrix into a list. For each row, the label with 100 will be selected.
    df_densemat = df[new_col_names].idxmax(axis=1)
    
    # create a column called "label", fill with the label list.
    df = df.assign(**{label_name:df_densemat})
    
    df.replace({label_name: recode_dict},inplace=True)
    
    return df


def combine_labelers(pd_list, by=["Point_ID","Clust"], label_name="label", fs=[]):
    
    """
    user 1's label will be like "label_1"; 
    user 2 is "label_2" etc...
    """
    label_name = "labeler"
    base = pd_list[0]
    
    # user 2's suffix is 2 (by setting enumerate idx start=2) 
    if len(pd_list) > 1:
        for idx, i in enumerate(pd_list[1:], start=2):
            # Extract the last part of the file path without the ".csv" extension
            file_name = os.path.splitext(os.path.basename(fs[idx - 1]))[0]
            base = pd.merge(base, i[[*by, label_name]], how='left', on=by, suffixes=(None, file_name))

    # Renaming the first user column name
    base = rename_cols(base, {label_name:os.path.splitext(os.path.basename(fs[0]))[0]})
    # Dropping label_name from the column names
    base.columns = [col.replace(label_name, '') for col in base.columns]
    return base

def process_csv(csv_path, rename_dict, recode_dict, new_col_names):

    """
    A csv processing pipeline. This function takes a single csv file
    and let it pass through a sequence of our pre-defined functions
    return: a pandas dataframe of the processed csv.
    """
    # Set columns to keep:
    # key_col and label_name are used for joining users's datasets,
    # columns in useful_col will not participate in joining
    # and come from the first user instead to avoid repetition.
    key_col = ["Point_ID", "Clust"]
    label_name = "labeler"
    useful_col = ["Lat", "Lon"]
    print("processing: {}".format(csv_path))
    df = load_csv(csv_path)
    df = rename_cols(df, rename_dict)
    #check_exclusive(df, csv_path, new_col_names)

    # if you want to combine Young and Mature, just recode both to be 1.
    df = recode(df, recode_dict, label_name, new_col_names)

    df = subset_cols(df, [*key_col,  *useful_col, label_name])

    return df


def match_CEO_projects(file_path):
    
    """    
    Compare the content of multiple CSV files and identify differences in the data.
    - file_path (list of str): A list of file paths to the CSV files to be compared.
    """

    def round_float(value):
        if isinstance(value, float):
            return round(value, 7)
        return value

    data_dicts = []
    file_names = []

    for csv_file in file_path:
        df = pd.read_csv(csv_file)
        df = df[['plot_id', 'center_lon', 'center_lat']].applymap(round_float)
        data_dict = df.to_dict('records')
        data_dicts.append(data_dict)
        file_names.append(os.path.basename(csv_file))

    match = True

    for i in range(1, len(data_dicts)):
        if data_dicts[0] != data_dicts[i]:
            match = False
            break

    if match:
        print("Samples are identical in all CSV files.")
    else:
        print("Samples must be the same in all CSV files.")
        for i in range(1, len(data_dicts)):
            differing_rows = []
            for j, (row1, row2) in enumerate(zip(data_dicts[0], data_dicts[i])):
                if row1 != row2:
                    differing_rows.append((j, row1, row2))
            if differing_rows:
                print(f"Differences found in file '{file_names[i]}':")
                for row_index, row1, row2 in differing_rows:
                    print(f"Row {row_index} -> {row1} != {row2}")

def select_columns(file_path):
  
    """
    Process a CSV file, rename columns, and create new column categories.

    Takes: file_path (list of str): A list of file paths to the CSV files to be
    processed.

    Returns: new_col_names (list of str): A list of new column names.
    """

    df = pd.read_csv(file_path[0])

    print("\nColumn Names and Indices:")
    for i, column in enumerate(df.columns):
        print(f"{i}: {column}")

    new_col_names_input = input(
        "Enter the names you want to represent presence and absence\n"
        " , separated by a comma (e.g. Presence, Absence): "
    )
    new_col_names = [name.strip() for name in new_col_names_input.split(',')]

    unsure_category = input("Do you want to include an 'Unsure' category? (y/n): ")
    if unsure_category.lower() == 'y':
        new_col_names.append("Unsure")

    presence_columns = []
    absence_columns = []
    not_sure_columns = []

    while True:
        column_indices_input = input(
            f"Enter column indices to change to '{new_col_names[0]}' \n"
            "(separate with commas): "
        )
        column_indices = [
            int(index.strip())
            for index in column_indices_input.rstrip(',').split(',')
        ]
        invalid_selection = False
        for col_index in column_indices:
            column_name = df.columns[col_index]
            invalid_values = df[column_name][~df[column_name].isin([0, 100])]
            if not invalid_values.empty:
                print(f"Warning: Invalid values found in column '{column_name}':\n"
                      f"{invalid_values.unique()}. Valid values are [0, 100].")
                invalid_selection = True
                break
        if not invalid_selection:
            presence_columns.extend(column_indices)
        if len(presence_columns) == len(column_indices):
            break

    while True:
        column_indices_input = input(
            f"Enter column indices to change to '{new_col_names[1]}' \n"
            "(separate with commas): "
        )
        column_indices = [
            int(index.strip())
            for index in column_indices_input.rstrip(',').split(',')
        ]
        invalid_selection = False
        for col_index in column_indices:
            column_name = df.columns[col_index]
            invalid_values = df[column_name][~df[column_name].isin([0, 100])]
            if not invalid_values.empty:
                print(f"Warning: Invalid values found in column '{column_name}':\n"
                      f"{invalid_values.unique()}. Valid values are [0, 100].")
                invalid_selection = True
                break
        if not invalid_selection:
            absence_columns.extend(column_indices)
        if len(absence_columns) == len(column_indices):
            break

    if len(new_col_names) == 3:
        while True:
            column_indices_input = input(
                f"Enter column indices to change to '{new_col_names[2]}' "
                "(separate with commas): "
            )
            column_indices = [
                int(index.strip())
                for index in column_indices_input.rstrip(',').split(',')
            ]
            invalid_selection = False
            for col_index in column_indices:
                column_name = df.columns[col_index]
                invalid_values = df[column_name][~df[column_name].isin([0, 100])]
                if not invalid_values.empty:
                    print(f"Warning: Invalid values found in column "
                          f"'{column_name}': {invalid_values.unique()}. "
                          "Valid values are [0, 100].")
                    invalid_selection = True
                    break
            if not invalid_selection:
                not_sure_columns.extend(column_indices)
            if len(not_sure_columns) == len(column_indices):
                break
    
    rename_dict = {
        "plot_id": "Point_ID",
        "pl_cluster": "Clust",
        "center_lat": "Lat",
        "center_lon": "Lon"
    }
    for i, column in enumerate(df.columns):
        if i in presence_columns:
            rename_dict[column] = new_col_names[0]
        elif i in absence_columns:
            rename_dict[column] = new_col_names[1]
        elif i in not_sure_columns:
            rename_dict[column] = new_col_names[2]

    print(rename_dict)
    print(new_col_names)
    
    return new_col_names