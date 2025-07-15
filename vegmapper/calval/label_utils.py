import pandas as pd
import numpy as np
import re
import random
import pdb


#from google.colab import drive
from datetime import datetime as dt
from pylab import rcParams

import warnings
import os
pd.set_option('future.no_silent_downcasting', True)

def project_file_selector():
    """
    Function for interactive selection of CEO project files. Constrains choice
    to 1 or at least 3 files. 
    """
    try:
        while True:
            try:
                num_users = int(
                    input("Enter the number of CEO projects (must be either 1 \
                          or at least 3): ")
                )
                if num_users == 2 or num_users < 1:
                    print("The number of CEO projects must be either 1 or 3+. \
                          Please re-run and try again")
                    sys.exit(0)
                else:
                    break
            except ValueError:
                print("Invalid input. Please re-run and enter a valid number.")
                sys.exit(0)

        fs = []
        for i in range(num_users):
            while True:
                file_path = input(f"Enter the CSV file path & name for \
                                    project {i+1}: ")
                if not file_path:
                    print("File path/name cannot be empty.")
                else:
                    fs.append(file_path)
                    break
                
        return fs
                
    except SystemExit:
        pass  

# read datasets into list, keep the most important columns, and do some renaming
def load_csv(csv_path):

    """
    Load a single csv file into a pandas dataframe.
    - args:
    csv_path: the path (str) to the CSV file to be loaded
    - return:
    csv_data: a pandas DataFrame containing the data from the CSV file.
    """
    csv_data = pd.read_csv(csv_path, index_col=False)
    
    return csv_data


def subset_cols(df, col_list):

    """
    Keep the selected columns only.
    -args:
    df: a pandas dataframe
    col_list: list of selected columns
    - return: a pandas dataframe with only selected columns
    """
    
    df = df.copy()

    return df.loc[:, col_list]

def rename_cols(df, update_dict):

    """
    Update a pandas dataframe's column names use a dictionary. The function will
    raise a ValueError if the user asks to change a column name that does not
    exist.
    -args:
    df: a pandas dataframe  
    update_dict: a dictionary with {old_name: new_name}
    return: a pandas dataframe with updated column names
    """
    df = df.copy()
    
    if not set(update_dict.keys()).issubset(df.columns): 
        raise ValueError("One or multiple old name(s) do not exist in the\
                          dataframe.")
    
    df.rename(columns=update_dict, inplace=True)
    
    return df

def find_mode(df):
    
    """
    Create a concensus label called "mode_label" based on the most freqent label
    (mode) across labelers.
    When there is a complete disagreement among labelers, give -9999
    """
    
    df = df.copy()
    labels = df.filter(like='label')
    mode_label = labels.mode(axis=1)
    agreement_flag = mode_label.isnull().any(axis=1)
    
    df = df.assign(mode_label = -9999)
    df.mode_label.mask(agreement_flag, mode_label[0], axis=0, inplace=True)

    return df

def get_mode_occurence(row):

    """
    Get modal and the occurance of modal (a ratio) for each row.
    This function will be applied to each row of a pandas dataframe.
    -agrs:
    row: a row that only includes labeler's labels
    return two value
    """
    row = row.dropna()
    mode_values = row.mode(dropna=True).values
    if len(mode_values) > 0:
        mode = mode_values[0]
        occurrence = row.value_counts()[mode] / len(row)
        ct = len(row)

    else:
        mode = np.nan
        occurrence = 0.0

    return mode, occurrence, ct

# def get_mode_occurence(row):

#     """
#     Get modal and the occurance of modal (a ratio) for each row.
#     This function will be applied to each row of a pandas dataframe.
#     -agrs:
#     row: a row that only includes labeler's labels
#     return two value
#     """
#     mode_values = row.mode(dropna=True).values
#     if len(mode_values) > 0:
#         mode = mode_values[0]
#         occurrence = row.value_counts()[mode] / len(row)
#     else:
#         mode = np.nan
#         occurrence = 0.0

#     return mode, occurrence


def check_exclusive(fs, rename_dict):

    """
    Check and modify CSV files based on the sum of values in specified columns.

    This function iterates through a list of CSV files and checks the sum of 
    values in specified columns. If the sum of values in the columns is not 
    equal to 100 for any row, it identifies and removes the
    problematic rows from the related CSV file.

    - args:
    fs: A list of file paths to the CSV files to be processed.
    rename_dict: A dictionary containing column names.
    """
    
    col_names = list(rename_dict.keys())
    col_names = col_names[4:]
    problematic_rows = []

    for file_path in fs:
        df = pd.read_csv(file_path)
    
        for index, row in df.iterrows():
            row_sum = row[col_names].sum()
            if row_sum != 100:
                problematic_rows.append((file_path, index))

        if problematic_rows:
            file_name = os.path.basename(file_path)
            print(f"{len(problematic_rows)} problematic rows found in\
                   {file_name}:")

            problematic_mask = df.index.isin([row_index for (_, row_index) in\
                                               problematic_rows])

            for col in col_names:
                df.loc[problematic_mask, col] = np.nan
    return df

def recode(df, recode_dict, label_name, new_col_names):
    
    """
    Create a new column called label. Fill this class column based on labels
    *Use check_exclusive() first to make sure there is one and only 
    one column = 100.
    
    -args:
    df: a pandas dataframe
    recode_dict: a dictionary with {col1:[old_value,new_value], col2:[old_value,
    new_value]}
    
    return: a pandas dataframe with recode values
    """
    df = df.copy()
    
    # collapse sparse matrix into a list. For each row, the label with 100 will
    #  be selected.
    df_densemat = df[new_col_names].idxmax(axis=1, skipna=True)
    
    # create a column called "label", fill with the label list.
    df = df.assign(**{label_name:df_densemat})
    
    df.replace({label_name: recode_dict},inplace=True)
    
    return df

# def combine_labelers(pd_list, by=["Point_ID","Clust"], label_name="label",\
#                       fs=[]):
    
#     """
#     user 1's label will be like "label_1"; 
#     user 2 is "label_2" etc...
#     """
#     label_name = "labeler"
#     base = pd_list[0]
    
#     # user 2's suffix is 2 (by setting enumerate idx start=2) 
#     if len(pd_list) > 1:
#         for idx, i in enumerate(pd_list[1:], start=2):
#             # Extract the last part of the file path without the ".csv" 
#             # extension
#             file_name = os.path.splitext(os.path.basename(fs[idx - 1]))[0]
#             base = pd.merge(base, i[[*by, label_name]], how='left', on=by,\
#                              suffixes=(None, file_name))

#     # Renaming the first user column name
#     base = rename_cols(base, {label_name:os.path\
#                               .splitext(os.path.basename(fs[0]))[0]})
#     # Dropping label_name from the column names
#     base.columns = [col.replace(label_name, '') for col in base.columns]
#     return base

def combine_labelers(pd_list, by=["Point_ID","Clust"], label_name="label"):
    """
    user 1's label will be like "label_1"; 
    user 2 is "label_2" etc...
    """
    label_name = "labeler"
    base = pd_list[0]
    
    # user 2's suffix is 2 (by setting enumerate idx start=2) 
    if len(pd_list) > 1:
        for idx, i in enumerate(pd_list[1:], start=2):
            # Extract the last part of the file path without the ".csv" 
            # extension
            file_name = f"ceo-survey-user{idx}"
            # file_name = os.path.splitext(os.path.basename(fs[idx - 1]))[0]
            base = pd.merge(base, i[[*by, label_name]], how='left', on=by,\
                            suffixes=(None, file_name))

    # Renaming the first user column name
    base = rename_cols(
        base, 
        {label_name:"ceo-survey-user1"}
    )

    # Dropping label_name from the column names
    base.columns = [col.replace(label_name, '') for col in base.columns]
    return base

# def process_csv(csv_path, rename_dict, recode_dict, new_col_names):

#     """
#     A csv processing pipeline. This function takes a single csv file
#     and let it pass through a sequence of our pre-defined functions
#     return: a pandas dataframe of the processed csv.
#     """
#     # Set columns to keep:
#     # key_col and label_name are used for joining users's datasets,
#     # columns in useful_col will not participate in joining
#     # and come from the first user instead to avoid repetition.
#     key_col = ["Point_ID", "Clust"]
#     label_name = "labeler"
#     useful_col = ["Lat", "Lon"]
    
#     print("processing: {}".format(csv_path))
    
#     df = load_csv(csv_path)
#     df = check_exclusive([csv_path], rename_dict)
#     df = rename_cols(df, rename_dict)

#     # if you want to combine Young and Mature, just recode both to be 1.
#     df = recode(df, recode_dict, label_name, new_col_names)

#     df = subset_cols(df, [*key_col,  *useful_col, label_name])

#     return df

def process_csv(csv_path, rename_dict, recode_dict, new_col_names, 
                key_col=["Point_ID", "Clust"]):

    """
    A csv processing pipeline. This function takes a single csv file
    and let it pass through a sequence of our pre-defined functions
    return: a pandas dataframe of the processed csv.
    """
    # Set columns to keep:
    # key_col and label_name are used for joining users's datasets,
    # columns in useful_col will not participate in joining
    # and come from the first user instead to avoid repetition.
    label_name = "labeler"
    useful_col = ["Lat", "Lon"]
    
    print("processing: {}".format(csv_path))
    
    df = load_csv(csv_path)
    df = check_exclusive([csv_path], rename_dict)
    df = rename_cols(df, rename_dict)

    # if you want to combine Young and Mature, just recode both to be 1.
    df = recode(df, recode_dict, label_name, new_col_names)

    df = subset_cols(df, [*key_col,  *useful_col, label_name])
    return df

def match_ceo_projects(file_paths):
    """    
    Compare multiple CEO project CSV files for consistency of plot ids and 
    locations, and check if they have they same column names, if more than one 
    is given
    - file_paths (list of str): A list of file paths to the CSV files to be 
    compared.
    """

    def round_float(value):
        return round(value, 7) if isinstance(value, float) else value

    data_dicts = []
    file_names = []
    expected_column_names = None

    for i, file_path in enumerate(file_paths):
        if not os.path.isfile(file_path):
            print(f"File {file_path} does not exist.")
            return

        df = pd.read_csv(file_path)

        # Initialize expected column names from the first file
        if i == 0:
            expected_column_names = set(df.columns)
        else:
            # Check for column name consistency
            if set(df.columns) != expected_column_names:
                print(f"Column names in {file_path} are not the same.")
                return

        # Check for the presence of 'plotid' or 'plot_id'
        plot_column = 'plot_id' if 'plot_id' in df.columns else 'plotid'
        if plot_column not in df.columns:
            raise ValueError("Neither 'plotid' nor 'plot_id' column \
                             found in the CSV file.")

        # Round float values and convert to dictionary
        df = df[[plot_column, 'center_lon', 'center_lat']].map(round_float)
        data_dicts.append(df.to_dict('records'))
        file_names.append(os.path.basename(file_path))

    # Skip checks if there is only one file
    if len(data_dicts) <= 1:
        print("Only one file provided. No comparison needed.")
        return

    # Compare data across files
    if all(data_dicts[0] == data_dict for data_dict in data_dicts[1:]):
        print("Samples in all project files are identical.")
    else:
        print("Samples are not but must all be the same.")
        for i in range(1, len(data_dicts)):
            differing_rows = [(j, row1, row2) for j, (row1, row2) in 
                              enumerate(zip(data_dicts[0], data_dicts[i])) 
                              if row1 != row2]
            if differing_rows:
                print(f"Differences found in file '{file_names[i]}':")
                for row_index, row1, row2 in differing_rows:
                    print(f"Row {row_index} -> {row1} != {row2}")

def select_columns(file_path):
  
    """
    Process a CSV file, rename columns, and create new column categories.

    Takes: file_path (list of str): A list of file paths to the CSV files to be
    processed.

    Returns: 
    new_col_names (list of str): A list of new column names.
    rename_dict (dictionary of str): A dictionary which the keys are old column
    names and values are the new column names.
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

    unsure_category = input("Do you want to include an 'Unsure' category?\
                             (y/n): ")
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
                print(f"Warning: Invalid values found in column '{column_name}'\
                      :\n"
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
                print(f"Warning: Invalid values found in column '{column_name}'\
                      :\n"
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
                invalid_values = df[column_name][~df[column_name]\
                                                 .isin([0, 100])]
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
    
    cluster_name = "pl_strata_cat" if "pl_strata_cat" in df.columns \
        else "pl_cluster"
    plotid_name = "plotid" if "plotid" in df.columns else "plot_id"
    
    rename_dict = {
        plotid_name: "Point_ID",
        cluster_name: "Clust",
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
    
    return new_col_names, rename_dict
