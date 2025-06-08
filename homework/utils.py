import os
import zipfile
import pandas as pd

def get_dataframes_from_zip(input_folder):
    """
    Load all CSV files from ZIP archives in the input folder.
    """
    all_dfs = []
    for filename in os.listdir(input_folder):
        if filename.endswith(".zip"):
            zip_path = os.path.join(input_folder, filename)
            with zipfile.ZipFile(zip_path, "r") as z:
                for csv_filename in z.namelist():
                    with z.open(csv_filename) as f:
                        df = pd.read_csv(f)
                        all_dfs.append(df)
    return all_dfs

def load_and_combine_dataframes(input_folder):
    """
    Load all CSV files and combine them into a single DataFrame.
    """
    all_dfs = get_dataframes_from_zip(input_folder)
    return pd.concat(all_dfs, ignore_index=True)

def select_columns(data, column_names):
    """
    Select specific columns from a DataFrame.
    """
    df = pd.DataFrame()
    for col in column_names:
        df[col] = data[col]
    return df

def apply_transformations(df, transformations):
    """
    Apply a series of transformations to specified columns in a DataFrame.
    Each transformation is a function that takes a Series and returns a transformed Series.
    The transformations are specified in a dictionary where keys are column names and values are transformation functions.
    """
    for col, func in transformations.items():
        df[col] = func(df[col])
    return df

def save_processed_data(output_folder, dataframes):
    """
    Save processed DataFrames to CSV files in the specified output folder.
    Each DataFrame is saved with its name as the filename.
    """
    create_directory(output_folder)
    for name, df in dataframes.items():
        filename = os.path.join(output_folder, f"{name}.csv")
        df.to_csv(filename, index=False)

def create_directory(path):
    """
    Create a directory if it does not exist, or clear it if it does.
    """
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                os.rmdir(file_path)


def generate_last_contact_date(df, month_map):
    """
    Generate a new column 'last_contact_date' by combining 'day' and 'month' with a fixed year.
    The 'day' is zero-padded to two digits, and the 'month' is mapped to a numeric string.
    """
    
    day_str = df["day"].astype(str).str.zfill(2)
    month_str = df["month"].str.lower().map(month_map)
    df["last_contact_date"] = "2022-" + month_str + "-" + day_str
    return df.drop(columns=["day", "month"])
