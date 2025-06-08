import os
import zipfile
import pandas as pd

def list_zip_files():
    input_folder = "files/input/"

    for filename in os.listdir(input_folder):
        if filename.endswith(".zip"):
            zip_path = os.path.join(input_folder, filename)
            with zipfile.ZipFile(zip_path, "r") as z:
                print(f"Archivos en {filename}:")
                print(z.namelist())

def read_and_print_csv_from_zip():
    input_folder = "files/input/"
    for filename in os.listdir(input_folder):
        if filename.endswith(".zip"):
            zip_path = os.path.join(input_folder, filename)
            with zipfile.ZipFile(zip_path, "r") as z:
                for csv_filename in z.namelist():
                    with z.open(csv_filename) as f:
                        df = pd.read_csv(f)
                        print(f"Primeras filas de {csv_filename} en {filename}:")
                        print(df.head())

if __name__ == "__main__":
    list_zip_files()
    read_and_print_csv_from_zip()