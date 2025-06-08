import os
import zipfile
import pandas as pd

def clean_campaign_data():
    input_folder = "files/input/"
    output_folder = "files/output/"
    
    all_dfs = get_dataframes_from_zip(input_folder)

    # 2. Unir todos los DataFrames en uno solo
    data = pd.concat(all_dfs, ignore_index=True)

    # 3. Procesar client.csv
    client = pd.DataFrame()
    client["client_id"] = data["client_id"]
    client["age"] = data["age"]
    # job: "." por "" y "-" por "_"
    client["job"] = data["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
    client["marital"] = data["marital"]
    # education: "." por "_" y "unknown" por pd.NA
    client["education"] = data["education"].str.replace(".", "_", regex=False)
    client["education"] = client["education"].replace("unknown", pd.NA)
    # credit_default: "yes" a 1, otro valor a 0
    client["credit_default"] = (data["credit_default"] == "yes").astype(int)
    # mortage: "yes" a 1, otro valor a 0
    client["mortage"] = (data["mortage"] == "yes").astype(int)

    # 4. Procesar campaign.csv
    campaign = pd.DataFrame()
    campaign["client_id"] = data["client_id"]
    campaign["number_contacts"] = data["number_contacts"]
    campaign["contact_duration"] = data["contact_duration"]
    campaign["previous_campaing_contacts"] = data["previous_campaing_contacts"]
    # previous_outcome: "success" a 1, otro valor a 0
    campaign["previous_outcome"] = (data["previous_outcome"] == "success").astype(int)
    # campaign_outcome: "yes" a 1, otro valor a 0
    campaign["campaign_outcome"] = (data["campaign_outcome"] == "yes").astype(int)
    # last_contact_day: combinar day y month con año 2022
    month_map = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
        "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }
    day_str = data["day"].astype(str).str.zfill(2)
    month_str = data["month"].str.lower().map(month_map)
    campaign["last_contact_day"] = "2022-" + month_str + "-" + day_str

    # 5. Procesar economics.csv
    economics = pd.DataFrame()
    economics["client_id"] = data["client_id"]
    economics["const_price_idx"] = data["cons_price_idx"]
    economics["eurobor_three_months"] = data["euribor_three_months"]

    # 6. Guardar los archivos
    os.makedirs(output_folder, exist_ok=True)
    client.to_csv(os.path.join(output_folder, "client.csv"), index=False)
    campaign.to_csv(os.path.join(output_folder, "campaign.csv"), index=False)
    economics.to_csv(os.path.join(output_folder, "economics.csv"), index=False)

    return

def get_dataframes_from_zip(input_folder):
    all_dfs = []

    # 1. Leer todos los archivos .zip en la carpeta de entrada
    for filename in os.listdir(input_folder):
        if filename.endswith(".zip"):
            zip_path = os.path.join(input_folder, filename)
            with zipfile.ZipFile(zip_path, "r") as z:
                for csv_filename in z.namelist():
                    with z.open(csv_filename) as f:
                        df = pd.read_csv(f)
                        all_dfs.append(df)
    return all_dfs