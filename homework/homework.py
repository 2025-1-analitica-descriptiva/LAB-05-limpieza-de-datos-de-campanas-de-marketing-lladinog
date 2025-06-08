"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import os
import pandas as pd
from homework.utils import load_and_combine_dataframes, save_processed_data, select_columns, apply_transformations, generate_last_contact_date


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    input_folder = "files/input/"
    output_folder = "files/output/"

    # 1. Cargar data original combinada desde ZIP
    data = load_and_combine_dataframes(input_folder)

    #2. Seleccionar columnas relevantes para cada DataFrame
    client_columns = ["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]
    client_df = select_columns(data, client_columns)

    campaign_columns = ["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts",
                         "previous_outcome", "campaign_outcome", "day", "month"]
    campaign_df = select_columns(data, campaign_columns)

    economics_columns = ["client_id", "cons_price_idx", "euribor_three_months"]
    economics_df = select_columns(data, economics_columns)


    # 3. Procesar y transformar los DataFrames
    client_transforms = {
        "job": lambda x: x.str.replace(".", "", regex=False).str.replace("-", "_", regex=False),
        "education": lambda x: x.str.replace(".", "_", regex=False).replace("unknown", pd.NA),
        "credit_default": lambda x: (x == "yes").astype(int),
        "mortgage": lambda x: (x == "yes").astype(int)
    }
    client_df = apply_transformations(client_df, client_transforms)

    campaign_transforms = {
        "previous_outcome": lambda x: (x == "success").astype(int),
        "campaign_outcome": lambda x: (x == "yes").astype(int)
    }
    campaign_df = apply_transformations(campaign_df, campaign_transforms)

    month_map = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
        "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }
    campaign_df = generate_last_contact_date(campaign_df, month_map)


    # 4. Guardar resultados

    save_processed_data(output_folder, {
        "client": client_df,
        "campaign": campaign_df,
        "economics": economics_df
    })

if __name__ == "__main__":
    clean_campaign_data()
