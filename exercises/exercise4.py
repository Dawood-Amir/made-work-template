import pandas as pd
import os
import zipfile
import urllib.request
from sqlalchemy import create_engine, types


download_url = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
zip_filename = "data.zip"
output_folder = os.getcwd()

urllib.request.urlretrieve(download_url, zip_filename)


with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
    zip_ref.extractall(output_folder)

csv_filepath = os.path.join(output_folder, 'data.csv')

df = pd.read_csv(csv_filepath, delimiter=';', decimal=',', usecols=["Geraet"])

# Save cleaned data to CSV 
clean_csv_path = os.path.join(output_folder, "finalResult.csv")
df["Geraet"].to_csv(clean_csv_path, sep=",")

read_columns = ["Geraet", "Hersteller", "Model", "Monat",
                   "Temperatur in °C (DWD)", "Latitude (WGS84)",
                   "Longitude (WGS84)", "Verschleierung (m)",
                   "Aufenthaltsdauer im Freien (ms)",
                   "Batterietemperatur in °C",
                   "Geraet aktiv", "extra"]

df = pd.read_csv(clean_csv_path)
df.columns = read_columns


#Reshape data
df.drop(columns=["extra", "Latitude (WGS84)",
                 "Longitude (WGS84)", "Verschleierung (m)",
                 "Aufenthaltsdauer im Freien (ms)"], inplace=True)

column_mapping = {"Temperatur in °C (DWD)": "Temperatur",
                   "Batterietemperatur in °C": "Batterietemperatur"}
df.rename(columns=column_mapping, inplace=True)


df['Temperatur'] = df['Temperatur'].apply(lambda x: (x * 9/5) + 32)
df['Batterietemperatur'] = df['Batterietemperatur'].apply(lambda x: (x * 9/5) + 32)


sqlite_types = {
    "Geraet": types.BIGINT(),
    "Hersteller": types.TEXT(),
    "Model": types.TEXT(),
    "Monat": types.BIGINT(),
    "Temperatur": types.FLOAT(),
    "Batterietemperatur": types.FLOAT(),
    "Geraet aktiv": types.TEXT()
}


pth = os.getcwd()
db_filepath = os.path.join(pth, "temperatures.sqlite")

engine = create_engine(f"sqlite:///{db_filepath}")
df.to_sql("temperatures", engine, index=False, if_exists="replace", dtype=sqlite_types)

print(f"Data is written to {db_filepath}")
