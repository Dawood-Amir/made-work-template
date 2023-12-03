import pandas as pd
from sqlalchemy import create_engine , types
import os


csv_url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
df = pd.read_csv(csv_url, delimiter=';')

# Droping the "Status" column
df = df.drop(columns=["Status"])
# Convert "Laenge" and "Breite" columns to numeric, replacing commas with dots
df["Laenge"] = pd.to_numeric(df["Laenge"].str.replace(',', '.'), errors="coerce")
df["Breite"] = pd.to_numeric(df["Breite"].str.replace(',', '.'), errors="coerce")

df = df[df["Verkehr"].isin(["FV", "RV", "nur DPN"])]
df = df[(df["Laenge"].notnull()) & (df["Breite"].notnull())]
df = df[(df["Laenge"] >= -90) & (df["Laenge"] <= 90) & (df["Breite"] >= -90) & (df["Breite"] <= 90)]

# validity of IFOPT  values
def is_valid_ifopt(value):
    if isinstance(value, str):
        parts = value.split(":")
        if len(parts) >= 3 and all(part.isdigit() for part in parts[1:]):
            return True
    return False


df = df[df["IFOPT"].apply(is_valid_ifopt)]
df = df.dropna()

# Define SQLite types for each column
sqlite_types = {
    "EVA_NR": types.BIGINT(),
    "DS100": types.TEXT(),
    "IFOPT": types.TEXT(),
    "NAME": types.TEXT(),
    "Verkehr": types.TEXT(),
    "Laenge": types.FLOAT(),
    "Breite": types.FLOAT(),
    "Betreiber_Name": types.TEXT(),
    "Betreiber_Nr": types.INT()
}
#engine = create_engine("sqlite:///trainstops.sqlite")
script_directory = os.getcwd()
db_file_path = os.path.join(script_directory, "trainstops.sqlite")
engine = create_engine(f"sqlite:///{db_file_path}")# Write the DataFrame to the SQLite database

df.to_sql("trainstops", engine, index=False, if_exists="replace", dtype=sqlite_types)
 


#script_directory = os.path.dirname(os.path.abspath(__file__))
#db_file_path = os.path.join(script_directory, "trainstops.sqlite")
#engine = create_engine(f"sqlite:///{db_file_path}")# Write the DataFrame to the SQLite database
#df.to_sql("trainstops", engine, index=False, if_exists="replace")
print(f"Data is written to {db_file_path}")
print(f"created file")
