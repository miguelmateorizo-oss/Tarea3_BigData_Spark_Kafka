import pandas as pd
from pathlib import Path

carpeta = Path(__file__).resolve().parent

archivo_excel = carpeta / "Online Retail.xlsx"
archivo_csv = carpeta / "online_retail.csv"

print("Leyendo archivo Excel...")

datos = pd.read_excel(archivo_excel, engine="openpyxl")

print("Total de registros:", len(datos))
print("Columnas:", list(datos.columns))

datos.to_csv(archivo_csv, index=False, encoding="utf-8")

print("Archivo CSV creado correctamente.")