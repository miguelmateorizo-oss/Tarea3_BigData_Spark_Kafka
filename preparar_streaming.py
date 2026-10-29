
from pathlib import Path
import pandas as pd

carpeta = Path(__file__).resolve().parent
origen = carpeta / "online_retail.csv"
destino = carpeta / "streaming_entrada"

destino.mkdir(exist_ok=True)

datos = pd.read_csv(origen, nrows=100)

# Dividir los datos en 5 lotes de 20 registros
for i in range(5):
    lote = datos.iloc[i * 20:(i + 1) * 20]
    archivo = destino / f"lote_{i + 1}.csv"
    lote.to_csv(archivo, index=False)

print("Se prepararon 5 lotes de 20 registros.")
print("Carpeta:", destino)
