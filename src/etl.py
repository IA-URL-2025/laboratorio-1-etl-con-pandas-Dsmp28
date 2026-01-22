import pandas as pd

def run_etl():
    """
    Implementa el proceso ETL.
    No cambies el nombre de esta función.
    """
    #Extraccion
    df = pd.read_csv("data/citas_clinica.csv")

    #Transformacion
    # Normalizacion de texto
    df["paciente"] = df["paciente"].str.title()
    df["especialidad"] = df["especialidad"].str.upper()

    # Convertir fecha_cita a tipo fecha y filtrar fechas inválidas
    df["fecha_cita"] = pd.to_datetime(df["fecha_cita"], format="%Y-%m-%d", errors="coerce")
    df = df.dropna(subset=["fecha_cita"])

    #Reglas de negocio: eliminar filas donde estado != "CONFIRMADA" o costo <= 0
    #La 'sub matriz' toma solo las filas que cumplen las condiciones 
    df = df[df["estado"] == "CONFIRMADA"]
    df = df[df["costo"] > 0]

    # Valores nulos: si telefono es nulo, reemplazar por "NO REGISTRA"
    df["telefono"] = df["telefono"].fillna("NO REGISTRA")

    #Cargar
    df.to_csv("data/output.csv", index=False)
    


if __name__ == "__main__":
    run_etl()