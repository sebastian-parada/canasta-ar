"""
ingestion/indec/fetch.py
------------------------
Descarga el CSV de Canasta Básica Alimentaria (CBA) y Canasta Básica Total (CBT)
desde datos.gob.ar y lo inserta en raw.indec_canasta_basica.

Dataset: https://datos.gob.ar/dataset/sspm-canasta-basica-alimentaria-total-ciudad-buenos-aires
CSV URL: actualizar si cambia — verificar en el link anterior
"""

import os
import io
import requests
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

# ----------------------------------------------------------------
# URL del CSV de la canasta básica (datos.gob.ar)
# Verificar que siga activa en: https://datos.gob.ar/dataset/sspm_59
# ----------------------------------------------------------------
CSV_URL = (
    "https://infra.datos.gob.ar/catalog/sspm/dataset/444/distribution/"
    "444.1/download/canastas-basicas-ciudad-de-buenos-aires.csv"
)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME", "canasta_ar"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"),
    )


def fetch_canasta_csv() -> pd.DataFrame:
    """
    Descarga el CSV de INDEC y retorna un DataFrame limpio.

    El CSV tiene columnas como:
    indice_tiempo | cba_adulto_equivalente | cbt_adulto_equivalente | ...

    Nota: los nombres exactos pueden variar — se imprime el header
    para que puedas ajustar si hay cambios.
    """
    logger.info(f"Descargando CSV INDEC desde: {CSV_URL}")

    response = requests.get(CSV_URL, timeout=60)
    if response.status_code != 200:
        logger.error(f"Error descargando CSV INDEC: {response.status_code}")
        response.raise_for_status()

    df = pd.read_csv(io.StringIO(response.text))

    logger.info(f"CSV descargado: {len(df)} filas")
    logger.info(f"Columnas encontradas: {df.columns.tolist()}")

    return df


def clean_canasta(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y normaliza el DataFrame del INDEC.
    Ajustar los nombres de columnas si el CSV cambia.
    """
    # Renombrar columnas a nombres estándar del proyecto
    # IMPORTANTE: si el script falla acá, imprimí df.columns.tolist()
    # y ajustá el mapeo
    column_map = {
        "indice_tiempo": "periodo",
        "canasta_basica_alimentaria": "cba_adulto",
        "canasta_basica_total": "cbt_adulto",
    }

    # Solo renombramos las que existen
    existing = {k: v for k, v in column_map.items() if k in df.columns}
    df = df.rename(columns=existing)

    # Convertir periodo a fecha (primer día del trimestre/mes)
    df["periodo"] = pd.to_datetime(df["periodo"], errors="coerce")
    df = df.dropna(subset=["periodo"])

    # Convertir valores numéricos
    for col in ["cba_adulto", "cbt_adulto"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Ordenar por fecha
    df = df.sort_values("periodo").reset_index(drop=True)

    logger.info(f"Datos limpios: {len(df)} filas | rango: {df['periodo'].min()} → {df['periodo'].max()}")

    return df


def insert_canasta(df: pd.DataFrame, conn) -> int:
    """
    Inserta el DataFrame en raw.indec_canasta_basica.
    ON CONFLICT DO NOTHING para evitar duplicados.
    """
    cols_needed = ["periodo", "cba_adulto", "cbt_adulto"]

    rows = []
    for _, row in df.iterrows():
        rows.append((
            row["periodo"].date(),
            row.get("cba_adulto"),
            row.get("cbt_adulto"),
        ))

    sql = """
        INSERT INTO raw.indec_canasta_basica
            (periodo, cba_adulto, cbt_adulto)
        VALUES %s
        ON CONFLICT (periodo) DO NOTHING
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
        inserted = cur.rowcount

    conn.commit()
    logger.success(f"INDEC: {inserted} filas insertadas en raw.indec_canasta_basica")
    return inserted


def run():
    """Entry point principal."""
    logger.info("=== INDEC Canasta Básica Ingestion ===")

    try:
        df_raw = fetch_canasta_csv()
        df_clean = clean_canasta(df_raw)

        conn = get_db_connection()
        try:
            insert_canasta(df_clean, conn)
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error en INDEC ingestion: {e}")
        raise


if __name__ == "__main__":
    run()
