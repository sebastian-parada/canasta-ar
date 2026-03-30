"""
ingestion/indec/fetch.py
------------------------
Descarga el XLS de Canasta Básica Alimentaria (CBA) y Canasta Básica Total (CBT)
directamente desde el INDEC y lo inserta en raw.indec_canasta_basica.

Fuente: https://www.indec.gob.ar/indec/web/Nivel4-Tema-4-43-149
XLS URL: https://www.indec.gob.ar/ftp/cuadros/sociedad/serie_cba_cbt.xls

Los valores son POR ADULTO EQUIVALENTE (varón adulto 30-60 años, actividad moderada).
Cobertura: abril 2016 en adelante. Región: Gran Buenos Aires.
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
# URL del XLS del INDEC — valores por adulto equivalente
# Cobertura: abril 2016 en adelante, GBA, mensual
# ----------------------------------------------------------------
XLS_URL = "https://www.indec.gob.ar/ftp/cuadros/sociedad/serie_cba_cbt.xls"


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME", "canasta_ar"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"),
    )


def fetch_canasta_xls() -> pd.DataFrame:
    """
    Descarga el XLS del INDEC y retorna un DataFrame limpio.
    Usa la hoja 'CBA-CBT' que contiene los valores por adulto equivalente.
    """
    logger.info(f"Descargando XLS INDEC desde: {XLS_URL}")

    response = requests.get(XLS_URL, timeout=60)
    if response.status_code != 200:
        logger.error(f"Error descargando XLS INDEC: {response.status_code}")
        response.raise_for_status()

    # Leer la hoja CBA-CBT saltando las filas de encabezado
    df = pd.read_excel(
        io.BytesIO(response.content),
        sheet_name="CBA-CBT",
        skiprows=5,        # saltar encabezados decorativos
        usecols=[0, 1, 3], # col 0=fecha, col 1=CBA, col 3=CBT
        header=None,
        names=["periodo", "cba_adulto", "cbt_adulto"],
    )

    logger.info(f"XLS descargado: {len(df)} filas crudas")

    return df


def clean_canasta(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y normaliza el DataFrame del INDEC.
    """
    # Eliminar filas sin fecha o sin valores
    df = df.dropna(subset=["periodo"])

    # Convertir periodo a fecha
    df["periodo"] = pd.to_datetime(df["periodo"], errors="coerce")
    df = df.dropna(subset=["periodo"])

    # Eliminar filas que no sean fechas reales (títulos, notas)
    df = df[df["periodo"].dt.year >= 2016]

    # Convertir valores numéricos
    df["cba_adulto"] = pd.to_numeric(df["cba_adulto"], errors="coerce")
    df["cbt_adulto"] = pd.to_numeric(df["cbt_adulto"], errors="coerce")

    # Eliminar filas sin valores
    df = df.dropna(subset=["cba_adulto", "cbt_adulto"])

    # Agregar fuente
    df["fuente"] = "INDEC - GBA - Adulto equivalente"

    # Ordenar por fecha
    df = df.sort_values("periodo").reset_index(drop=True)

    logger.info(f"Datos limpios: {len(df)} filas | rango: {df['periodo'].min().date()} → {df['periodo'].max().date()}")
    logger.info(f"Último valor — CBA: ${df['cba_adulto'].iloc[-1]:,.2f} | CBT: ${df['cbt_adulto'].iloc[-1]:,.2f}")

    return df


def insert_canasta(df: pd.DataFrame, conn) -> int:
    """
    Inserta el DataFrame en raw.indec_canasta_basica.
    ON CONFLICT DO UPDATE para actualizar valores si cambian.
    """
    rows = [
        (
            row["periodo"].date(),
            row["cba_adulto"],
            row["cbt_adulto"],
            row["fuente"],
        )
        for _, row in df.iterrows()
    ]

    sql = """
        INSERT INTO raw.indec_canasta_basica
            (periodo, cba_adulto, cbt_adulto, fuente)
        VALUES %s
        ON CONFLICT (periodo) DO UPDATE SET
            cba_adulto = EXCLUDED.cba_adulto,
            cbt_adulto = EXCLUDED.cbt_adulto,
            fuente     = EXCLUDED.fuente
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
        inserted = cur.rowcount

    conn.commit()
    logger.success(f"INDEC: {inserted} filas insertadas/actualizadas en raw.indec_canasta_basica")
    return inserted


def run():
    """Entry point principal."""
    logger.info("=== INDEC Canasta Básica Ingestion ===")
    logger.info("Fuente: INDEC GBA — valores por adulto equivalente")

    try:
        df_raw = fetch_canasta_xls()
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