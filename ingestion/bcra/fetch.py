"""
ingestion/bcra/fetch.py
-----------------------
Descarga el tipo de cambio minorista desde la API pública del BCRA
y lo inserta en raw.bcra_tipo_cambio.

API doc: https://api.bcra.gob.ar
Endpoint usado: /estadisticas/v4.0/monetarias/{idVariable}   (antes 3.0, lo actualicé a 4.0)
Variable 4 = Tipo de cambio minorista ($ por USD)
"""

import os
import requests
import pandas as pd
from datetime import date, timedelta
from loguru import logger
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

# ----------------------------------------------------------------
# Configuración
# ----------------------------------------------------------------
BCRA_BASE_URL = "https://api.bcra.gob.ar"
VARIABLE_ID = 4          # Tipo de cambio minorista USD
VARIABLE_NOMBRE = "Tipo de cambio minorista ($ por USD)"
DIAS_HACIA_ATRAS = 30    # Cuántos días traer por defecto


def get_db_connection():
    """Retorna una conexión a PostgreSQL usando variables de entorno."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME", "canasta_ar"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"),
    )


def fetch_tipo_cambio(desde: date, hasta: date) -> list[dict]:
    """
    Llama a la API del BCRA y retorna una lista de dicts con
    fecha y valor del tipo de cambio.
    """
    url = f"{BCRA_BASE_URL}/estadisticas/v4.0/monetarias/{VARIABLE_ID}"
    params = {
        "desde": desde.strftime("%Y-%m-%d"),
        "hasta": hasta.strftime("%Y-%m-%d"),
        "limit": 1000,
    }

    logger.info(f"Consultando BCRA: {url} | desde={desde} hasta={hasta}")

    response = requests.get(url, params=params, timeout=30)

    if response.status_code != 200:
        logger.error(f"Error BCRA API: {response.status_code} - {response.text}")
        response.raise_for_status()

    data = response.json()

    # La API retorna: {"results": [{"fecha": "2024-01-01", "valor": 808.5}, ...]}
    resultados = data.get("results", [{}])[0].get("detalle", [])
    logger.info(f"BCRA: {len(resultados)} registros recibidos")

    return resultados


def insert_tipo_cambio(registros: list[dict], conn) -> int:
    """
    Inserta los registros en raw.bcra_tipo_cambio.
    Usa ON CONFLICT DO NOTHING para evitar duplicados.
    Retorna la cantidad de filas insertadas.
    """
    if not registros:
        logger.warning("No hay registros para insertar")
        return 0

    rows = [
        (r["fecha"], VARIABLE_NOMBRE, r["valor"])
        for r in registros
    ]

    sql = """
        INSERT INTO raw.bcra_tipo_cambio (fecha, variable, valor)
        VALUES %s
        ON CONFLICT (fecha, variable) DO NOTHING
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
        inserted = cur.rowcount

    conn.commit()
    logger.success(f"BCRA: {inserted} filas insertadas en raw.bcra_tipo_cambio")
    return inserted


def run(dias: int = DIAS_HACIA_ATRAS):
    """
    Entry point principal.
    Descarga los últimos N días y los carga en la DB.
    """
    hasta = date.today()
    desde = hasta - timedelta(days=dias)

    logger.info(f"=== BCRA Ingestion | {desde} → {hasta} ===")

    try:
        registros = fetch_tipo_cambio(desde, hasta)

        conn = get_db_connection()
        try:
            insert_tipo_cambio(registros, conn)
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Error en BCRA ingestion: {e}")
        raise


if __name__ == "__main__":
    run()
