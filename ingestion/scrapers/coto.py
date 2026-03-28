"""
ingestion/scrapers/coto.py
--------------------------
Scraper de precios de Coto Digital.
Soporta dos modos:
- search: búsqueda por término de texto
- category: navegación por ID de categoría
"""

import os
import math
import time
import requests
from datetime import date
from loguru import logger
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

# ----------------------------------------------------------------
# Configuración de categorías
# ----------------------------------------------------------------
CATEGORIAS = [
    {"nombre": "arroz",           "modo": "search", "query": "arroz"},
    {"nombre": "fideos",          "modo": "search", "query": "fideos"},
    {"nombre": "aceite_girasol",          "modo": "search", "query": "aceite de girasol"},
        {"nombre": "aceite_oliva",          "modo": "search", "query": "aceite de oliva"},
    {"nombre": "polenta",         "modo": "search", "query": "polenta"},
    {"nombre": "sal",             "modo": "search", "query": "sal"},
    {"nombre": "manteca",         "modo": "search", "query": "manteca"},
    {"nombre": "harina",         "modo": "search", "query": "harina"},
    {"nombre": "yerba_mate",      "modo": "search", "query": "yerba mate"},
    {"nombre": "cafe",            "modo": "search", "query": "cafe"},
    {"nombre": "galletita",      "modo": "search", "query": "galletita"},
    {"nombre": "pan",             "modo": "search", "query": "pan"},
    {"nombre": "galletas",        "modo": "search", "query": "galletas"},
    {"nombre": "conservas_atun",  "modo": "search", "query": "atun"},
    {"nombre": "aceitunas",       "modo": "search", "query": "aceitunas"},
    {"nombre": "azucar",           "modo": "search", "query": "azucar"},
    {"nombre": "cereales",        "modo": "search", "query": "cereales"},
    {"nombre": "agua",            "modo": "search", "query": "agua"},
    {"nombre": "gaseosa",        "modo": "search", "query": "gaseosa"},
    {"nombre": "cerveza",         "modo": "search", "query": "cerveza"},
    {"nombre": "vino",            "modo": "search", "query": "vino"},
    {"nombre": "yogur",           "modo": "search", "query": "yogur"},
    {"nombre": "leche",           "modo": "search", "query": "leche"},
    {"nombre": "dulce_de_leche",  "modo": "search", "query": "dulce de leche"},
    {"nombre": "jamon_cocido",        "modo": "search", "query": "jamon cocido"},
    {"nombre": "salame",          "modo": "search", "query": "salame"},
    {"nombre": "salchichas",      "modo": "search", "query": "salchichas"},
    {"nombre": "pate",            "modo": "search", "query": "pate"},
    {"nombre": "quesos",          "modo": "search", "query": "queso"},
    {"nombre": "frutos_secos", "modo": "search", "query": "mani"},
    {"nombre": "frutos_secos", "modo": "search", "query": "almendras"},
    {"nombre": "frutos_secos", "modo": "search", "query": "nueces"},
    {"nombre": "frutos_secos", "modo": "search", "query": "castanas"},
    {"nombre": "frutos_secos", "modo": "search", "query": "pistacho"},
    {"nombre": "carnes_vacunas", "modo": "search", "query": "asado"},
    {"nombre": "carnes_vacunas", "modo": "search", "query": "nalga"},
    {"nombre": "carnes_vacunas", "modo": "search", "query": "bife"},
    {"nombre": "carnes_vacunas", "modo": "search", "query": "lomo"},
    {"nombre": "carnes_vacunas", "modo": "search", "query": "picada"},
    {"nombre": "carnes_vacunas", "modo": "search", "query": "matambre"},
    {"nombre": "carnes_vacunas", "modo": "search", "query": "peceto"},
    {"nombre": "carnes_vacunas", "modo": "search", "query": "vacío"},
    {"nombre": "carnes_vacunas", "modo": "search", "query": "cuadril"},
    {"nombre": "carnes_vacunas", "modo": "search", "query": "falda"},
    {"nombre": "carnes_vacunas", "modo": "search", "query": "marucha"},
    {"nombre": "carnes_vacunas", "modo": "search", "query": "roast beef"},
    {"nombre": "carnes_vacunas", "modo": "search", "query": "entraña"},
    {"nombre": "carnes_vacunas", "modo": "search", "query": "carnaza"},
    {"nombre": "carnes_vacunas", "modo": "search", "query": "cuadrada"},
    {"nombre": "carnes_vacunas", "modo": "search", "query": "osobuco"},
    {"nombre": "cerdo",          "modo": "search", "query": "cerdo"},
    {"nombre": "pollo",           "modo": "search", "query": "pollo"},
    {"nombre": "pescado",         "modo": "search", "query": "pescado"},
    {"nombre": "salmon",          "modo": "search", "query": "salmon"},
    {"nombre": "huevos",          "modo": "search", "query": "huevos"},
      {"nombre": "huevos",          "modo": "search", "query": "huevo"},
    {"nombre": "pastas_frescas",  "modo": "search", "query": "pastas frescas"},
    {"nombre": "pastas_rellenas", "modo": "search", "query": "pastas rellenas"},
    {"nombre": "untables",        "modo": "search", "query": "untables"},
    {"nombre": "jamon_crudo",     "modo": "search", "query": "jamon crudo"},
    {"nombre": "verduras", "modo": "search", "query": "papa"},
    {"nombre": "verduras", "modo": "search", "query": "tomate"},
    {"nombre": "verduras", "modo": "search", "query": "lechuga"},
    {"nombre": "verduras", "modo": "search", "query": "zanahoria"},
    {"nombre": "verduras", "modo": "search", "query": "rucula"},
    {"nombre": "verduras", "modo": "search", "query": "acelga"},
    {"nombre": "verduras", "modo": "search", "query": "choclo"},
    {"nombre": "verduras", "modo": "search", "query": "cebolla"},
    {"nombre": "verduras", "modo": "search", "query": "espinaca"},
    {"nombre": "verduras", "modo": "search", "query": "zapallo"},
    {"nombre": "verduras", "modo": "search", "query": "pepino"},
    {"nombre": "verduras", "modo": "search", "query": "pimiento"},
    {"nombre": "verduras", "modo": "search", "query": "brocoli"},
    {"nombre": "verduras", "modo": "search", "query": "kale"},
    {"nombre": "verduras", "modo": "search", "query": "rabanito"},
    {"nombre": "verduras", "modo": "search", "query": "palta"},
    {"nombre": "frutas", "modo": "search", "query": "manzana"},
    {"nombre": "frutas", "modo": "search", "query": "banana"},
    {"nombre": "frutas", "modo": "search", "query": "naranja"},
    {"nombre": "frutas", "modo": "search", "query": "mandarina"},
    {"nombre": "frutas", "modo": "search", "query": "pera"},
    {"nombre": "frutas", "modo": "search", "query": "durazno"},
    {"nombre": "frutas", "modo": "search", "query": "uvas"},
    {"nombre": "frutas", "modo": "search", "query": "limon"},
    {"nombre": "frutas", "modo": "search", "query": "melon"},
    {"nombre": "frutas", "modo": "search", "query": "sandia"},
    {"nombre": "frutas", "modo": "search", "query": "anana"},
    {"nombre": "frutas", "modo": "search", "query": "frutillas"},
    {"nombre": "frutas", "modo": "search", "query": "cerezas"},
    {"nombre": "frutas", "modo": "search", "query": "kiwi"},
    {"nombre": "frutas", "modo": "search", "query": "arandano"},
    {"nombre": "frutas", "modo": "search", "query": "tomate cherry"},
]

# Parámetros fijos de la API
API_BASE     = "https://api.coto.com.ar/api/v1/ms-digital-sitio-bff-web/api/v1/products"
API_KEY      = "key_r6xzz4IAoTWcipni"
SUCURSAL     = "200"   # CABA
POR_PAGINA   = 24
DELAY        = 1.0     # segundos entre requests para no saturar la API

# ----------------------------------------------------------------
# Construcción de URLs
# ----------------------------------------------------------------
def build_url(categoria: dict, page: int = 1) -> str:
    query = categoria["query"].replace(" ", "%20")
    params = (
        f"key={API_KEY}"
        f"&num_results_per_page={POR_PAGINA}"
        f"&page={page}"
        f"&pre_filter_expression=%7B%22name%22:%22store_availability%22,"
        f"%22value%22:%22{SUCURSAL}%22%7D"
    )
    return f"{API_BASE}/search/{query}?{params}"
# ----------------------------------------------------------------
# Fetch de productos de una categoría
# ----------------------------------------------------------------
def fetch_categoria(categoria: dict) -> list[dict]:
    """
    Trae todos los productos de una categoría manejando paginación.
    Devuelve una lista de productos crudos del JSON de la API.
    """
    todos_los_productos = []
    page = 1

    while True:
        url = build_url(categoria, page)
        logger.info(f"  Página {page} → {url[:80]}...")

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.error(f"  Error en página {page}: {e}")
            break

        # Extraer resultados
        resultados = data.get("response", {}).get("results", [])
        total = data.get("response", {}).get("total_num_results", 0)

        if not resultados:
            break

        todos_los_productos.extend(resultados)
        logger.info(f"  {len(todos_los_productos)}/{total} productos obtenidos")

        # Verificar si hay más páginas
        total_paginas = math.ceil(total / POR_PAGINA)
        if page >= total_paginas:
            break

        page += 1
        time.sleep(DELAY)

    return todos_los_productos  

# ----------------------------------------------------------------
# Extracción de campos de un producto crudo
# ----------------------------------------------------------------
def extraer_producto(producto: dict, categoria: str) -> dict | None:
    """
    Toma un producto crudo del JSON de la API y extrae
    solo los campos que necesitamos guardar.
    Retorna None si el producto no tiene precio válido.
    """
    data = producto.get("data", {})

    # Precio de lista — campo principal
    precio_lista = data.get("product_list_price")
    if not precio_lista or precio_lista <= 0:
        return None

    precio_promo = None


    return {
        "fecha":           date.today(),
        "supermercado":    "coto",
        "categoria":       categoria,
        "id_producto":     data.get("id"),
        "nombre_producto": data.get("sku_display_name"),
        "marca":           data.get("product_brand"),
        "precio":          precio_lista,
        "precio_promo":    precio_promo,
        "unidad_medida":   data.get("product_unit_of_measure"),  # UNI, KG, etc.
        "contenido":       data.get("product_format_quantity"),   # 1, 0.5, etc.
        "unidad_contenido":data.get("product_format"),            # Kilogramo, Litro, etc.
        "url_producto":    f"https://www.cotodigital.com.ar/sitios/cdigi/productos/{data.get('url', '')}",
    }

# ----------------------------------------------------------------
# Inserción en base de datos
# ----------------------------------------------------------------
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME", "canasta_ar"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"),
    )


def insert_productos(productos: list[dict], conn) -> int:
    """
    Inserta los productos en raw.scraper_precios.
    ON CONFLICT DO NOTHING para evitar duplicados
    del mismo producto en el mismo día.
    """
    if not productos:
        return 0

    rows = [
        (
            p["fecha"],
            p["supermercado"],
            p["categoria"],
            p["id_producto"],
            p["nombre_producto"],
            p["marca"],
            p["precio"],
            p["precio_promo"],
            p["unidad_medida"],
            p["contenido"],
            p["unidad_contenido"],
            p["url_producto"],
        )
        for p in productos
    ]

    sql = """
        INSERT INTO raw.scraper_precios (
            fecha, supermercado, categoria, id_producto,
            nombre_producto, marca, precio, precio_promo,
            unidad_medida, contenido, unidad_contenido, url_producto
        )
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
        inserted = cur.rowcount

    conn.commit()
    return inserted


# ----------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------
def run():
    logger.info("=== Coto Scraper ===")
    logger.info(f"Categorías a procesar: {len(CATEGORIAS)}")

    conn = get_db_connection()

    try:
        total_insertados = 0

        for cat in CATEGORIAS:
            logger.info(f"→ Procesando: {cat['nombre']}")

            productos_crudos = fetch_categoria(cat)
            logger.info(f"  {len(productos_crudos)} productos obtenidos")

            productos_limpios = []
            for p in productos_crudos:
                extraido = extraer_producto(p, cat["nombre"])
                if extraido:
                    productos_limpios.append(extraido)

            insertados = insert_productos(productos_limpios, conn)
            total_insertados += insertados
            logger.success(f"  {insertados} insertados en DB")

            time.sleep(DELAY)

    finally:
        conn.close()

    logger.success(f"=== Coto Scraper finalizado | {total_insertados} registros totales ===")


if __name__ == "__main__":
    run()