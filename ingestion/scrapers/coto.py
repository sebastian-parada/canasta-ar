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
    # --- ALMACÉN ---
    {"nombre": "arroz",           "modo": "search", "query": "arroz largo"},
    {"nombre": "arroz",           "modo": "search", "query": "arroz parboil"},
    {"nombre": "arroz",           "modo": "search", "query": "arroz integral"},
    {"nombre": "fideos",          "modo": "search", "query": "fideos secos"},
    {"nombre": "fideos",          "modo": "search", "query": "fideos tallarin"},
    {"nombre": "fideos",          "modo": "search", "query": "fideos spaghetti"},
    {"nombre": "aceite_girasol",  "modo": "search", "query": "aceite girasol"},
    {"nombre": "aceite_oliva",    "modo": "search", "query": "aceite oliva"},
    {"nombre": "polenta",         "modo": "search", "query": "polenta"},
    {"nombre": "sal",             "modo": "search", "query": "sal fina"},
    {"nombre": "sal",             "modo": "search", "query": "sal gruesa"},
    {"nombre": "manteca",         "modo": "search", "query": "manteca"},
    {"nombre": "harina",          "modo": "search", "query": "harina 0000"},
    {"nombre": "harina",          "modo": "search", "query": "harina 000"},
    {"nombre": "harina",          "modo": "search", "query": "harina integral"},
    {"nombre": "azucar",          "modo": "search", "query": "azucar blanco"},
    {"nombre": "azucar",          "modo": "search", "query": "azucar rubio"},

    # --- INFUSIONES ---
    {"nombre": "yerba_mate",      "modo": "search", "query": "yerba mate"},
    {"nombre": "yerba_mate",      "modo": "search", "query": "yerba"},
    {"nombre": "cafe",            "modo": "search", "query": "cafe molido"},
    {"nombre": "cafe",            "modo": "search", "query": "cafe instantaneo"},
    {"nombre": "cafe",            "modo": "search", "query": "cafe torrado"},

    # --- PANIFICADOS ---
    {"nombre": "pan",             "modo": "search", "query": "pan lactal"},
    {"nombre": "pan",             "modo": "search", "query": "pan integral"},
    {"nombre": "galletitas",       "modo": "search", "query": "galletita dulce"},
    {"nombre": "galletitas",       "modo": "search", "query": "galletita salada"},
    {"nombre": "galletitas", "modo": "search", "query": "bizcochitos"},
    {"nombre": "galletitas", "modo": "search", "query": "vainillas"},

    # --- CONSERVAS Y ENLATADOS ---
    {"nombre": "conservas_atun",  "modo": "search", "query": "atun en aceite"},
    {"nombre": "conservas_atun",  "modo": "search", "query": "atun al natural"},
    {"nombre": "aceitunas",       "modo": "search", "query": "aceitunas verdes"},
    {"nombre": "aceitunas",       "modo": "search", "query": "aceitunas negras"},

    # --- CEREALES Y LEGUMBRES ---
    {"nombre": "cereales",        "modo": "search", "query": "cereales desayuno"},
    {"nombre": "legumbres",       "modo": "search", "query": "lentejas"},
    {"nombre": "legumbres",       "modo": "search", "query": "porotos"},

    # --- BEBIDAS ---
    {"nombre": "agua",            "modo": "search", "query": "agua mineral"},
    {"nombre": "agua",            "modo": "search", "query": "agua saborizada"},
    {"nombre": "gaseosa",         "modo": "search", "query": "gaseosa cola"},
    {"nombre": "gaseosa",         "modo": "search", "query": "gaseosa naranja"},
    {"nombre": "gaseosa",         "modo": "search", "query": "gaseosa lima limon"},
    {"nombre": "cerveza",         "modo": "search", "query": "cerveza rubia"},
    {"nombre": "cerveza",         "modo": "search", "query": "cerveza negra"},
    {"nombre": "vino",            "modo": "search", "query": "vino tinto"},
    {"nombre": "vino",            "modo": "search", "query": "vino blanco"},
    {"nombre": "vino",            "modo": "search", "query": "vino rose"},

    # --- LÁCTEOS ---
    {"nombre": "leche",           "modo": "search", "query": "leche entera"},
    {"nombre": "leche",           "modo": "search", "query": "leche descremada"},
    {"nombre": "yogur",           "modo": "search", "query": "yogur entero"},
    {"nombre": "yogur",           "modo": "search", "query": "yogur descremado"},
    {"nombre": "dulce_de_leche",  "modo": "search", "query": "dulce de leche"},

    # --- QUESOS (por tipo para reducir ruido) ---
    {"nombre": "quesos",          "modo": "search", "query": "queso cremoso"},
    {"nombre": "quesos",          "modo": "search", "query": "queso mozzarella"},
    {"nombre": "quesos",          "modo": "search", "query": "queso port salut"},
    {"nombre": "quesos",          "modo": "search", "query": "queso danbo"},
    {"nombre": "quesos",          "modo": "search", "query": "queso tybo"},
    {"nombre": "quesos",          "modo": "search", "query": "queso gouda"},
    {"nombre": "quesos",          "modo": "search", "query": "queso brie"},
    {"nombre": "quesos",          "modo": "search", "query": "queso camembert"},
    {"nombre": "quesos",          "modo": "search", "query": "queso sardo"},
    {"nombre": "quesos",          "modo": "search", "query": "queso reggianito"},
    {"nombre": "quesos",          "modo": "search", "query": "queso crema"},
    {"nombre": "quesos",          "modo": "search", "query": "queso azul"},
    {"nombre": "quesos",          "modo": "search", "query": "muzarella"},
    {"nombre": "quesos",          "modo": "search", "query": "ricotta"},

    # --- FIAMBRES ---
    {"nombre": "jamon_cocido",    "modo": "search", "query": "jamon cocido feteado"},
    {"nombre": "jamon_cocido",    "modo": "search", "query": "jamon cocido por kg"},
    {"nombre": "jamon_crudo",     "modo": "search", "query": "jamon crudo"},
    {"nombre": "salame",          "modo": "search", "query": "salame milan"},
    {"nombre": "salame",          "modo": "search", "query": "salamin picado"},
    {"nombre": "salchichas",      "modo": "search", "query": "salchicha"},
    {"nombre": "pate",            "modo": "search", "query": "pate"},
   
    # --- UNTABLES ---
    {"nombre": "untables", "modo": "search", "query": "nutella"},
    {"nombre": "untables", "modo": "search", "query": "pasta de mani"},
    {"nombre": "untables", "modo": "search", "query": "mermelada"},
    {"nombre": "untables", "modo": "search", "query": "dulce de leche"},
    {"nombre": "untables", "modo": "search", "query": "queso untable"},

    # --- CARNES VACUNAS (por corte específico) ---
    {"nombre": "carnes_vacunas",  "modo": "search", "query": "asado vacuno"},
    {"nombre": "carnes_vacunas",  "modo": "search", "query": "nalga vacuna"},
    {"nombre": "carnes_vacunas",  "modo": "search", "query": "bife chorizo vacuno"},
    {"nombre": "carnes_vacunas",  "modo": "search", "query": "bife angosto"},
    {"nombre": "carnes_vacunas",  "modo": "search", "query": "lomo vacuno"},
    {"nombre": "carnes_vacunas",  "modo": "search", "query": "picada vacuna"},
    {"nombre": "carnes_vacunas",  "modo": "search", "query": "matambre vacuno"},
    {"nombre": "carnes_vacunas",  "modo": "search", "query": "peceto vacuno"},
    {"nombre": "carnes_vacunas",  "modo": "search", "query": "vacio vacuno"},
    {"nombre": "carnes_vacunas",  "modo": "search", "query": "cuadril vacuno"},
    {"nombre": "carnes_vacunas",  "modo": "search", "query": "marucha vacuna"},
    {"nombre": "carnes_vacunas",  "modo": "search", "query": "roast beef vacuno"},
    {"nombre": "carnes_vacunas",  "modo": "search", "query": "entraña vacuna"},
    {"nombre": "carnes_vacunas",  "modo": "search", "query": "carnaza vacuna"},
    {"nombre": "carnes_vacunas",  "modo": "search", "query": "osobuco vacuno"},

    # --- CERDO ---
    {"nombre": "cerdo",           "modo": "search", "query": "carre cerdo"},
    {"nombre": "cerdo",           "modo": "search", "query": "bondiola cerdo"},
    {"nombre": "cerdo",           "modo": "search", "query": "pechito cerdo"},
    {"nombre": "cerdo",           "modo": "search", "query": "picada cerdo"},
    {"nombre": "cerdo",           "modo": "search", "query": "matambre cerdo"},

    # --- POLLO ---
    {"nombre": "pollo",           "modo": "search", "query": "pollo entero"},
    {"nombre": "pollo",           "modo": "search", "query": "pechuga pollo"},
    {"nombre": "pollo",           "modo": "search", "query": "muslo pollo"},
    {"nombre": "pollo",           "modo": "search", "query": "pata pollo"},
    {"nombre": "pollo",           "modo": "search", "query": "alitas pollo"},

    # --- PESCADO ---
    {"nombre": "pescado",         "modo": "search", "query": "merluza fresca"},
    {"nombre": "pescado",         "modo": "search", "query": "merluza congelada"},
    {"nombre": "pescado",         "modo": "search", "query": "salmon fresco"},
    {"nombre": "pescado",         "modo": "search", "query": "salmon congelado"},

    # --- HUEVOS ---
    {"nombre": "huevos",          "modo": "search", "query": "huevo blanco"},
    {"nombre": "huevos",          "modo": "search", "query": "huevo color"},

    # --- PASTAS FRESCAS ---
    {"nombre": "pastas_frescas",  "modo": "search", "query": "fideos frescos"},
    {"nombre": "pastas_frescas",  "modo": "search", "query": "ñoquis frescos"},
    {"nombre": "pastas_rellenas", "modo": "search", "query": "ravioles frescos"},
    {"nombre": "pastas_rellenas", "modo": "search", "query": "sorrentinos"},
    {"nombre": "pastas_rellenas", "modo": "search", "query": "capellettis"},

    # --- FRUTOS SECOS ---
    {"nombre": "frutos_secos",    "modo": "search", "query": "mani pelado"},
    {"nombre": "frutos_secos",    "modo": "search", "query": "almendras"},
    {"nombre": "frutos_secos",    "modo": "search", "query": "nueces"},
    {"nombre": "frutos_secos",    "modo": "search", "query": "castanas"},
    {"nombre": "frutos_secos",    "modo": "search", "query": "pistacho"},

    # --- VERDURAS (por producto) ---
    {"nombre": "verduras",        "modo": "search", "query": "papa"},
    {"nombre": "verduras",        "modo": "search", "query": "tomate"},
    {"nombre": "verduras",        "modo": "search", "query": "lechuga"},
    {"nombre": "verduras",        "modo": "search", "query": "zanahoria"},
    {"nombre": "verduras",        "modo": "search", "query": "rucula"},
    {"nombre": "verduras",        "modo": "search", "query": "acelga"},
    {"nombre": "verduras",        "modo": "search", "query": "choclo"},
    {"nombre": "verduras",        "modo": "search", "query": "cebolla"},
    {"nombre": "verduras",        "modo": "search", "query": "espinaca"},
    {"nombre": "verduras",        "modo": "search", "query": "zapallo"},
    {"nombre": "verduras",        "modo": "search", "query": "pepino"},
    {"nombre": "verduras",        "modo": "search", "query": "pimiento"},
    {"nombre": "verduras",        "modo": "search", "query": "brocoli"},
    {"nombre": "verduras",        "modo": "search", "query": "kale"},
    {"nombre": "verduras",        "modo": "search", "query": "rabanito"},
    {"nombre": "verduras",        "modo": "search", "query": "palta"},

    # --- FRUTAS (por producto) ---
    {"nombre": "frutas",          "modo": "search", "query": "manzana"},
    {"nombre": "frutas",          "modo": "search", "query": "banana"},
    {"nombre": "frutas",          "modo": "search", "query": "mandarina"},
    {"nombre": "frutas",          "modo": "search", "query": "pera"},
    {"nombre": "frutas",          "modo": "search", "query": "durazno"},
    {"nombre": "frutas",          "modo": "search", "query": "uvas"},
    {"nombre": "frutas",          "modo": "search", "query": "limon"},
    {"nombre": "frutas",          "modo": "search", "query": "melon"},
    {"nombre": "frutas",          "modo": "search", "query": "sandia"},
    {"nombre": "frutas",          "modo": "search", "query": "anana"},
    {"nombre": "frutas",          "modo": "search", "query": "frutillas"},
    {"nombre": "frutas",          "modo": "search", "query": "cerezas"},
    {"nombre": "frutas",          "modo": "search", "query": "kiwi"},
    {"nombre": "frutas",          "modo": "search", "query": "arandano"},
    {"nombre": "frutas",          "modo": "search", "query": "tomate cherry"},
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