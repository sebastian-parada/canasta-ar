from ingestion.scrapers.coto import fetch_categoria, extraer_producto, insert_productos, get_db_connection

cat = {"nombre": "yerba_mate", "modo": "search", "query": "yerba"}

conn = get_db_connection()

productos_crudos = fetch_categoria(cat)
print(f"Total crudos: {len(productos_crudos)}")

productos_limpios = []
for p in productos_crudos:
    extraido = extraer_producto(p, cat["nombre"])
    if extraido:
        productos_limpios.append(extraido)

print(f"Total limpios: {len(productos_limpios)}")
insertados = insert_productos(productos_limpios, conn)
print(f"Insertados en DB: {insertados}")

conn.close()