from ingestion.scrapers.coto import fetch_categoria, extraer_producto, CATEGORIAS

# Probar banana
cat_banana = next(c for c in CATEGORIAS if c["query"] == "banana")
print(f"=== FRUTAS: banana ===")
productos = fetch_categoria(cat_banana)
print(f"Total crudos: {len(productos)}")
if productos:
    p = extraer_producto(productos[0], cat_banana["nombre"])
    print(p)

print()

# Probar papa
cat_papa = next(c for c in CATEGORIAS if c["query"] == "papa")
print(f"=== VERDURAS: papa ===")
productos = fetch_categoria(cat_papa)
print(f"Total crudos: {len(productos)}")
if productos:
    p = extraer_producto(productos[0], cat_papa["nombre"])
    print(p)