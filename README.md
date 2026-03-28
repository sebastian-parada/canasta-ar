# 🛒 canasta-ar

Pipeline de datos para analizar y comparar el costo real de alimentarse en CABA, Argentina.

Cruza tres fuentes: precios oficiales (SEPA), datos del INDEC y scraping directo de supermercados para responder una pregunta simple con datos complejos:

> **¿Cuánto cuesta realmente comer por persona por mes en Buenos Aires, según qué supermercado y qué calidad de productos elegís?**

---

## Fuentes de datos

| Fuente | Tipo | Frecuencia | Descripción |
|---|---|---|---|
| **SEPA** | Oficial | Diaria | 12M registros de precios de 70k productos en todo el país |
| **INDEC CBA** | Oficial | Mensual | Canasta Básica Alimentaria oficial desde 1988 |
| **BCRA** | Oficial | Diaria | Tipo de cambio para expresar todo en ARS y USD |
| **Scraping** | Real | Semanal | Precios reales de Coto, Carrefour, Dia y Disco en CABA |

## Supermercados relevados (CABA)

| Cadena | Segmento |
|---|---|
| Coto | Popular / precio bajo |
| Carrefour | Masivo / precio medio |
| Dia | Descuento / hard discount |
| Disco | Medio-alto |

## Stack tecnológico

- **Python + Pandas** — ETL e ingestion
- **PostgreSQL** — almacenamiento raw y final
- **dbt** — modelado y transformación
- **Airflow** — orquestación del pipeline diario

## Estructura del proyecto

```
canasta-ar/
├── ingestion/          # Scripts de carga por fuente
│   ├── bcra/           # Tipo de cambio diario
│   ├── indec/          # Canasta básica mensual
│   ├── sepa/           # Precios SEPA diarios
│   └── scrapers/       # Scraping de supermercados
├── dbt_project/        # Modelos de transformación
│   └── models/
│       ├── staging/    # Limpieza y normalización
│       └── marts/      # Modelos finales para análisis
├── airflow/
│   └── dags/           # DAGs de orquestación
└── docs/
    └── setup_db.sql    # Script de creación de tablas
```

## Setup inicial

### 1. Clonar y crear entorno virtual

```bash
git clone https://github.com/TU_USUARIO/canasta-ar.git
cd canasta-ar
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 2. Configurar variables de entorno

```bash
cp .env.example .env
# Editar .env con tus credenciales de PostgreSQL
```

### 3. Crear la base de datos

```sql
-- En psql o pgAdmin:
CREATE DATABASE canasta_ar;
```

```bash
psql -U postgres -d canasta_ar -f docs/setup_db.sql
```

### 4. Correr la primera ingestion (BCRA — la más simple)

```bash
python -m ingestion.bcra.fetch
```

### 5. Cargar canasta INDEC

```bash
python -m ingestion.indec.fetch
```

## Canastas definidas

| Canasta | Criterio | Ejemplo de marcas |
|---|---|---|
| **Mínima** | Marca propia del supermercado | Coto, Carrefour (marca blanca) |
| **Básica** | Marca masiva económica | Marolio, Cuisine & Co |
| **Completa** | Marca líder estándar | Arcor, Molinos, La Serenísima |
| **Premium** | Marca premium | Danone, Nestlé, Santa Rosa |

---

Proyecto de portfolio — Data Engineering con Python, dbt y Airflow.
