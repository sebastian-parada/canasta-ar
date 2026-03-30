# 🛒 canasta-ar
Proyecto en desarrollo. El código será refactorizado y documentado progresivamente.

Pipeline de datos para analizar y comparar el costo real de alimentarse en CABA, Argentina.
Cruza precios scrapeados de supermercados con datos oficiales del INDEC y BCRA para responder una pregunta simple con datos complejos:

> **¿Cuánto cuesta realmente comer por persona por mes? ¿Son representativos los datos de las fuentes oficiales?**

---

## Fuentes de datos

| Fuente | Tipo | Frecuencia | Descripción |
|---|---|---|---|
| **Coto Digital** | Scraping | Diaria | Precios reales scrapeados de la API interna de Coto |
| **INDEC CBA** | Oficial | Mensual | Canasta Básica Alimentaria oficial CABA desde 2013 |
| **BCRA** | Oficial | Diaria | Tipo de cambio minorista USD para expresar valores en ARS y USD |

## Supermercados relevados (CABA)

| Cadena | Segmento | Estado |
|---|---|---|
| Coto | Popular| ✅ Activo |
| Carrefour | Masivo | 🔜 Próximamente |
| Dia | Masivo | 🔜 Próximamente |
| Disco | Variedad | 🔜 Próximamente |

---

## Stack tecnológico

| Capa | Tecnología |
|---|---|
| Scraping / Ingestion | Python + Requests |
| Almacenamiento | PostgreSQL |
| Transformación | dbt |
| Orquestación | Airflow *(próximamente)* |

---

## Estructura del proyecto

```
canasta-ar/
├── ingestion/
│   ├── bcra/
│   │   └── fetch.py              ← Tipo de cambio diario BCRA
│   ├── indec/
│   │   └── fetch.py              ← Canasta básica CABA
│   └── scrapers/
│       └── coto.py               ← Scraper Coto Digital (~140 búsquedas, +3000 productos)
├── dbt_project/canasta_ar/
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── models/
│       ├── staging/
│       │   ├── stg_bcra.sql      ← Tipo de cambio limpio
│       │   ├── stg_indec.sql     ← Canasta INDEC limpia
│       │   └── stg_coto.sql      ← Precios Coto normalizados
│       └── marts/
│           ├── fct_precios_categoria.sql   ← Percentiles por categoría
│           ├── fct_canastas.sql            ← Costo mensual por canasta
│           └── fct_comparacion_indec.sql   ← Comparación vs INDEC
├── docs/
│   └── setup_db.sql              ← Schema PostgreSQL
├── requirements.txt
└── .env                          ← Credenciales (no en git)
```

---

## Canastas definidas

El proyecto define 4 canastas que representan distintos niveles de consumo alimentario en CABA, calculadas a partir de percentiles de precio de mercado:

| Canasta | Descripción | Percentil de precio |
|---|---|---|
| **Mínima** | Alimentación básica de subsistencia | P10 - P20 |
| **Básica** | Alimentación completa clase media-baja | P45 - P55 |
| **Completa** | Alimentación variada clase media | P60 - P75 |
| **Premium** | Alimentación premium clase alta | P80 - P90 |

> De momento, si una categoría tiene menos de 10 productos se usa la mediana en lugar de percentiles.
---

## Metodología

### Normalización de precios
Todos los precios se normalizan a una unidad estándar antes de comparar:
- Productos sólidos → **precio/kg**
- Líquidos → **precio/L**
- Otros → por ejemplo, Huevos: **precio/docena**

El peso real se extrae en el scrapping de la cifra detectada en la página o del nombre del producto usando regex (ej: "Arroz 500g" → 0.5 kg).

### Limpieza de outliers
Se aplica el método IQR antes de calcular percentiles:
- Se eliminan valores fuera del rango `[Q1 - 1.5×IQR, Q3 + 1.5×IQR]`

### Dispersión de mercado
Se calcula `P90 - P10` por categoría para medir la desigualdad de precios en el mercado.

---

## Base de datos

```
canasta_ar/
├── raw/
│   ├── scraper_precios       ← Precios crudos del scraper
│   ├── bcra_tipo_cambio      ← Tipo de cambio histórico
│   └── indec_canasta_basica  ← Canasta básica histórica INDEC
├── staging/
│   ├── stg_coto              ← Vista: precios limpios y normalizados
│   ├── stg_bcra              ← Vista: tipo de cambio limpio
│   └── stg_indec             ← Vista: canasta INDEC limpia
└── marts/
    ├── fct_precios_categoria ← Tabla: percentiles por categoría
    ├── fct_canastas          ← Tabla: costo mensual por canasta
    └── fct_comparacion_indec ← Tabla: comparación vs INDEC
```

---

## Setup inicial

### 1. Clonar y crear entorno virtual
```bash
git clone https://github.com/sebastian-parada/canasta-ar
cd canasta-ar
python -m venv .venv
.venv\Scripts\activate       # Windows
pip install -r requirements.txt
```

### 2. Configurar variables de entorno 
```bash
cp .env.example .env
# Editar .env con tus credenciales de PostgreSQL
```

### 3. Crear la base de datos
```bash
psql -U postgres -f docs/setup_db.sql
```

### 4. Correr ingestion
```bash
python -m ingestion.bcra.fetch
python -m ingestion.indec.fetch
python -m ingestion.scrapers.coto
```

### 5. Correr transformaciones dbt
```bash
cd dbt_project/canasta_ar
dbt run
```

---

## Resultados *(30/03/2026)*
*Costo estimado de alimentación por persona por mes en CABA*

| Canasta | Costo mensual (ARS) | Costo mensual (USD) |
|---|---|---|
| Mínima | $96.432 | ~USD 69 |
| Básica | $317.176 | ~USD 226 |
| Completa | $663.023 | ~USD 473 |
| Premium | $1.179.775 | ~USD 841 |

> Tipo de cambio: ~$1.404 ARS/USD (BCRA, 27/03/2026)


---

## Próximamente

- [ ] **Comparación vs INDEC** - cruzar precios reales del scraper con la canasta básica oficial
- [ ] **Scrapers adicionales** - Carrefour, Dia, Disco para comparar precios entre cadenas
- [ ] **Orquestación con Airflow** - DAGs para correr el pipeline diariamente de forma automática
- [ ] **Dashboard** - visualización interactiva de la evolución de precios y canastas
- [ ] **¿Integración SEPA?** - cruzar datos oficiales de preciosclaros.gob.ar para detectar diferencias entre los precios declarados y los reales en la página de cada supermercado 

---
**Sebastián Parada** - 2026
