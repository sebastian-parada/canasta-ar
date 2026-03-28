-- =============================================================
-- canasta-ar: Setup inicial de la base de datos
-- Ejecutar una sola vez para crear el schema raw
-- =============================================================

-- Crear la base de datos (ejecutar conectado a postgres)
-- CREATE DATABASE canasta_ar;

-- Schema para datos crudos (sin transformar)
CREATE SCHEMA IF NOT EXISTS raw;

-- Schema para modelos dbt transformados
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- ---------------------------------------------------------------
-- TABLA: tipo de cambio BCRA (se actualiza diariamente)
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.bcra_tipo_cambio (
    id              SERIAL PRIMARY KEY,
    fecha           DATE NOT NULL,
    variable        TEXT NOT NULL,   -- ej: "Tipo de cambio minorista ($ por USD) Comunicación B 9791"
    valor           NUMERIC(15, 4),
    inserted_at     TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_bcra_fecha_variable
    ON raw.bcra_tipo_cambio (fecha, variable);

-- ---------------------------------------------------------------
-- TABLA: canasta básica INDEC (se actualiza mensualmente)
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.indec_canasta_basica (
    id              SERIAL PRIMARY KEY,
    periodo         DATE NOT NULL,
    cba_adulto      NUMERIC(15, 2),
    cbt_adulto      NUMERIC(15, 2),
    fuente          TEXT DEFAULT 'INDEC-CABA',
    inserted_at     TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_indec_periodo
    ON raw.indec_canasta_basica (periodo);

-- ---------------------------------------------------------------
-- TABLA: precios SEPA (se actualiza diariamente, muchos registros)
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.sepa_precios (
    id                  BIGSERIAL PRIMARY KEY,
    fecha               DATE NOT NULL,
    id_comercio         TEXT,
    id_bandera          TEXT,
    descripcion_bandera TEXT,           -- Nombre cadena: Coto, Carrefour, etc.
    id_sucursal         TEXT,
    sucursal_nombre     TEXT,
    sucursal_tipo       TEXT,
    provincia           TEXT,
    localidad           TEXT,
    id_producto         TEXT,
    nombre_producto     TEXT,
    marca               TEXT,
    precio_lista        NUMERIC(15, 2),
    precio_promo_a      NUMERIC(15, 2),
    precio_promo_b      NUMERIC(15, 2),
    inserted_at         TIMESTAMP DEFAULT NOW()
);

-- Índice clave para filtrar por fecha y cadena
CREATE INDEX IF NOT EXISTS idx_sepa_fecha_bandera
    ON raw.sepa_precios (fecha, descripcion_bandera);

CREATE INDEX IF NOT EXISTS idx_sepa_producto
    ON raw.sepa_precios (id_producto);

-- ---------------------------------------------------------------
-- TABLA: precios scrapeados de webs de supermercados
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.scraper_precios (
    id              BIGSERIAL PRIMARY KEY,
    fecha           DATE NOT NULL,
    supermercado    TEXT NOT NULL,       -- 'coto', 'carrefour', 'dia', 'disco'
    id_producto     TEXT,               -- EAN o ID interno del sitio
    nombre_producto TEXT NOT NULL,
    marca           TEXT,
    precio          NUMERIC(15, 2),
    precio_promo    NUMERIC(15, 2),
    url_producto    TEXT,
    inserted_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scraper_fecha_super
    ON raw.scraper_precios (fecha, supermercado);

-- ---------------------------------------------------------------
-- Vista rápida para verificar que todo está cargando
-- ---------------------------------------------------------------
CREATE OR REPLACE VIEW raw.resumen_cargas AS
SELECT 'bcra_tipo_cambio'   AS tabla, COUNT(*) AS registros, MAX(fecha) AS ultimo_dato FROM raw.bcra_tipo_cambio
UNION ALL
SELECT 'indec_canasta_basica',          COUNT(*), MAX(periodo)  FROM raw.indec_canasta_basica
UNION ALL
SELECT 'sepa_precios',                  COUNT(*), MAX(fecha)    FROM raw.sepa_precios
UNION ALL
SELECT 'scraper_precios',               COUNT(*), MAX(fecha)    FROM raw.scraper_precios;
