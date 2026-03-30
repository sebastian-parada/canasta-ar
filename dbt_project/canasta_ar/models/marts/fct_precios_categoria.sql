-- fct_precios_categoria.sql
-- Calcula estadísticas de precio por categoría y fecha
-- Limpia outliers con método IQR antes de calcular percentiles
-- Si hay >= 10 productos usa percentiles, sino usa mediana

with base as (
    select
        fecha,
        categoria,
        precio_por_unidad_std,
        unidad_precio_std
    from staging.stg_coto
    where precio_por_unidad_std is not null
      and precio_por_unidad_std > 0
      and not (categoria = 'aceite_oliva'   and unidad_precio_std = 'precio/kg')
      and not (categoria = 'dulce_de_leche' and unidad_precio_std = 'precio/L')
      and not (categoria = 'yogur'          and unidad_precio_std = 'precio/L')
),

-- Calcular Q1 y Q3 por categoría y fecha para el filtro IQR
iqr_bounds as (
    select
        fecha,
        categoria,
        percentile_cont(0.25) within group (order by precio_por_unidad_std) as q1,
        percentile_cont(0.75) within group (order by precio_por_unidad_std) as q3,
        percentile_cont(0.75) within group (order by precio_por_unidad_std)
            - percentile_cont(0.25) within group (order by precio_por_unidad_std) as iqr
    from base
    group by fecha, categoria
),

-- Filtrar outliers: excluir < Q1 - 1.5*IQR o > Q3 + 1.5*IQR
sin_outliers as (
    select b.*
    from base b
    join iqr_bounds i
        on b.fecha = i.fecha
        and b.categoria = i.categoria
    where b.precio_por_unidad_std >= (i.q1 - 1.5 * i.iqr)
      and b.precio_por_unidad_std <= (i.q3 + 1.5 * i.iqr)
),

-- Contar productos por categoría después de limpiar outliers
conteos as (
    select
        fecha,
        categoria,
        count(*) as n_productos
    from sin_outliers
    group by fecha, categoria
),

-- Calcular percentiles finales
final as (
    select
        s.fecha,
        s.categoria,
        s.unidad_precio_std,
        c.n_productos,
        round(cast(percentile_cont(0.10) within group (order by s.precio_por_unidad_std) as numeric), 2) as p10,
        round(cast(percentile_cont(0.20) within group (order by s.precio_por_unidad_std) as numeric), 2) as p20,
        round(cast(percentile_cont(0.25) within group (order by s.precio_por_unidad_std) as numeric), 2) as p25,
        round(cast(percentile_cont(0.45) within group (order by s.precio_por_unidad_std) as numeric), 2) as p45,
        round(cast(percentile_cont(0.50) within group (order by s.precio_por_unidad_std) as numeric), 2) as p50,
        round(cast(percentile_cont(0.55) within group (order by s.precio_por_unidad_std) as numeric), 2) as p55,
        round(cast(percentile_cont(0.60) within group (order by s.precio_por_unidad_std) as numeric), 2) as p60,
        round(cast(percentile_cont(0.75) within group (order by s.precio_por_unidad_std) as numeric), 2) as p75,
        round(cast(percentile_cont(0.80) within group (order by s.precio_por_unidad_std) as numeric), 2) as p80,
        round(cast(percentile_cont(0.90) within group (order by s.precio_por_unidad_std) as numeric), 2) as p90,
        -- Precio representativo: percentil 50 si >= 10 productos, sino mediana igual
        round(cast(percentile_cont(0.50) within group (order by s.precio_por_unidad_std) as numeric), 2) as precio_mediana,
        -- Dispersión del mercado
        round(cast(percentile_cont(0.90) within group (order by s.precio_por_unidad_std) as numeric), 2)
            - round(cast(percentile_cont(0.10) within group (order by s.precio_por_unidad_std) as numeric), 2) as dispersion_p90_p10
    from sin_outliers s
    join conteos c
        on s.fecha = c.fecha
        and s.categoria = c.categoria
    group by s.fecha, s.categoria, s.unidad_precio_std, c.n_productos
)

select * from final