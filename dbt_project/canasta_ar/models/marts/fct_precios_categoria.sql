-- fct_precios_categoria.sql
-- Calcula estadísticas de precio por categoría
-- Usa solo la fecha más reciente por categoría
-- Limpia outliers con método IQR antes de calcular percentiles

with fechas_max as (
    select categoria, max(fecha) as fecha_max
    from staging.stg_coto
    group by categoria
),

base as (
    select
        s.categoria,
        s.precio_por_unidad_std,
        s.unidad_precio_std
    from staging.stg_coto s
    join fechas_max f
        on s.categoria = f.categoria
        and s.fecha = f.fecha_max
    where s.precio_por_unidad_std is not null
      and s.precio_por_unidad_std > 0
      and not (s.categoria = 'aceite_oliva'   and s.unidad_precio_std = 'precio/kg')
      and not (s.categoria = 'dulce_de_leche' and s.unidad_precio_std = 'precio/L')
      and not (s.categoria = 'yogur'          and s.unidad_precio_std = 'precio/L')
      and not (s.categoria = 'vino'           and s.unidad_precio_std = 'precio/kg')
),

iqr_bounds as (
    select
        categoria,
        percentile_cont(0.25) within group (order by precio_por_unidad_std) as q1,
        percentile_cont(0.75) within group (order by precio_por_unidad_std) as q3,
        percentile_cont(0.75) within group (order by precio_por_unidad_std)
            - percentile_cont(0.25) within group (order by precio_por_unidad_std) as iqr
    from base
    group by categoria
),

sin_outliers as (
    select b.*
    from base b
    join iqr_bounds i on b.categoria = i.categoria
    where b.precio_por_unidad_std >= (i.q1 - 1.5 * i.iqr)
      and b.precio_por_unidad_std <= (i.q3 + 1.5 * i.iqr)
),

conteos as (
    select
        categoria,
        count(*) as n_productos
    from sin_outliers
    group by categoria
),

final as (
    select
        current_date                                                                                        as fecha,
        s.categoria,
        s.unidad_precio_std,
        c.n_productos,
        round(cast(percentile_cont(0.10) within group (order by s.precio_por_unidad_std) as numeric), 2)   as p10,
        round(cast(percentile_cont(0.20) within group (order by s.precio_por_unidad_std) as numeric), 2)   as p20,
        round(cast(percentile_cont(0.25) within group (order by s.precio_por_unidad_std) as numeric), 2)   as p25,
        round(cast(percentile_cont(0.45) within group (order by s.precio_por_unidad_std) as numeric), 2)   as p45,
        round(cast(percentile_cont(0.50) within group (order by s.precio_por_unidad_std) as numeric), 2)   as p50,
        round(cast(percentile_cont(0.55) within group (order by s.precio_por_unidad_std) as numeric), 2)   as p55,
        round(cast(percentile_cont(0.60) within group (order by s.precio_por_unidad_std) as numeric), 2)   as p60,
        round(cast(percentile_cont(0.75) within group (order by s.precio_por_unidad_std) as numeric), 2)   as p75,
        round(cast(percentile_cont(0.80) within group (order by s.precio_por_unidad_std) as numeric), 2)   as p80,
        round(cast(percentile_cont(0.90) within group (order by s.precio_por_unidad_std) as numeric), 2)   as p90,
        round(cast(percentile_cont(0.50) within group (order by s.precio_por_unidad_std) as numeric), 2)   as precio_mediana,
        round(cast(percentile_cont(0.90) within group (order by s.precio_por_unidad_std) as numeric), 2)
            - round(cast(percentile_cont(0.10) within group (order by s.precio_por_unidad_std) as numeric), 2) as dispersion_p90_p10
    from sin_outliers s
    join conteos c on s.categoria = c.categoria
    group by s.categoria, s.unidad_precio_std, c.n_productos
)

select * from final