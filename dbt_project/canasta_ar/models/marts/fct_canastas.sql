-- fct_canastas.sql
-- Calcula el costo mensual estimado de cada canasta
-- usando los percentiles de precio por categoría
-- Canasta mínima: P10-P20, básica: P45-P55, completa: P60-P75, premium: P80-P90

with precios as (
    select *
    from marts.fct_precios_categoria
    where not (categoria = 'aceite_oliva'   and unidad_precio_std = 'precio/kg')
      and not (categoria = 'dulce_de_leche' and unidad_precio_std = 'precio/L')
      and not (categoria = 'yogur'          and unidad_precio_std = 'precio/L')
      and not (categoria = 'vino'           and unidad_precio_std = 'precio/kg')
),

-- Canastas con cantidades mensuales por producto
-- Unidades: kg, L, docena según corresponda
canasta_items as (
    select 'minima' as canasta, categoria, cantidad_mensual from (values
        ('arroz',          2.0),
        ('fideos',         1.5),
        ('polenta',        0.5),
        ('aceite_girasol', 1.5),
        ('sal',            0.5),
        ('huevos',         2.0),  -- 2 docenas
        ('yogur',          2.0),
        ('pan',            1.5),
        ('verduras',       4.0),
        ('frutas',         3.0),
        ('frutos_secos',   0.5),
        ('legumbres',      1.0),
        ('agua',           8.0),
        ('cereales',       0.5),
        ('galletitas',     0.5),
        ('pate',           0.3)
    ) as t(categoria, cantidad_mensual)

    union all

    select 'basica' as canasta, categoria, cantidad_mensual from (values
        ('arroz',          2.0),
        ('polenta',        0.5),
        ('fideos',         2.0),
        ('aceite_girasol', 1.5),
        ('sal',            0.5),
        ('azucar',         1.0),
        ('leche',          8.0),
        ('yogur',          3.0),
        ('yerba_mate',     1.0),
        ('cafe',           0.25),
        ('pan',            2.0),
        ('pollo',          2.0),
        ('carnes_vacunas', 2.0),
        ('cerdo',          1.0),
        ('huevos',         3.0),
        ('harina',         1.0),
        ('verduras',       5.0),
        ('frutas',         4.0),
        ('frutos_secos',   0.5),
        ('manteca',        0.5),
        ('dulce_de_leche', 0.5),
        ('legumbres',      1.5),
        ('agua',           8.0),
        ('cereales',       0.5),
        ('galletitas',     0.5),
        ('salchichas',     0.5),
        ('pate',           0.3)
    ) as t(categoria, cantidad_mensual)

    union all

    select 'completa' as canasta, categoria, cantidad_mensual from (values
        ('arroz',          2.0),
        ('polenta',        0.5),
        ('fideos',         2.0),
        ('aceite_girasol', 1.5),
        ('aceite_oliva',   0.5),
        ('sal',            0.5),
        ('azucar',         1.0),
        ('leche',          8.0),
        ('yogur',          4.0),
        ('yerba_mate',     1.0),
        ('cafe',           0.25),
        ('pan',            2.0),
        ('pollo',          3.0),
        ('carnes_vacunas', 3.0),
        ('cerdo',          1.5),
        ('pescado',        1.0),
        ('huevos',         3.0),
        ('harina',         1.0),
        ('quesos',         1.0),
        ('jamon_cocido',   0.5),
        ('salame',         0.3),
        ('verduras',       6.0),
        ('frutas',         5.0),
        ('frutos_secos',   0.5),
        ('aceitunas',      0.3),
        ('manteca',        0.5),
        ('dulce_de_leche', 0.5),
        ('legumbres',      1.5),
        ('conservas_atun', 0.5),
        ('agua',           8.0),
        ('gaseosa',        4.0),
        ('cerveza',        2.0),
        ('vino',           2.0),
        ('cereales',       0.5),
        ('galletitas',     1.0),
        ('untables',       0.5),
        ('salchichas',     0.5),
        ('pate',           0.3)
    ) as t(categoria, cantidad_mensual)

    union all

    select 'premium' as canasta, categoria, cantidad_mensual from (values
        ('arroz',          2.0),
        ('fideos',         2.0),
        ('aceite_girasol', 1.0),
        ('aceite_oliva',   1.0),
        ('sal',            0.5),
        ('azucar',         1.0),
        ('leche',          8.0),
        ('yogur',          4.0),
        ('yerba_mate',     1.0),
        ('cafe',           0.5),
        ('pan',            2.0),
        ('pollo',          3.0),
        ('carnes_vacunas', 4.0),
        ('cerdo',          2.0),
        ('pescado',        2.0),
        ('huevos',         3.0),
        ('harina',         1.0),
        ('quesos',         1.5),
        ('jamon_cocido',   0.5),
        ('jamon_crudo',    0.3),
        ('salame',         0.5),
        ('verduras',       7.0),
        ('frutas',         6.0),
        ('frutos_secos',   1.0),
        ('aceitunas',      0.5),
        ('manteca',        0.5),
        ('dulce_de_leche', 0.5),
        ('legumbres',      1.5),
        ('conservas_atun', 0.5),
        ('pastas_frescas', 0.5),
        ('pastas_rellenas',0.5),
        ('agua',           8.0),
        ('gaseosa',        4.0),
        ('cerveza',        2.0),
        ('vino',           3.0),
        ('cereales',       1.0),
        ('galletitas',     1.0),
        ('untables',       1.0),
        ('salchichas',     0.5),
        ('pate',           0.3)
    ) as t(categoria, cantidad_mensual)
),

-- Precio representativo por canasta según percentil
precio_por_canasta as (
    select
        p.fecha,
        c.canasta,
        c.categoria,
        c.cantidad_mensual,
        p.n_productos,
        p.unidad_precio_std,
        -- Precio según canasta
        case c.canasta
            when 'minima'   then (p.p10 + p.p20) / 2
            when 'basica'   then (p.p45 + p.p55) / 2
            when 'completa' then (p.p60 + p.p75) / 2
            when 'premium'  then (p.p80 + p.p90) / 2
        end as precio_unitario,
        -- Si hay menos de 10 productos usar mediana
        case
            when p.n_productos < 10 then p.precio_mediana
            else case c.canasta
                when 'minima'   then (p.p10 + p.p20) / 2
                when 'basica'   then (p.p45 + p.p55) / 2
                when 'completa' then (p.p60 + p.p75) / 2
                when 'premium'  then (p.p80 + p.p90) / 2
            end
        end as precio_usado
    from canasta_items c
    inner join precios p
        on c.categoria = p.categoria
),

final as (
    select
        fecha,
        canasta,
        round(sum(precio_usado * cantidad_mensual), 0) as costo_total_ars,
        count(categoria) as n_categorias,
        count(case when precio_usado is null then 1 end) as categorias_sin_precio
    from precio_por_canasta
    group by fecha, canasta
)

select * from final
order by fecha, case canasta
    when 'minima'   then 1
    when 'basica'   then 2
    when 'completa' then 3
    when 'premium'  then 4
end