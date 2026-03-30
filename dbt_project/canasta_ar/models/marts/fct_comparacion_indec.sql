-- fct_comparacion_indec.sql
-- Compara el costo de las canastas reales (scraper supermercados)
-- con la canasta básica alimentaria y total oficial del INDEC (CABA)
-- Expresa valores en ARS y USD usando el tipo de cambio del BCRA

with canastas as (
    select * from marts.fct_canastas
),

indec as (
    select
        periodo,
        cba_pesos,
        cbt_pesos
    from staging.stg_indec
    where periodo = (select max(periodo) from staging.stg_indec)
),

bcra as (
    select
        tipo_cambio_ars
    from staging.stg_bcra
    where fecha = (select max(fecha) from staging.stg_bcra)
),

final as (
    select
        c.canasta,
        c.costo_total_ars,
        round(c.costo_total_ars / b.tipo_cambio_ars, 2)            as costo_total_usd,
        i.periodo                                                    as indec_periodo,
        i.cba_pesos                                                  as indec_cba_ars,
        i.cbt_pesos                                                  as indec_cbt_ars,
        round(i.cba_pesos / b.tipo_cambio_ars, 2)                   as indec_cba_usd,
        round(i.cbt_pesos / b.tipo_cambio_ars, 2)                   as indec_cbt_usd,
        round(c.costo_total_ars - i.cba_pesos, 0)                   as diff_vs_cba_ars,
        round((c.costo_total_ars / i.cba_pesos - 1) * 100, 1)       as diff_vs_cba_pct,
        round(c.costo_total_ars - i.cbt_pesos, 0)                   as diff_vs_cbt_ars,
        round((c.costo_total_ars / i.cbt_pesos - 1) * 100, 1)       as diff_vs_cbt_pct,
        b.tipo_cambio_ars                                            as tipo_cambio_ars
    from canastas c
    cross join indec i
    cross join bcra b
)

select * from final
order by case canasta
    when 'minima'   then 1
    when 'basica'   then 2
    when 'completa' then 3
    when 'premium'  then 4
end