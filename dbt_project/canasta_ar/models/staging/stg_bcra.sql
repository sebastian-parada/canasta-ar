-- stg_bcra.sql
-- Limpia y normaliza el tipo de cambio diario del BCRA

with source as (
    select * from raw.bcra_tipo_cambio
),

renamed as (
    select
        fecha,
        variable,
        valor                           as tipo_cambio_ars,
        inserted_at
    from source
    where valor is not null
      and valor > 0
)

select * from renamed