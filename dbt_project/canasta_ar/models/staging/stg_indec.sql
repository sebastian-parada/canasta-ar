-- stg_indec.sql
-- Limpia y normaliza la canasta básica del INDEC (CABA)

with source as (
    select * from raw.indec_canasta_basica
),

renamed as (
    select
        periodo,
        cba_adulto              as cba_pesos,
        cbt_adulto              as cbt_pesos,
        fuente,
        inserted_at
    from source
    where cba_adulto is not null
      and cba_adulto > 0
)

select * from renamed