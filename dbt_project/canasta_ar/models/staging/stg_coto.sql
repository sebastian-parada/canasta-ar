-- stg_coto.sql
-- Limpia y normaliza los precios scrapeados de Coto Digital
-- Aplica reglas de filtrado para eliminar productos no alimenticios

with source as (
    select * from raw.scraper_precios
    where supermercado = 'coto'
      and precio is not null
      and precio > 0
),

cleaned as (
    select
        fecha,
        supermercado,
        categoria,
        id_producto,
        nombre_producto,
        lower(nombre_producto)          as nombre_lower,
        marca,
        precio                          as precio_lista,
        unidad_medida,
        contenido,
        unidad_contenido,
        url_producto,
        inserted_at
    from source
),

filtered as (
    select * from cleaned
    where
        nombre_lower not like '%shampoo%'
        and nombre_lower not like '%detergente%'
        and nombre_lower not like '%desodorante%'
        and nombre_lower not like '%jabón de tocador%'
        and nombre_lower not like '%pañal%'
        and nombre_lower not like '%almohadón%'
        and nombre_lower not like '%sillón%'
        and nombre_lower not like '%cuchillo%'
        and nombre_lower not like '%sartén%'
        and nombre_lower not like '%molde%'
        and nombre_lower not like '%caramelo%'
        and not (
            categoria = 'sal'
            and nombre_lower not like '%sal fina%'
            and nombre_lower not like '%sal gruesa%'
            and nombre_lower not like '%sal entrefina%'
            and nombre_lower not like '%sal de mesa%'
            and nombre_lower not like '%sal parrillera%'
        )
        and not (
            categoria = 'agua'
            and nombre_lower not like '%agua mineral%'
            and nombre_lower not like '%agua saborizada%'
            and nombre_lower not like '%agua con gas%'
            and nombre_lower not like '%agua sin gas%'
            and nombre_lower not like '%agua purificada%'
            and nombre_lower not like '%agua glaciar%'
            and nombre_lower not like '%agua villavicencio%'
            and nombre_lower not like '%agua ser%'
        )
        and not (
            categoria = 'huevos'
            and nombre_lower not like '%huevo blanco%'
            and nombre_lower not like '%huevo color%'
            and nombre_lower not like '%huevos%'
            and nombre_lower not like '%maple%'
        )
        and not (
            categoria = 'pan'
            and nombre_lower not like '%pan %'
            and nombre_lower not like '%lactal%'
            and nombre_lower not like '%baguette%'
            and nombre_lower not like '%bollito%'
            and nombre_lower not like '%ciabatta%'
            and nombre_lower not like '%cremona%'
            and nombre_lower not like '%chipa%'
            and nombre_lower not like '%media luna%'
        )
        and not (
            categoria = 'arroz'
            and nombre_lower not like '%arroz largo%'
            and nombre_lower not like '%arroz parboil%'
            and nombre_lower not like '%arroz doble%'
            and nombre_lower not like '%arroz integral%'
            and nombre_lower not like '%arroz 00000%'
        )
),

with_cantidad as (
    select
        *,
        case
            -- Huevos: extraer cantidad de unidades del nombre
            when categoria = 'huevos' and nombre_lower ~ '\d+\s*(u|uni|unidades?)(?!\w)'
                then cast(
                    regexp_replace(nombre_lower, '^.*?(\d+)\s*(u|uni|unidades?).*$', '\1') as numeric
                )
            when nombre_lower ~ '\d+[\.,]?\d*\s*(kg|kgm)(?!\w)'
                then cast(
                    regexp_replace(
                        regexp_replace(nombre_lower, '^.*?(\d+[\.,]?\d*)\s*(kg|kgm).*$', '\1'),
                        ',', '.'
                    ) as numeric
                )
            when nombre_lower ~ '\d+\s*(g|gr|grm|gramos?)(?!\w)'
                then cast(
                    regexp_replace(
                        regexp_replace(nombre_lower, '^.*?(\d+[\.,]?\d*)\s*(g|gr|grm|gramos?).*$', '\1'),
                        ',', '.'
                    ) as numeric
                ) / 1000.0
            when nombre_lower ~ '\d+[\.,]?\d*\s*(l|lt|lts|litros?)(?!\w)'
                then cast(
                    regexp_replace(
                        regexp_replace(nombre_lower, '^.*?(\d+[\.,]?\d*)\s*(l|lt|lts|litros?).*$', '\1'),
                        ',', '.'
                    ) as numeric
                )
            when nombre_lower ~ '\d+\s*(ml|cc)(?!\w)'
                then cast(
                    regexp_replace(nombre_lower, '^.*?(\d+)\s*(ml|cc).*$', '\1') as numeric
                ) / 1000.0
            when unidad_contenido ilike '%kilogram%' and contenido > 0 then contenido
            when unidad_contenido ilike '%kilo%' and contenido > 0 then contenido
            when unidad_contenido ilike '%litro%' and contenido > 0 then contenido
            when unidad_contenido ilike '%gramo%' and contenido > 0 then contenido / 1000.0
            else null
        end as cantidad_std,
        case
            when categoria = 'huevos' and nombre_lower ~ '\d+\s*(u|uni|unidades?)(?!\w)' then 'unidades'
            when nombre_lower ~ '\d+[\.,]?\d*\s*(kg|kgm)(?!\w)'           then 'kg'
            when nombre_lower ~ '\d+\s*(g|gr|grm|gramos?)(?!\w)'          then 'kg'
            when nombre_lower ~ '\d+[\.,]?\d*\s*(l|lt|lts|litros?)(?!\w)' then 'L'
            when nombre_lower ~ '\d+\s*(ml|cc)(?!\w)'                     then 'L'
            when unidad_contenido ilike '%kilogram%'                       then 'kg'
            when unidad_contenido ilike '%kilo%'                           then 'kg'
            when unidad_contenido ilike '%litro%'                          then 'L'
            else unidad_medida
        end as unidad_std
    from filtered
),

final as (
    select
        fecha,
        supermercado,
        categoria,
        id_producto,
        nombre_producto,
        marca,
        precio_lista,
        cantidad_std                        as cantidad,
        unidad_std                          as unidad,
        case
            when categoria = 'huevos' and cantidad_std > 0
                then round(precio_lista / cantidad_std * 12, 2)
            when cantidad_std > 0
                then round(precio_lista / cantidad_std, 2)
            else null
        end                                 as precio_por_unidad_std,
        case
            when categoria = 'huevos'  then 'precio/docena'
            when unidad_std = 'kg'     then 'precio/kg'
            when unidad_std = 'L'      then 'precio/L'
            else 'precio/unidad'
        end                                 as unidad_precio_std,
        url_producto,
        inserted_at
    from with_cantidad
)

select * from final