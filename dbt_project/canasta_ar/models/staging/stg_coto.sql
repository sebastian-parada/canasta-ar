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
        and not (
            categoria = 'jamon_cocido'
            and (
                nombre_lower like '%panzotti%'
                or nombre_lower like '%ravioles%'
                or nombre_lower like '%capelletti%'
            )
        )
        and not (
            categoria = 'salame'
            and nombre_lower like '%queso untable%'
        )
        and not (
            categoria = 'cafe'
            and (
                nombre_lower like '%licor%'
                or nombre_lower like '%cacao%'
                or nombre_lower like '%chocolat%'
                or nombre_lower like '%taza%'
                or nombre_lower like '%lata%'
                or nombre_lower like '%yogur%'
                or nombre_lower like '%cereal%'
                or nombre_lower like '%alfajor%'
            )
        )
        and not (
            categoria = 'yerba_mate'
            and (
                nombre_lower like '%saquito%'
                or nombre_lower like '%mate cocido%'
                or nombre_lower like '%te %'
                or nombre_lower like '% te%'
                or nombre_lower like '%tizana%'
            )
        )
        and not (
            categoria = 'leche'
            and (
                nombre_lower like '%almendra%'
                or nombre_lower like '%avena%'
                or nombre_lower like '%soja%'
                or nombre_lower like '%coco%'
                or nombre_lower like '%vegetal%'
                or nombre_lower like '%bebida a base%'
                or nombre_lower like '%ades%'
                or nombre_lower like '%silk%'
            )
        )
        and not (
            categoria = 'manteca'
            and (
                nombre_lower like '%lechuga%'
                or nombre_lower like '%galletita%'
                or nombre_lower like '%mejillon%'
                or nombre_lower like '%margarina%'
                or nombre_lower like '%crema%'
                or nombre_lower like '%aceite%'
                or nombre_lower like '%limpiador%'
            )
        )
        and not (
            categoria = 'azucar'
            and (
                nombre_lower like '%gaseosa%'
                or nombre_lower like '%amargo%'
                or nombre_lower like '%sprite%'
                or nombre_lower like '%seven up%'
                or nombre_lower like '%paso de los toros%'
                or nombre_lower like '%bebida%'
                or nombre_lower like '%edulcorante%'
            )
        )
        and not (
            categoria = 'frutos_secos'
            and (
                nombre_lower like '%bebida%'
                or nombre_lower like '%leche de%'
                or nombre_lower like '%jabón%'
                or nombre_lower like '%limpiador%'
                or nombre_lower like '%esencia%'
                or nombre_lower like '%crema de%'
                or nombre_lower like '%aceite de%'
            )
        )
        and not (
            categoria = 'yogur'
            and (
                nombre_lower like '%yogurtera%'
                or nombre_lower like '%máquina%'
                or nombre_lower like '%accesorio%'
            )
        )
        and not (
            categoria = 'cereales'
            and (
                nombre_lower like '%chocolate%'
                or nombre_lower like '%yogur%'
                or nombre_lower like '%alfajor%'
                or nombre_lower like '%golosina%'
                or nombre_lower like '%caramelo%'
                or nombre_lower like '%turron%'
            )
        )
        and not (
            categoria = 'verduras'
            and (
                nombre_lower like '%limpiador%'
                or nombre_lower like '%desengrasante%'
                or nombre_lower like '%detergente%'
                or nombre_lower like '%lavandina%'
                or nombre_lower like '%perfume%'
                or nombre_lower like '%fragancia%'
            )
        )
        and not (
            categoria = 'frutas'
            and (
                nombre_lower like '%limpiador%'
                or nombre_lower like '%desengrasante%'
                or nombre_lower like '%detergente%'
                or nombre_lower like '%lavandina%'
                or nombre_lower like '%perfume%'
                or nombre_lower like '%fragancia%'
                or nombre_lower like '%amargo%'
                or nombre_lower like '%jugo concentrado%'
            )
        )
        and not (
            categoria = 'pollo'
            and (
                nombre_lower like '%alimento para perro%'
                or nombre_lower like '%alimento para gato%'
                or nombre_lower like '%alimento adulto%'
                or nombre_lower like '%alimento cachorro%'
                or nombre_lower like '%pedigree%'
                or nombre_lower like '%dog chow%'
                or nombre_lower like '%cat chow%'
                or nombre_lower like '%whiskas%'
                or nombre_lower like '%purina%'
                or nombre_lower like '%kongo%'
                or nombre_lower like '%snack%'
            )
        )
        and not (
            categoria = 'carnes_vacunas'
            and (
                nombre_lower like '%hermetico%'
                or nombre_lower like '%cesto%'
                or nombre_lower like '%canasto%'
                or nombre_lower like '%recipiente%'
                or nombre_lower like '%frasco%'
                or nombre_lower like '%contenedor%'
                or nombre_lower like '%botella%'
                or nombre_lower like '%jarra%'
                or nombre_lower like '%tarro%'
                or nombre_lower like '%mixer%'
                or nombre_lower like '%procesadora%'
                or nombre_lower like '%picadora%'
                or nombre_lower like '%licuadora%'
                or nombre_lower like '%parrilla%'
                or nombre_lower like '%tabla%'
                or nombre_lower like '%cuaderno%'
                or nombre_lower like '%falda%'
                or nombre_lower like '%alfombra%'
                or nombre_lower like '%manta%'
                or nombre_lower like '%traje%'
                or nombre_lower like '%short%'
                or nombre_lower like '%sarten%'
                or nombre_lower like '%cacerola%'
                or nombre_lower like '%olla%'
                or nombre_lower like '%termo%'
                or nombre_lower like '%ravioles%'
                or nombre_lower like '%capelletti%'
                or nombre_lower like '%tap.empanada%'
                or nombre_lower like '%tapa de empanada%'
                or nombre_lower like '%sorrentino%'
                or nombre_lower like '%aderezo%'
                or nombre_lower like '%condimento%'
                or nombre_lower like '%cuadro%'
                or nombre_lower like '%reloj%'
            )
        )
        and not (
            categoria in ('galletita', 'galletas')
            and (
                nombre_lower like '%cracker%'
                or nombre_lower like '%criollita%'
                or nombre_lower like '%grisine%'
                or nombre_lower like '%bizcochito%'
                or nombre_lower like '%marinera%'
                or nombre_lower like '%picante%'
                or nombre_lower like '%pedigree%'
                or nombre_lower like '%dr. zoo%'
                or nombre_lower like '%mon ami%'
                or nombre_lower like '%juego de mesa%'
                or nombre_lower like '%lata redonda%'
                or nombre_lower like '%snack%'
                or nombre_lower like '%talitas sabor queso%'
                or nombre_lower like '%talitas sabor jamon%'
            )
        )
),

with_cantidad as (
    select
        *,
        case
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
            when unidad_medida = 'KGS' then 1.0
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
            when unidad_medida = 'KGS'                                     then 'kg'
            else unidad_medida
        end as unidad_std
    from filtered
),

final as (
    select
        fecha,
        supermercado,
        case
            when categoria = 'galletas' then 'galletitas'
            else categoria
        end                                 as categoria,
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