-- stg_coto.sql
-- Limpia y normaliza los precios scrapeados de Coto Digital
-- Aplica reglas de filtrado para eliminar productos no alimenticios

with source as (
    select distinct on (supermercado, categoria, nombre_producto) *
    from raw.scraper_precios
    where supermercado = 'coto'
      and precio is not null
      and precio > 0
    order by supermercado, categoria, nombre_producto, fecha desc
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
        and nombre_lower not like '%sandwich%'
        and nombre_lower not like '%sándwich%'
        and nombre_lower not like '%bloque%'
        and nombre_lower not like '%remera%'

        -- SAL: solo sal de cocina
        and not (
            categoria = 'sal'
            and nombre_lower not like '%sal fina%'
            and nombre_lower not like '%sal gruesa%'
            and nombre_lower not like '%sal entrefina%'
            and nombre_lower not like '%sal de mesa%'
            and nombre_lower not like '%sal parrillera%'
            and nombre_lower not like '%saler%'
        )
        -- SAL: excluir sales modificadas y light
        and not (
            categoria = 'sal'
            and (
                nombre_lower like '%light%'
                or nombre_lower like '%modificada%'
                or nombre_lower like '%dietética%'
                or nombre_lower like '%reducida%'
                or nombre_lower like '%finas hierbas%'
            )
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
                or nombre_lower like '%chocolate%'
                or nombre_lower like '%café%'
                or nombre_lower like '%cafe%'
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
                or nombre_lower like '%rollo%'
                or nombre_lower like '%poroto%'
                or nombre_lower like '%galleta%'
                or nombre_lower like '%galletita%'
                or nombre_lower like '%medialuna%'
                or nombre_lower like '%bizco%'
                or nombre_lower like '%emulsion%'
                or nombre_lower like '%papel%'
                or nombre_lower like '%protector%'
                or nombre_lower like '%cacao%'
                or nombre_lower like '%labial%'
                or nombre_lower like '%crema%'
                or nombre_lower like '%figazzita%'
                or nombre_lower like '%pizz%'
                or nombre_lower like '%mantecado%'
            )
        )

        and not (
            categoria = 'frutos_secos'
            and (
                nombre_lower like '%bebida%'
                or nombre_lower like '%jabón%'
                or nombre_lower like '%limpiador%'
                or nombre_lower like '%esencia%'
                or nombre_lower like '%crema de%'
                or nombre_lower like '%aceite de%'
                or nombre_lower like '%leche%'
                or nombre_lower like '%vegetal%'
                or nombre_lower like '%bot%'
                or nombre_lower like '%chocolate%'
                or nombre_lower like '%harina%'
                or nombre_lower like '%cereal%'
                or nombre_lower like '%granola%'
                or nombre_lower like '%barr%'
                or nombre_lower like '%galle%'
                or nombre_lower like '%turr%'
                or nombre_lower like '%bomb%'
                or nombre_lower like '%helado%'
                or nombre_lower like '%postre%'
                or nombre_lower like '%huevo%'
                or nombre_lower like '%jab%'
                or nombre_lower like '%crema%'
                or nombre_lower like '%hidr%'
                or nombre_lower like '%acondicionador%'
                or nombre_lower like '%tint%'
                or nombre_lower like '%color%'
                or nombre_lower like '%tono%'
                or nombre_lower like '%alfajor%'
                or nombre_lower like '%licor%'
                or nombre_lower like '%mortadela%'
                or nombre_lower like '%sabor%'
                or nombre_lower like '%miel%'
                or nombre_lower like '%croc%'
                or nombre_lower like '%frito%'
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
                length(nombre_lower) > 25
                or array_length(regexp_split_to_array(nombre_lower, '\s+'), 1) > 5
                or nombre_lower like '%limpiador%'
                or nombre_lower like '%desengrasante%'
                or nombre_lower like '%detergente%'
                or nombre_lower like '%lavandina%'
                or nombre_lower like '%perfume%'
                or nombre_lower like '%fragancia%'
                or nombre_lower like '%sopa%'
                or nombre_lower like '%takis%'
                or nombre_lower like '%semilla%'
                or nombre_lower like '%peluche%'
                or nombre_lower like '%pizz%'
                or nombre_lower like '%snack%'
                or nombre_lower like '%salsa%'
                or nombre_lower like '%pure%'
                or nombre_lower like '%gnocchi%'
                or nombre_lower like '%puré%'
                or nombre_lower like '%hamburgesa%'
                or nombre_lower like '%libro%'
            )
        )
        and not (
            categoria = 'frutas' 
            and (
                length(nombre_lower) > 25
                or array_length(regexp_split_to_array(nombre_lower, '\s+'), 1) > 5
                or nombre_lower like '%limpiador%'
                or nombre_lower like '%desengrasante%'
                or nombre_lower like '%detergente%'
                or nombre_lower like '%lavandina%'
                or nombre_lower like '%perfume%'
                or nombre_lower like '%fragancia%'
                or nombre_lower like '%amargo%'
                or nombre_lower like '%jugo%'
                or nombre_lower like '%gelatina%'
                or nombre_lower like '%pure%'
                or nombre_lower like '%vinagre%'
                or nombre_lower like '%salsa%'
                or nombre_lower like '%barra%'
                or nombre_lower like '%yogur%'
                or nombre_lower like '%galletita%'
                or nombre_lower like '%bebida%'
                or nombre_lower like '%barrita%'
                or nombre_lower like '%agua%'
                or nombre_lower like '%sabor%'
                or nombre_lower like '%lavavajillas%'
                or nombre_lower like '%budin%'
                or nombre_lower like '%trenza%'
                or nombre_lower like '%torta%'
                or nombre_lower like '%postre%'
                or nombre_lower like '%postrecito%'
                or nombre_lower like '%accondicionador%'
                or nombre_lower like '%renovador%'
                or nombre_lower like '%avena%'
                or nombre_lower like '%cereal%'
                or nombre_lower like '%aromatizante%'
                or nombre_lower like '%auto%'
                or nombre_lower like '%hogar%'
                or nombre_lower like '%vodka%'
                or nombre_lower like '%tintura%'
                or nombre_lower like '%latex%'
                or nombre_lower like '%granola%'
                or nombre_lower like '%chips%'
                or nombre_lower like '%dulce%'
                or nombre_lower like '%mermelada%'
                or nombre_lower like '%chocolate%'
                or nombre_lower like '%helado%'
                or nombre_lower like '%loción%'
                or nombre_lower like '%locion%'
                or nombre_lower like '%protector%'
                or nombre_lower like '%pastilla%'
                or nombre_lower like '%banderin%'
                or nombre_lower like '%cuaderno%'
                or nombre_lower like '%champagne%'
                or nombre_lower like '%sidra%'
                or nombre_lower like '%media%'
                or nombre_lower like '%cono%'
                or nombre_lower like '%naranja%'
                or nombre_lower like '%chicle%'
                or nombre_lower like '%té%'
                or nombre_lower like '%traje%'
                or nombre_lower like '%relleno%'
                or nombre_lower like '%sabor%'
                or nombre_lower like '%máquina%'
                or nombre_lower like '%espuma%'
                or nombre_lower like '%jabón%'
                or nombre_lower like '%trans%'
                or nombre_lower like '%batidor%'
                or nombre_lower like '%gaseosa%'
                or nombre_lower like '%marq%'
                or nombre_lower like '%peluche%'
                or nombre_lower like '%mascarilla%'
                or nombre_lower like '%alfajor%'
                or nombre_lower like '%esencia%'
                or nombre_lower like '%artificial%'
                or nombre_lower like '%rellen%'
                or nombre_lower like '%oblea%'
                or nombre_lower like '%gomita%'
                or nombre_lower like '%gel%'
                or nombre_lower like '%licor%'
                or nombre_lower like '%gin%'
                or nombre_lower like '%cepita%'
                or nombre_lower like '%chupetin%'
                or nombre_lower like '%tinte%'
                or nombre_lower like '%acondicionador%'
                or nombre_lower like '%rojo%'
                or nombre_lower like '%azul%'
                or nombre_lower like '%champ%'
                or nombre_lower like '%color%'
                or nombre_lower like '%lacteo%'
                or nombre_lower like '%leche%'
                or nombre_lower like '%yogur%'
                or nombre_lower like '%postre%'
                or nombre_lower like '%nikito%'
                or nombre_lower like '%pulpa%'
                or nombre_lower like '%leche%'
                or nombre_lower like '%crema%'
                or nombre_lower like '%frita%'
                or nombre_lower like '%fizz%'
                or nombre_lower like '%bot%'
                or nombre_lower like '%energi%'
                or nombre_lower like '%azucar%'
                or nombre_lower like '%enguaje%'
                or nombre_lower like '%bowl%'
                or nombre_lower like '%yerba%'
                or nombre_lower like '%individual%'
                or nombre_lower like '%granulado%'
                or nombre_lower like '%arena%'
                or nombre_lower like '%cerveza%'
                or nombre_lower like '%pasta%'
                or nombre_lower like '%perfum%'
                or nombre_lower like '%aperitivo%'
                or nombre_lower like '%aceite%'
                or nombre_lower like '%picante%'
                or nombre_lower like '%saz%'
                or nombre_lower like '%dr%'
                or nombre_lower like '%deo%'
                or nombre_lower like '%libro%'
                or nombre_lower like '%te%'
                or nombre_lower like '%play%'
                or nombre_lower like '%aromat%'
                or nombre_lower like '%repuesto%'
                or nombre_lower like '%anti%'
                or nombre_lower like '%aceto%'
                or nombre_lower like '%suplemento%'
                or nombre_lower like '%snack%'
                or nombre_lower like '%crunch%'
                or nombre_lower like '%silicon%'
                or nombre_lower like '%mou%'
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
                or nombre_lower like '%sopa%'
                or nombre_lower like '%medallon%'
                or nombre_lower like '%nuggets%'
                or nombre_lower like '%empanada%'
                or nombre_lower like '%panzotti%'
                or nombre_lower like '%ravioles%'
                or nombre_lower like '%chicle%'
                or nombre_lower like '%tarta%'
                or nombre_lower like '%patita%'
                or nombre_lower like '%formitas%'
                or nombre_lower like '%crunch%'
                or nombre_lower like '%choclo%'
                
            )
        )
        and not (
            categoria = 'cerdo'
            and (
                nombre_lower like '%medallon%'
                or nombre_lower like '%pan%'
                or nombre_lower like '%sazonador%'
                or nombre_lower like '%pollo%'
                or nombre_lower like '%vaca%'
                or nombre_lower like '%pechuga%'
                or nombre_lower like '%cebolla%'
                or nombre_lower like '%parmesana%'
                or nombre_lower like '%paleta%'
                or nombre_lower like '%jamon%'
                or nombre_lower like '%jamón%'
                or nombre_lower like '%medallón%'
                or nombre_lower like '%chips%'
                or nombre_lower like '%papas%'
                or nombre_lower like '%salsa%'
                or nombre_lower like '%picada%'
                or nombre_lower like '%pimentón%'
                or nombre_lower like '%fiambre%'
                or nombre_lower like '%picante%'
                or nombre_lower like '%salame%'
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
                or nombre_lower like '%limpiador%'
                or nombre_lower like '%vegetal%'
                or nombre_lower like '%vacalin%'
                or nombre_lower like '%desinfectante%'
                or nombre_lower like '%verdura%'
                or nombre_lower like '%bizcochuelo%'
                or nombre_lower like '%cafe%'
                or nombre_lower like '%rollo%'
                or nombre_lower like '%escurridor%'
                or nombre_lower like '%fideos%'
                or nombre_lower like '%limón%'
                or nombre_lower like '%medallon%'
                or nombre_lower like '%pickles%'
                or nombre_lower like '%vinagre%'
                or nombre_lower like '%vaso%'
                or nombre_lower like '%ocular%'
                or nombre_lower like '%lavanda%'
                or nombre_lower like '%guante%'
                or nombre_lower like '%secador%'
                or nombre_lower like '%piso%'
                or nombre_lower like '%peluche%'
                or nombre_lower like '%multiuso%'
                or nombre_lower like '%antihumedad%'
                or nombre_lower like '%anticong%'
                or nombre_lower like '%escobillón%'
                or nombre_lower like '%vileda%'
                or nombre_lower like '%liliana%'
                or nombre_lower like '%cebo%'
                or nombre_lower like '%manteca%'
                or nombre_lower like '%milanesa%'
                or nombre_lower like '%café%'
                or nombre_lower like '%medallón%'
                or nombre_lower like '%licor%'
                or nombre_lower like '%cerdo%'
                or nombre_lower like '%pollo%'
                or nombre_lower like '%lavavajillas%'
                or nombre_lower like '%viruta%'
                or nombre_lower like '%cartucho%'
                or nombre_lower like '%seca%'
                or nombre_lower like '%vidrio%'
            )
        )
        and not (
            categoria in ('galletita', 'galletas, galletitas')
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
        and not (
            categoria in ('untables')
            and (
                nombre_lower like '%galletita%'
                or nombre_lower like '%budin%'
                or nombre_lower like '%oblea%'
                or nombre_lower like '%alfajor%'
                or nombre_lower like '%barra%'
                or nombre_lower like '%barrita%'
                or nombre_lower like '%salsa%'
                or nombre_lower like '%donas%'
                or nombre_lower like '%chocolate%'

            )
        )
        and not (
            categoria in ('harina')
            and (
                nombre_lower like '%tapa%'
                or nombre_lower like '%premezcla%'
                or nombre_lower like '%sabor%'

            )
        )
        and not (
            categoria in ('yerba_mate')
            and (
                nombre_lower like '%yerbero%'
                or nombre_lower like '%despolvillador%'
                or nombre_lower like '%matero%'
                or nombre_lower like '%saquito%'
                or nombre_lower like '%cocido%'
                or nombre_lower like '%té%'
                or nombre_lower like '%bebida%'
                or nombre_lower like '%lata%'
                or nombre_lower like '%azuc%'
                or nombre_lower like '%gin%'            )
        )     
        and not (
            categoria in ('azucar')
            and (
                nombre_lower like '%sin azucar%'
                or nombre_lower like '%sin azúcar%'
                or nombre_lower like '%matero%'
                or nombre_lower like '%amargo%'
                or nombre_lower like '%gaseosa%'
                or nombre_lower like '%jugo%'
                or nombre_lower like '%bebida%'
                or nombre_lower like '%edulcorante%'
                or nombre_lower like '%hileret%'
                or nombre_lower like '%stevia%'
                or nombre_lower like '%impalpable%'
                or nombre_lower like '%%'

            )
        )   
        and not (
            categoria = 'vino'
            and (
                nombre_lower like '%coci%'
                or nombre_lower like '%vinagre%'
                or nombre_lower like '%salsa%'
                or unidad_medida = 'KGS'
                or unidad_medida = 'kg'
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
            when categoria = 'galletita' then 'galletitas'
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