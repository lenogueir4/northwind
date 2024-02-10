with 
    fonte_produtos as (
        select
            cast(product_id as int) as id_produto
            , cast(supplier_id as int) as id_fornecedor
            , cast(category_id as int) as id_categoria
            , cast(product_name as string) as nome_produto
            , cast(quantity_per_unit as string) as quantidades_por_unidade
            , cast(unit_price as numeric) as preco_unitario
            , cast(units_in_stock as int) as unidades_em_estoque
            , cast(units_on_order as int) as unidades_por_ordem
            , cast(reorder_level as int) as nivel_reabastecimento
            , case 
                when discontinued = 1 then 'sim'
                else 'nao'
            end as eh_descontinuado
        from {{ source('erp_northwind', 'products') }}
    )

select * 
from fonte_produtos
