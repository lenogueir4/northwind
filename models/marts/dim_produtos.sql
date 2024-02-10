with
    stg_categorias as (
        select
            id_categoria
            , nome_categoria
            , descricao_categoria
            --, nome_categoria || descricao_categoria --concatena
        from {{ ref('stg_erp__categorias') }}
        --where pais_fornecedor = 'Brazil'
    )
    , stg_fornecedores as (
        select
            id_fornecedor
            , nome_fornecedor
            , nome_contato_fornecedor
            , cargo_contato_fornecedor
            , cidade_fornecedor
            , regiao_fornecedor
            , pais_fornecedor
        from {{ ref('stg_erp__fornecedores') }}
    )
    , stg_produtos as (
        select
            id_produto
            , id_fornecedor
            , id_categoria
            , nome_produto
            , quantidades_por_unidade
            , preco_unitario
            , unidades_em_estoque
            , unidades_por_ordem
            , nivel_reabastecimento
            , eh_descontinuado
        from {{ ref('stg_erp__produtos') }}
    )
    , joined_tabelas as (
        select
            stg_produtos.id_produto
            , stg_produtos.id_fornecedor
            , stg_produtos.id_categoria
            , stg_produtos.nome_produto
            , stg_produtos.quantidades_por_unidade
            , stg_produtos.preco_unitario
            , stg_produtos.unidades_em_estoque
            , stg_produtos.unidades_por_ordem
            , stg_produtos.nivel_reabastecimento
            , stg_produtos.eh_descontinuado
            , stg_categorias.nome_categoria
            , stg_fornecedores.nome_fornecedor
            , stg_fornecedores.regiao_fornecedor
            , stg_fornecedores.pais_fornecedor
        from stg_produtos --sempre começar o join pela tabela maior
        left join stg_categorias on
            stg_produtos.id_categoria = stg_categorias.id_categoria
        left join stg_fornecedores on
            stg_produtos.id_fornecedor = stg_fornecedores.id_fornecedor
    )

select *
from joined_tabelas
