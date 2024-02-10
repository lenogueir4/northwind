with 
    fonte_fornecedores as (
        select
            cast (supplier_id as int) as id_fornecedor
            , cast(company_name as string) as nome_fornecedor
            , cast(contact_name as string) as nome_contato_fornecedor
            , cast(contact_title as string) as cargo_contato_fornecedor
            --, cast(address as string) as endereco_fornecedor --nao usada na regra de negocio
            , cast(city as string) as cidade_fornecedor
            , cast(region as string) as regiao_fornecedor
            --, cast(postal_code as string) as cep_fornecedor --nao usada na regra de negocio
            , cast(country as string) as pais_fornecedor
            --, cast(phone as string) as telefone_fornecedor --nao usada na regra de negocio
            --, cast(fax as string) as fax_fornecedor --nao usada na regra de negocio
            --, cast(homepage as string) as www_fornecedor --nao usada na regra de negocio
        from {{ source('erp_northwind', 'suppliers') }}
    )

select * 
from fonte_fornecedores
