use dados_abertos_gov_br;
create schema if not exists mart_receita_federal;

create or replace table mart_receita_federal.estabelecimentos_cnaes_tb as (
  with
    estabelecimentos as (
      select
        cnpj_basico
        , cnpj_ordem
        , cnpj_dv
        , identificador_matriz_filial
        , nome_fantasia
        , situacao_cadastral
        , data_situacao_cadastral
        , motivo_situacao_cadastral
        , nome_da_cidade_no_exterior
        , pais
        , data_inicio_atividade
        , cnae_fiscal_principal
        , cnae_fiscal_secundaria
        , tipo_logradouro
        , logradouro
        , numero
        , complemento
        , bairro
        , cep
        , uf
        , municipio
        , ddd1
        , telefone1
        , ddd2
        , telefone2
        , ddd_fax
        , fax
        , correio_eletronico
        , situacao_especial
        , data_situacao_especial
      from src_receita_federal.estabelecimentos
    )
  
    , empresas as (
      select 
        cnpj_basico
        , razao_social
        , natureza_juridica
        , qualificacao_responsavel
        , capital_social
        , porte_da_empresa
        , ente_federativo_responsavel
      from src_receita_federal.empresas
    )
    
    , cnaes as (
        select
          codigo as cod_cnae
          , descricao as descricao_cnae
        from src_receita_federal.cnaes
    )
    
    , paises as (
        select
          codigo as cod_pais
          , descricao as nome_pais
        from src_receita_federal.paises
    )
  
    , municipios as (
        select
          codigo as cod_municipio
          , descricao as nome_municipio
        from src_receita_federal.municipios
    )
    
    , motivo as (
      select
        codigo as cod_motivo
        , descricao as descricao_motivo
      from src_receita_federal.motivo_situacao_cadastral
    )
  
    , qualificacao_socios as (
      select
        codigo as cod_qualificacao_socio
        , descricao as descricao_qualificacao_socio
      from src_receita_federal.qualificacao_socio
    )
  
    , socios as ( 
      select 
        cast(cnpj_basico as int) as cnpj_basico
        , identificador_de_socio
        , nome_socio
        , cnpj_cpf_do_socio
        -- , qualificacao_do_socio
        , q_soc.descricao_qualificacao_socio 
        , data_entrada_sociedade
        , pais as cod_pais_socio
        , paises.nome_pais as nome_pais_socio
        , representante_legal
        , nome_representante
        , qualificacao_do_representante
        , faixa_etaria
      from src_receita_federal.socios 
      left join paises on socios.pais = paises.cod_pais
      left join qualificacao_socios q_soc on socios.qualificacao_do_socio = q_soc.cod_qualificacao_socio
    )
  
    , dados_simples as (
      select
        cnpj_basico
        , opcao_pelo_simples
        , data_opcao_pelo_simples
        , data_exclusao_do_simples
        , opcao_pelo_mei
        , data_opcao_pelo_mei
        , data_exclusao_do_mei
      from src_receita_federal.dados_do_simples
    )
  
    , estabelecimentos_cnaes as ( 
      select
        -- estabelecimentos
          estab.cnpj_basico
          , estab.cnpj_ordem
          , estab.cnpj_dv
          , estab.identificador_matriz_filial
          , estab.nome_fantasia
          , estab.situacao_cadastral
          , estab.data_situacao_cadastral
          , estab.motivo_situacao_cadastral
          , estab.nome_da_cidade_no_exterior
          , estab.pais
          , estab.data_inicio_atividade
          , estab.cnae_fiscal_principal
          , estab.cnae_fiscal_secundaria
          , estab.tipo_logradouro
          , estab.logradouro
          , estab.numero
          , estab.complemento
          , estab.bairro
          , estab.cep
          , estab.uf
          , estab.municipio
          , estab.ddd1
          , estab.telefone1
          , estab.ddd2
          , estab.telefone2
          , estab.ddd_fax
          , estab.fax
          , estab.correio_eletronico
          , estab.situacao_especial
          , estab.data_situacao_especial
      -- empresas
          , empresas.razao_social
          , empresas.natureza_juridica
          , empresas.qualificacao_responsavel
          , empresas.capital_social
          , empresas.porte_da_empresa
          , empresas.ente_federativo_responsavel
      --cnaes
          , cnaes.cod_cnae
          , cnaes.descricao_cnae
      -- geografico
        , municipios.cod_municipio
        , municipios.nome_municipio
        , paises.cod_pais
        , paises.nome_pais
      -- motivo situacao cadastral
        , motivo.cod_motivo
        , motivo.descricao_motivo
      -- socios
        , socios.identificador_de_socio
        , socios.nome_socio
        , socios.cnpj_cpf_do_socio
        , socios.descricao_qualificacao_socio
        , socios.data_entrada_sociedade
        , socios.cod_pais_socio
        , socios.nome_pais_socio
        , socios.representante_legal
        , socios.nome_representante
        , socios.qualificacao_do_representante
        , socios.faixa_etaria
      from estabelecimentos estab 
      left join empresas on estab.cnpj_basico = empresas.cnpj_basico
      left join cnaes on estab.cnae_fiscal_principal = cnaes.cod_cnae
      left join municipios on estab.municipio = municipios.cod_municipio
      left join paises on estab.pais = paises.cod_pais
      left join motivo on estab.motivo_situacao_cadastral = motivo.cod_motivo
      left join socios on estab.cnpj_basico = socios.cnpj_basico
    )
  select * 	 
  from estabelecimentos_cnaes
)