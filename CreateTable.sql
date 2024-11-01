CREATE TABLE IF NOT EXISTS stage_cotahist (
    id SERIAL PRIMARY KEY,
    tipo_registro VARCHAR,
    data_pregao VARCHAR,
    cod_bdi VARCHAR,
    cod_negociacao VARCHAR(12),
    tipo_mercado VARCHAR,
    nome_empresa VARCHAR(50),
    moeda VARCHAR(3),
    preco_abertura VARCHAR,
    preco_maximo VARCHAR,
    preco_minimo VARCHAR,
    preco_medio VARCHAR,
    preco_ultimo_negocio VARCHAR,
    preco_melhor_oferta_compra VARCHAR,
    preco_melhor_oferta_venda VARCHAR,
    numero_negocios VARCHAR,
    quantidade_papeis_negociados VARCHAR,
    volume_total_negociado VARCHAR,
    codigo_isin VARCHAR(12),
    num_distribuicao_papel VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_calendario (
    id SERIAL PRIMARY KEY,
    data DATE UNIQUE,
    dia INT,
    mes INT,
    ano INT,
    trimestre INT,
    dia_da_semana VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dim_ativos (
    id SERIAL PRIMARY KEY,
    ativo VARCHAR(20) UNIQUE
);

CREATE TABLE IF NOT EXISTS fato_cotahist (
    id SERIAL PRIMARY KEY,
    ativo_id INT,
    data_id INT,
    preco_abertura VARCHAR,
    preco_fechamento VARCHAR,
    volume VARCHAR,
    FOREIGN KEY (ativo_id) REFERENCES dim_ativos(id),
    FOREIGN KEY (data_id) REFERENCES dim_calendario(id)
);