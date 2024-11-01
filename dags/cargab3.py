from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import os
import logging

def get_db_connection():
    """Obtém uma conexão com o banco de dados PostgreSQL."""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    return hook.get_conn()

def limpar_dados(coluna):
    """Limpa a coluna e a converte para string.""" 
    coluna = coluna.astype(str).str.replace('R\$', '', regex=True).str.strip()
    coluna = coluna.str.replace('.', '', regex=True)
    coluna = coluna.str.replace(',', '.', regex=True)
    return coluna

def extract():
    logging.info("Iniciando a extração...")
    arquivo = '/opt/airflow/files/COTAHIST_A2024.txt'
    logging.info("Caminho absoluto do arquivo: %s", os.path.abspath(arquivo))
    
    separar_campos = [2, 8, 2, 12, 3, 12, 4, 13, 13, 13, 13, 13, 13, 13, 5, 18, 18, 12, 3]
    colunas = [
        "tipo_registro", "data_pregao", "cod_bdi", "cod_negociacao", "tipo_mercado",
        "nome_empresa", "moeda", "preco_abertura", "preco_maximo", "preco_minimo",
        "preco_medio", "preco_ultimo_negocio", "preco_melhor_oferta_compra",
        "preco_melhor_oferta_venda", "numero_negocios", "quantidade_papeis_negociados",
        "volume_total_negociado", "codigo_isin", "num_distribuicao_papel"
    ]
    
    acoes = ['PETR4', 'VALE3', 'MGLU3', 'RAIZ4', 'AMBP3', 'B3SA3', 'ABEV3', 'ITUB4', 'WEGE3']

    todos_dados = []

    chunk_size = 10000
    for chunk in pd.read_fwf(arquivo, widths=separar_campos, header=0, chunksize=chunk_size):
        logging.info("Lendo um bloco de dados...")
        
        if chunk.shape[1] != len(colunas):
            logging.error("Número de colunas lidas (%d) não corresponde ao número de colunas definidas (%d)", 
                          chunk.shape[1], len(colunas))
            raise ValueError("Número de colunas não compatível.")

        chunk.columns = colunas
        chunk = chunk[chunk['cod_negociacao'].isin(acoes)]
        logging.info("Número de registros filtrados neste bloco: %d", chunk.shape[0])

        colunas_a_limpar = [
            "preco_abertura",
            "preco_maximo",
            "preco_minimo",
            "preco_medio",
            "preco_ultimo_negocio",
            "preco_melhor_oferta_compra",
            "preco_melhor_oferta_venda"
        ]

        for coluna in colunas_a_limpar:
            if coluna in chunk.columns:
                chunk[coluna] = limpar_dados(chunk[coluna])

        todos_dados.extend(chunk.to_dict(orient='records'))

    logging.info("Total de registros extraídos: %d", len(todos_dados))
    return todos_dados

def load_stage(data):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        logging.info("Inserindo dados na tabela stage...")

        insert_query = """
        INSERT INTO stage_cotahist (
            tipo_registro,
            data_pregao,
            cod_bdi,
            cod_negociacao,
            tipo_mercado,
            nome_empresa,
            moeda,
            preco_abertura,
            preco_maximo,
            preco_minimo,
            preco_medio,
            preco_ultimo_negocio,
            preco_melhor_oferta_compra,
            preco_melhor_oferta_venda,
            numero_negocios,
            quantidade_papeis_negociados,
            volume_total_negociado,
            codigo_isin,
            num_distribuicao_papel
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        if data:
            logging.info("Dados a serem inseridos: %s", data)

            for record in data:
                values = (
                    str(record['tipo_registro']),
                    str(record['data_pregao']),
                    str(record['cod_bdi']),
                    str(record['cod_negociacao']),
                    str(record['tipo_mercado']),
                    str(record['nome_empresa']),
                    str(record['moeda']),
                    str(record['preco_abertura']),
                    str(record['preco_maximo']),
                    str(record['preco_minimo']),
                    str(record['preco_medio']),
                    str(record['preco_ultimo_negocio']),
                    str(record['preco_melhor_oferta_compra']),
                    str(record['preco_melhor_oferta_venda']),
                    str(record['numero_negocios']),
                    str(record['quantidade_papeis_negociados']),
                    str(record['volume_total_negociado']),
                    str(record.get('codigo_isin', '')),
                    str(record.get('num_distribuicao_papel', ''))
                )

                logging.info("Executando a inserção com os valores: %s", values)
                cursor.execute(insert_query, values)
            
            conn.commit()
            logging.info("Dados inseridos com sucesso.")
        else:
            logging.warning("Nenhum dado para inserir na tabela stage.")

    except Exception as e:
        logging.error(f"Erro ao inserir dados: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

def insert_calendar_data(conn):
    """Insere dados na tabela de calendário, convertendo data_pregao para DATE."""
    with conn.cursor() as cursor:
        cursor.execute("""
        INSERT INTO dim_calendario (data, dia, mes, ano, trimestre, dia_da_semana)
        SELECT DISTINCT 
            TO_DATE(data_pregao, 'YYYYMMDD') AS data, 
            EXTRACT(DAY FROM TO_DATE(data_pregao, 'YYYYMMDD')),
            EXTRACT(MONTH FROM TO_DATE(data_pregao, 'YYYYMMDD')),
            EXTRACT(YEAR FROM TO_DATE(data_pregao, 'YYYYMMDD')),
            EXTRACT(QUARTER FROM TO_DATE(data_pregao, 'YYYYMMDD')),
            TO_CHAR(TO_DATE(data_pregao, 'YYYYMMDD'), 'Day')
        FROM stage_cotahist
        WHERE data_pregao IS NOT NULL AND data_pregao <> ''
        ON CONFLICT (data) DO NOTHING;
        """)
        conn.commit()

def transform_and_enrich():
    """Transforma e enriquece os dados da tabela de stage."""
    conn = get_db_connection()
    
    try:
        insert_calendar_data(conn)

        with conn.cursor() as cursor:
            cursor.execute(""" 
            INSERT INTO dim_ativos (ativo) 
            SELECT DISTINCT cod_negociacao FROM stage_cotahist 
            ON CONFLICT (ativo) DO NOTHING;
            """)

            cursor.execute(""" 
            INSERT INTO fato_cotahist (ativo_id, data_id, preco_abertura, preco_fechamento, volume) 
            SELECT da.id, dc.id, sc.preco_abertura, sc.preco_ultimo_negocio, sc.volume_total_negociado 
            FROM stage_cotahist sc 
            JOIN dim_ativos da ON sc.cod_negociacao = da.ativo
            JOIN dim_calendario dc ON TO_DATE(sc.data_pregao, 'YYYYMMDD') = dc.data;
            """)

        conn.commit()
        logging.info("Dados transformados e enriquecidos com sucesso.")

    except Exception as e:
        logging.error("Erro ao transformar e enriquecer dados: %s", e)
        conn.rollback()

    finally:
        conn.close()

with DAG('cotahist_etl', schedule_interval='@daily', start_date=datetime(2024, 1, 1), catchup=False) as dag:
    extract_task = PythonOperator(task_id='extract', python_callable=extract)
    
    load_stage_task = PythonOperator(
        task_id='load_stage',
        python_callable=load_stage,
        op_kwargs={'data': extract_task.output}
    )
    
    transform_and_enrich_task = PythonOperator(task_id='transform_and_enrich', python_callable=transform_and_enrich)

    extract_task >> load_stage_task >> transform_and_enrich_task
