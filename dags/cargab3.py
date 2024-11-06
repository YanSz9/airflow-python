from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import os
import logging

def get_db_connection():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    return hook.get_conn()

def load_stage(data_chunk):
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
        
        for record in data_chunk:
            values = (
                str(record['tipo_registro']),
                str(record['data_pregao']),
                str(record['cod_bdi']),
                str(record['cod_negociacao']),
                str(record['tipo_mercado']),
                str(record['nome_empresa']),
                str(record['moeda']),
                float(record['preco_abertura']),
                float(record['preco_maximo']),
                float(record['preco_minimo']),
                float(record['preco_medio']),
                float(record['preco_ultimo_negocio']),
                float(record['preco_melhor_oferta_compra']),
                float(record['preco_melhor_oferta_venda']),
                str(record['numero_negocios']),
                str(record['quantidade_papeis_negociados']),
                str(record['volume_total_negociado']),
                str(record.get('codigo_isin', '')),
                str(record.get('num_distribuicao_papel', ''))
            )
            cursor.execute(insert_query, values)
        
        conn.commit()
        logging.info("Chunk inserido com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao inserir chunk: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def extract():
    logging.info("Iniciando a extração...")
    anos = ['2022', '2023', '2024']
    arquivos = [f'/opt/airflow/files/COTAHIST_A{ano}.txt' for ano in anos]
    
    separar_campos = [2, 8, 2, 12, 3, 12, 10, 3, 4, 13, 13, 13, 13, 13, 13, 13, 5, 18, 18, 13, 1, 8, 7, 13, 12, 3]
    colunas = [
        "tipo_registro", "data_pregao", "cod_bdi", "cod_negociacao", "tipo_mercado", 
        "nome_empresa", "especificacao_papel", "prazo_dias_merc_termo", "moeda", 
        "preco_abertura", "preco_maximo", "preco_minimo", "preco_medio", 
        "preco_ultimo_negocio", "preco_melhor_oferta_compra", "preco_melhor_oferta_venda", 
        "numero_negocios", "quantidade_papeis_negociados", "volume_total_negociado", 
        "preco_exercicio", "indicador_correcao_precos", "data_vencimento", 
        "fator_cotacao", "preco_exercicio_pontos", "codigo_isin", "num_distribuicao_papel"
    ]

    chunk_size = 10000
    
    try:
        for arquivo in arquivos:
            logging.info("Processando arquivo: %s", arquivo)
            
            if not os.path.isfile(arquivo):
                logging.error("Arquivo não encontrado: %s", arquivo)
                continue

            for chunk in pd.read_fwf(arquivo, widths=separar_campos, header=0, chunksize=chunk_size):
                logging.info("Lendo um bloco de dados...")
                
                chunk.columns = colunas
                
                dinheiro = [
                    "preco_abertura", "preco_maximo", "preco_minimo", 
                    "preco_medio", "preco_ultimo_negocio", 
                    "preco_melhor_oferta_compra", "preco_melhor_oferta_venda"
                ]

                for coluna in dinheiro:
                    if coluna in chunk.columns:
                        chunk[coluna] = chunk[coluna].astype(float) / 100

                chunk['data_pregao'] = pd.to_datetime(chunk['data_pregao'], format='%Y%m%d', errors='coerce')
                chunk['data_pregao'] = chunk['data_pregao'].dt.strftime('%d/%m/%Y')
                
                load_stage(chunk.to_dict(orient='records'))

    except Exception as e:
        logging.error("Erro na extração: %s", e)

def insert_calendar_data(conn):
    """Insere dados na tabela de calendário, convertendo data_pregao para DATE."""
    with conn.cursor() as cursor:
        cursor.execute(""" 
        INSERT INTO dim_calendario (data, dia, mes, ano, trimestre, dia_da_semana)
        SELECT DISTINCT 
            TO_DATE(data_pregao, 'DD/MM/YYYY') AS data, 
            EXTRACT(DAY FROM TO_DATE(data_pregao, 'DD/MM/YYYY')),
            EXTRACT(MONTH FROM TO_DATE(data_pregao, 'DD/MM/YYYY')),
            EXTRACT(YEAR FROM TO_DATE(data_pregao, 'DD/MM/YYYY')),
            EXTRACT(QUARTER FROM TO_DATE(data_pregao, 'DD/MM/YYYY')),
            TO_CHAR(TO_DATE(data_pregao, 'DD/MM/YYYY'), 'Day')
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
            JOIN dim_calendario dc ON TO_DATE(sc.data_pregao, 'DD/MM/YYYY') = dc.data;
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
    transform_and_enrich_task = PythonOperator(task_id='transform_and_enrich', python_callable=transform_and_enrich)

    extract_task >> transform_and_enrich_task
