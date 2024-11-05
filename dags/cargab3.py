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

def extract():
    logging.info("Iniciando a extração...")
    anos = ['2022', '2023', '2024']
    arquivos = [f'/opt/airflow/files/COTAHIST_A{ano}.txt' for ano in anos]
    
    logging.info("Caminhos absolutos dos arquivos: %s", [os.path.abspath(arquivo) for arquivo in arquivos])
    
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
    acoes = ['PETR4', 'VALE3', 'MGLU3', 'RAIZ4', 'AMBP3', 'B3SA3', 'ABEV3', 'ITUB4', 'WEGE3', 'BBDC4', 'QUAL3']

    todos_dados = []
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
                
                chunk = chunk[chunk['cod_negociacao'].isin(acoes)]
                logging.info("Número de registros filtrados neste bloco: %d", chunk.shape[0])

                dinheiro = [
                    "preco_abertura", "preco_maximo", "preco_minimo", 
                    "preco_medio", "preco_ultimo_negocio", 
                    "preco_melhor_oferta_compra", "preco_melhor_oferta_venda"
                ]

                for coluna in dinheiro:
                    if coluna in chunk.columns:
                        logging.info("Antes da divisão - %s: %s", coluna, chunk[coluna].head())
                        chunk[coluna] = chunk[coluna].astype(float) / 100
                        logging.info("Depois da divisão - %s: %s", coluna, chunk[coluna].head())

                chunk['data_pregao'] = pd.to_datetime(chunk['data_pregao'], format='%Y%m%d', errors='coerce')
                chunk['data_pregao'] = chunk['data_pregao'].dt.strftime('%d/%m/%Y')

                todos_dados.extend(chunk.to_dict(orient='records'))

        logging.info("Total de registros extraídos: %d", len(todos_dados))
        return todos_dados

    except Exception as e:
        logging.error("Erro na extração: %s", e)
        return []


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
    
    load_stage_task = PythonOperator(
        task_id='load_stage',
        python_callable=load_stage,
        op_kwargs={'data': extract_task.output}
    )
    
    transform_and_enrich_task = PythonOperator(task_id='transform_and_enrich', python_callable=transform_and_enrich)

    extract_task >> load_stage_task >> transform_and_enrich_task
