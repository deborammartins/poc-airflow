import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta, timezone
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

Schema = [
{"name": "JSON_DATA",
"type": "STRING"},
{"name": "API",
"type": "STRING"},
{"name": "DT_EXTRACAO",
"type": "TIMESTAMP"}
]


def job_config():
    # Configuração do job para o carregamento
    config = bigquery.LoadJobConfig(
        schema=Schema,  # Esquema da tabela fornecido
        autodetect=False,  # Definir como False se o esquema já estiver especificado
        create_disposition="CREATE_NEVER", # CREATE_IF_NEEDED - CREATE_NEVER 
        write_disposition="WRITE_APPEND", # WRITE_TRUNCATE - WRITE_APPEND - WRITE_EMPTY 
        allow_jagged_rows=True,
        allow_quoted_newlines=True,
        max_bad_records=100,
        ignore_unknown_values=True
    )
    return config


def cnx_bigquery_load(dataframe):
    # Configuração do BigQuery
    PROJECT_ID = "abc-ti-infra-provider-dev"
    DATASET_ID = "DS_JIRA"
    TABLE_ID = "TB_API_JIRA"

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        # Inicializa o cliente do BigQuery
        client = bigquery.Client()
        
        # Carregamento do DataFrame diretamente para a tabela
        job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config())
        job.result()  # Aguarda a conclusão do job
        
        # print(f"Dados carregados com sucesso para a tabela: {TABLE_ID}")
    except Exception as e:
        print(f"Erro ao inserir os dados no BigQuery: {e}")
        raise


def select_bq_jira(query):
    try:
        # Constrói o cliente do BigQuery (certifique-se de configurar as credenciais corretamente)
        client = bigquery.Client()
        
        # Executa a query e converte o resultado para um DataFrame
        df = client.query(query).to_dataframe()

        return df
        
    except Exception as e:
        print(f"Erro ao executar consulta de dados no BigQuery: {e}")
    

def add_dt_extracao(dataframe):
    """
    Adiciona um campo 'DT_EXTRACAO' com o timestamp atual em UTC.

    Args:
        dataframe (pd.DataFrame): O DataFrame ao qual a coluna será adicionada.

    Returns:
        pd.DataFrame: O DataFrame com a nova coluna 'DT_EXTRACAO'.
    """
    try:
        # 1. Pega a hora atual em UTC. 
        #    Usar datetime.utcnow() é mais direto para esta abordagem.
        timestamp_utc = datetime.now(timezone.utc)
        
        # 2. A MÁGICA: Subtrai 3 horas do tempo atual
        horario_ajustado = timestamp_utc - timedelta(hours=3)
        
        # 3. Adiciona a nova coluna ao DataFrame com o horário ajustado
        dataframe['DT_EXTRACAO'] = horario_ajustado
        
        return dataframe
    except Exception as e:
        print(f"Erro ao adicionar a coluna 'DT_EXTRACAO': {e}")
        raise


def call_sp_bq(PROJECT_ID: str, DATASET_ID: str, SP_NAME: str):
    """
    Chama a stored procedure no BigQuery e loga o resultado.
    Levanta uma exceção em caso de erro.
    """
    CLIENT = bigquery.Client(project=PROJECT_ID)
    BQ_FULL_SP_ID = f"`{PROJECT_ID}.{DATASET_ID}.{SP_NAME}`"
    
    logging.info(f"Iniciando a chamada da stored procedure: {BQ_FULL_SP_ID}")
    start_time = datetime.now()

    try:
        query = f"CALL {BQ_FULL_SP_ID}()"
        query_job = CLIENT.query(query)
        query_job.result()  

        end_time = datetime.now()
        duration = end_time - start_time

        success_message = f"Stored procedure {SP_NAME} executada com sucesso em {duration}."
        logging.info(success_message)

    except Exception as e:
        logging.error(f"FALHA ao executar a stored procedure {SP_NAME}: {e}", exc_info=True)
        raise e
    
