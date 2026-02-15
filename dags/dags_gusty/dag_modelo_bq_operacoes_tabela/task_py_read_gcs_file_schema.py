# ---
# doc_md: Gera o schema a partir de um arquivo.
# dependencies:
#   - task_bq_create_empty_table
# trigger_rule: all_success
# python_callable: task_py_read_gcs_file_schema
# ---

import logging
log = logging.getLogger(__name__)

from plugins.my_utils.utils import (
    read_schema_from_gcs_with_hook
)

def task_py_read_gcs_file_schema(ti, **kwargs):
    '''
    Task que lê um arquivo no bucket que contém o schema para criar a tabela no BigQuery.
    
    kwargs: Parâmetros definidos no arquivo METADATA.yml
    '''

    gcs_schema_bucket = kwargs['params']['gcs_schema_bucket']
    schema_file_name = kwargs['params']['schema_file_name']
    gcp_conn_id = kwargs['params']['gcp_conn_id']

    try:
        log.info("Iniciando a geração do schema a partir do arquivo GCS...")
        schema = read_schema_from_gcs_with_hook(bucket_name=gcs_schema_bucket, 
                                                file_name=schema_file_name, 
                                                gcp_conn_id=gcp_conn_id)

        if not schema:
            log.error("A função get_bigquery_schema_from_file retornou um schema vazio ou None. Abortando.")
            raise ValueError("Schema não pôde ser gerado a partir do arquivo de origem.") 

        ti.xcom_push(key="bigquery_schema", value=schema)
        log.info("--- FINALIZANDO EXECUÇÃO DA TAREFA: Ler arquivo de schema (SUCESSO) ---")

    except Exception as e:
        log.error(f"--- FALHA NA EXECUÇÃO DA TAREFA: Erro ao recuperar params: {e} ---", exc_info=True)
        raise
