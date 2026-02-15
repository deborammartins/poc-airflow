# ---
# doc_md: Gera o schema a partir de um arquivo.
# dependencies:
#   - task_gcs_sensor_object_existence_bucket
# trigger_rule: all_success
# python_callable: task_py_generate_schema
# ---

import logging
log = logging.getLogger(__name__)

from plugins.my_utils.utils import (
    get_bigquery_schema_from_file,
    save_bq_schema_to_gcs
)

def task_py_generate_schema(**kwargs):
    '''
    Task que gera o schema para criar a tabela no BigQuery.
    
    kwargs: Parâmetros definidos no arquivo METADATA.yml
    '''

    delimiter = kwargs['params']['delimiter']
    file_format = kwargs['params']['file_format']
    gcs_path_file = f'gs://{kwargs["params"]["bucket"]}/{kwargs["params"]["file_name"]}'
    gcs_schema_bucket = kwargs['params']['gcs_schema_bucket']
    schema_file_name = kwargs['params']['schema_file_name']

    try:
        log.info("Iniciando a geração do schema a partir do arquivo GCS...")
        schema = get_bigquery_schema_from_file(file_path=gcs_path_file, file_format=file_format, delimiter=delimiter)

        if not schema:
            log.error("A função get_bigquery_schema_from_file retornou um schema vazio ou None. Abortando.")
            raise ValueError("Schema não pôde ser gerado a partir do arquivo de origem.") 

        log.info(f"Schema gerado com sucesso. Tipo: {type(schema)}")
        save_bq_schema_to_gcs(bq_schema=schema, bucket_name=gcs_schema_bucket, destination_blob_name=schema_file_name)
        log.info("--- FINALIZANDO EXECUÇÃO DA TAREFA: Gerar Schema (SUCESSO) ---")

    except Exception as e:
        log.error(f"--- FALHA NA EXECUÇÃO DA TAREFA: Erro ao recuperar params: {e} ---", exc_info=True)
        raise
