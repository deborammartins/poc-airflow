# ---
# doc_md: Atualiza o schema da tabela no BigQuery.
# dependencies:
#   - task_py_read_gcs_file_schema
# trigger_rule: all_success
# python_callable: task_py_update_schema_table
# ---

import json
import logging
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

log = logging.getLogger(__name__)

def task_py_update_schema_table(ti, **kwargs):
    """
    Puxa o schema do XCom e usa o BigQueryHook para atualizar a tabela.
    """
    log.info("--- INICIANDO EXECUÇÃO DA TAREFA: Atualizar schema do BigQuery ---")
    
    project_id = kwargs['params']['project_id']
    dataset_id = kwargs['params']['dataset_id']
    table_id = kwargs['params']['table_id']
    gcp_conn_id = kwargs['params']['gcp_conn_id']
    
    schema_object = ti.xcom_pull(task_ids='task_py_read_gcs_file_schema', key='bigquery_schema')
    
    if not schema_object:
        raise ValueError("Não foi possível encontrar o schema no XCom da task 'task_py_read_gcs_file_schema'.")

    if isinstance(schema_object, str):
        log.warning("O schema foi recebido como string! Fazendo parse com json.loads().")
        try:
            schema_fields_updates = json.loads(schema_object.replace("'", "\"")) 
        except json.JSONDecodeError:
            log.error("Falha ao fazer o parse da string do schema. Conteúdo: %s", schema_object)
            raise
    else:
        schema_fields_updates = schema_object
    
    try:
        log.info(f"Atualizando o schema da tabela {project_id}.{dataset_id}.{table_id}")
        bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
        
        bq_hook.update_table_schema(
            dataset_id=dataset_id,
            table_id=table_id,
            project_id=project_id,
            schema_fields_updates=schema_fields_updates,
            include_policy_tags=False
        )
        
        log.info("--- FINALIZANDO EXECUÇÃO DA TAREFA: Schema atualizado com SUCESSO ---")

    except Exception as e:
        log.error(f"--- FALHA NA EXECUÇÃO DA TAREFA: {e} ---", exc_info=True)
        raise