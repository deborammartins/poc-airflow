from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

def create_table(table_id: str, schema: dict):
    """
    Cria uma tabela no BigQuery com o esquema fornecido.
    """

    hook = BigQueryHook(gcp_conn_id="connection_bq")
    client = hook.get_client()
    table = bigquery.Table(table_id, schema=[bigquery.SchemaField(**field) for field in schema])
    client.create_table(table, exists_ok=True)
    print(f"Tabela criada: {table.full_table_id}")
