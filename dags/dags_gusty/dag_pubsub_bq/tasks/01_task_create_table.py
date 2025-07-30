# ---
# operator: airflow.operators.python.PythonOperator
# python_callable: main
# ---

from dags.scripts.dag_pubsub_bq.task_create_table import create_table
from google.cloud import bigquery

def main():
    """
    Função principal para criar uma tabela no BigQuery.
    """

    # ID completo da tabela no formato 'projeto.dataset.tabela'
    table_id = "pubsub-463417.DS_PUBSUB.TABELA_PUBSUB"

    # Definição do esquema da tabela
    schema = [
        bigquery.SchemaField("nome", "STRING"),
        bigquery.SchemaField("idade", "INTEGER"),
        bigquery.SchemaField("cidade", "STRING"),
        bigquery.SchemaField("data_envio", "TIMESTAMP"),
    ]

    # Cria a tabela
    create_table(table_id, schema)
