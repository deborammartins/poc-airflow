import base64
import json
from google.cloud import pubsub_v1
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def consume_and_insert(project_id: str, subscription_id: str, table_id: str, gcp_conn_id: str):
    """
    Consome mensagens do Pub/Sub e as insere no BigQuery.

    Args:
        project_id (str): ID do projeto GCP.
        subscription_id (str): ID da assinatura do Pub/Sub.
        table_id (str): ID da tabela BigQuery no formato 'dataset.table'.
        gcp_conn_id (str): ID da conexão GCP configurada no Airflow.    
    """

    # Cria client BigQuery a partir da connection do Airflow
    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
    bq_client = bq_hook.get_client()

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message):
        try:
            data = base64.b64decode(message.data).decode("utf-8")
            row = json.loads(data)

            # Inserindo a mensagem no BigQuery
            errors = bq_client.insert_rows_json(table_id, [row])
            if errors:
                print(f"Erro ao inserir: {errors}")
            else:
                print(f"Inserido: {row}")
                message.ack()
        except Exception as e:
            print(f"Falha: {e}")
            message.nack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Ouvindo {subscription_path}...")

    try:
        streaming_pull_future.result(timeout=30)
    except Exception as e:
        streaming_pull_future.cancel()
        print(f"Encerrado: {e}")
