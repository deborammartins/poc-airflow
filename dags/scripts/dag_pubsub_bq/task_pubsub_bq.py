import json
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def consume_and_insert(project_id, subscription_id, table_id, gcp_conn_id="connection_bq", max_messages=10):
    """
    Consome mensagens do Pub/Sub em batch e insere no BigQuery.

    Args:
        project_id (str): ID do projeto do Google Cloud.
        subscription_id (str): ID da assinatura do Pub/Sub.
        table_id (str): ID da tabela do BigQuery onde inserir os dados.
        gcp_conn_id (str): ID da conexão do Airflow para o BigQuery.
        max_messages (int): Quantidade máxima de mensagens a puxar em um lote.
    """

    # Configura o cliente do Pub/Sub com as credenciais
    credentials = service_account.Credentials.from_service_account_file(
        "/usr/local/airflow/include/credentials.json"
    )
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # Puxa mensagens em batch
    response = subscriber.pull(subscription=subscription_path, max_messages=max_messages)

    if not response.received_messages:
        print("Nenhuma mensagem para processar.")
        return

    ack_ids = []
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    client = hook.get_client()

    for msg in response.received_messages:
        try:
            print(f"Recebida mensagem: {msg.message.data}")
            payload = msg.message.data.decode("utf-8")
            row = json.loads(payload)

            errors = client.insert_rows_json(table_id, [row])
            if errors:
                print(f"Erro ao inserir: {errors}")
            else:
                print(f"Inserido: {row}")

            ack_ids.append(msg.ack_id)

        except Exception as e:
            print(f"Erro ao processar mensagem: {e}")

    # Acknowledge para liberar as mensagens processadas
    if ack_ids:
        subscriber.acknowledge(subscription=subscription_path, ack_ids=ack_ids)
        print(f"Ack das mensagens: {len(ack_ids)}")

