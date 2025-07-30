from google.cloud import pubsub_v1
from google.oauth2 import service_account

def create_subscription(project_id: str, topic_id: str, subscription_id: str):
    credentials = service_account.Credentials.from_service_account_file(
        "/usr/local/airflow/include/credentials.json"
    )
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    publisher = pubsub_v1.PublisherClient(credentials=credentials)

    topic_path = publisher.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    try:
        subscriber.create_subscription(
            name=subscription_path, topic=topic_path
        )
        print(f"Subscrição criada: {subscription_path}")
    except Exception as e:
        print(f"Erro ao criar subscrição: {e}")
