from google.cloud import pubsub_v1
from datetime import datetime, timezone
import json

def mensagem_pubsub(project_id: str, topic_id: str, mensagem: dict = None):
    """
    Publica uma mensagem no Pub/Sub com a data e hora de envio.

    Args:
        project_id (str): ID do projeto do Google Cloud.
        topic_id (str): ID do tópico do Pub/Sub.
        mensagem (dict): Mensagem a ser publicada.

    Returns:
        str: ID da mensagem publicada.
    """

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    if mensagem is None:
        mensagem = {"message": "Mensagem sem conteúdo"}

    # Adiciona data/hora de envio em UTC
    mensagem["data_envio"] = datetime.now(timezone.utc).isoformat()

    # Serializa para JSON (boa prática em vez de str)
    mensagem_bytes = json.dumps(mensagem).encode("utf-8")

    future = publisher.publish(topic_path, mensagem_bytes)
    print(f"Mensagem publicada com ID: {future.result()}")
    return future.result()