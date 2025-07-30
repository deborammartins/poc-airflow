from google.cloud import pubsub_v1

def create_subscription(project_id: str, topic_id: str, subscription_id: str):
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = subscriber.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    try:
        subscriber.create_subscription(
            request={
                "name": subscription_path,
                "topic": topic_path,
                "ack_deadline_seconds": 30,
            }
        )
        print(f"Subscription criada: {subscription_path}")
    except Exception as e:
        if "AlreadyExists" in str(e):
            print(f"Subscription '{subscription_id}' já existe.")
        else:
            raise
