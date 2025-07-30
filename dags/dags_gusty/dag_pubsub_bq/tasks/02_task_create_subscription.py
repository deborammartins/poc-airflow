# ---
# operator: airflow.operators.python.PythonOperator
# python_callable: main
# depends_on: [create_table]
# ---

from dags.scripts.dag_pubsub_bq.task_create_subscription import create_subscription

def main():

    create_subscription(
        project_id="pubsub-463417",
        topic_id="airflow-topic",
        subscription_id="airflow-pubsub-subscription",
    )


