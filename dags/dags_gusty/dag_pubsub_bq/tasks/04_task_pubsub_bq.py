# ---
# operator: airflow.operators.python.PythonOperator
# python_callable: main
# depends_on: [messagem_pubsub]
# ---

from dags.scripts.dag_pubsub_bq.task_pubsub_bq import consume_and_insert

def main():

    consume_and_insert(
        project_id="pubsub-463417",
        subscription_id="airflow-pubsub-subscription",
        table_id="DS_PUBSUB.TABELA_PUBSUB"
    )


