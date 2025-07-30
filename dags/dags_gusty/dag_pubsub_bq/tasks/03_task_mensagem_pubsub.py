# ---
# operator: airflow.operators.python.PythonOperator
# python_callable: main
# depends_on: [create_subscription]
# ---

from dags.scripts.dag_pubsub_bq.task_mensagem import mensagem_pubsub

def main():

    mensagem_pubsub(
        project_id="pubsub-463417",
        topic_id="airflow-topic",
        mensagem = {"nome": "Débora", "idade": 32, "cidade": "São Paulo"}
    )
    print("Mensagem publicada com sucesso!")


