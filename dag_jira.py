from airflow.decorators import dag
from dags.scripts.dag_jira.task_get_issues_jira import get_issues_jira


@dag(
    start_date=None,
    schedule=None,
    catchup=False,
    default_args={
        "owner": "Astro", 
        "retries": 3},
    tags=["jira"],
)

def dag_jira():
    """
    Dag de orquestração do pipeline de extração de dados do Jira.
    Esta DAG executa a task de conexão com o Jira e a task de obtenção dos
    chamados atualizados após uma data específica, salvando os dados em um CSV.
    """

    get_issues_jira()


DAG = dag_jira()
