# ---
# operator: airflow.operators.python.PythonOperator
# python_callable: main
# ---

from dags.scripts.dag_jira.task_get_issues_jira import get_issues_jira

def main():
    get_issues_jira()
    return None
