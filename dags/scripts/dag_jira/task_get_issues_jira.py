from airflow.decorators import task
from airflow.providers.atlassian.jira.hooks.jira import JiraHook
import pandas as pd
import json
import requests
import logging
from datetime import datetime

@task
def get_issues_jira():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Pega a conexão do Airflow
    hook = JiraHook(jira_conn_id="connection_jira")
    connection = hook.get_connection(hook.jira_conn_id)

    jira_base_url = connection.host  
    user = connection.login          
    api_token = connection.password  

    endpoint = "/rest/api/3/search"
    max_results = 100
    start_at = 0
    all_issues = []

    headers = {
        "Accept": "application/json"
    }
    

    auth = (user, api_token)

    while True:
        jql_query = 'updated >= -1d ORDER BY updated DESC'

        params = {
            "jql": jql_query,
            "maxResults": max_results,
            "startAt": start_at,
            "fields": "*all"
        }

        url = f"{jira_base_url}{endpoint}"

        try:
            response = requests.get(url, headers=headers, auth=auth, params=params, verify=False)  # cuidado com verify=False
            response.raise_for_status()  
            data = response.json()
            issues = data.get("issues", [])

            for issue in issues:
                all_issues.append({
                    "id": issue.get("id"),
                    "key": issue.get("key"),
                    "fields": issue.get("fields", {}),
                    "JSON_DATA": json.dumps(issue)
                })

            logging.info(f"Buscados {len(issues)} chamados. Total até agora: {len(all_issues)}")

            start_at += len(issues)

            if start_at >= data.get('total', 0):
                break

            if not issues:
                logging.info("Busca concluída. Não há mais chamados para retornar.")
                break

        except requests.exceptions.HTTPError as e:
            logging.error(f"Erro HTTP ao buscar dados do Jira: {e}")
            logging.error(f"Response Body: {response.text}")
            break
        except Exception as e:
            logging.error(f"Ocorreu um erro inesperado: {e}")
            break

    if not all_issues:
        logging.warning("Nenhum chamado encontrado.")
        return

    logging.info(f"Total de chamados encontrados: {len(all_issues)}")

    df = pd.DataFrame({
        "JSON_DATA": [json.dumps(issue) for issue in all_issues],
        "ISSUE_KEY": [i["key"] for i in all_issues],
        "API": "ISSUES",
        "DT_EXTRACAO": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

    output_path = "/tmp/issues.csv"
    df.to_csv(output_path, index=False)
    logging.info(f"Arquivo CSV salvo com sucesso em {output_path}.")

    return None
