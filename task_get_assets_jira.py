import pandas as pd
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from airflow.decorators import task
from airflow.providers.atlassian.jira.hooks.jira import JiraHook
from .dags.tasks_jira.utils import select_bq_jira, cnx_bigquery_load, add_dt_extracao

# Configurar logging básico
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_single_asset_detail(object_id):
    """
    Busca os detalhes de UM asset específico pelo seu ID dentro de um workspace.
    Retorna os dados do asset como um dicionário ou None em caso de erro/não encontrado.

    Args:
        object_id: ID de cada um dos assets.
    """
    
    # Conexão Jira
    hook = JiraHook(jira_conn_id="connection_jira")
    client = hook.get_conn()

    # Obtenção de headers e auth a partir da sessão autenticada
    session = client._session
    auth = session.auth
    HEADERS = session.headers

    try:

        url = f"https://api.atlassian.com/jsm/assets/workspace/89fd3be7-83fc-4384-a7d6-cbb8b8901bca/v1/object/{object_id}"
            
        if not object_id or pd.isna(object_id):
            return None
            
        #response = requests.request( "GET", url, headers=HEADERS, auth=auth, verify=True)
        response = requests.get(url, headers=HEADERS, auth=auth, verify=True, timeout=30)

        if response.status_code == 200:
            # Sucesso! Retorna o dicionário JSON do objeto diretamente
            return response.json()

        else:
            # Log de outros erros HTTP
            logging.error(f"🚨 Erro {response.status_code} ao buscar asset ID {object_id}: {response.text[:200]}") # Mostra início do erro
                

    except Exception as e: # Captura genérica para erros inesperados
        logging.error(f"🚨 Erro inesperado ao buscar asset ID {object_id}: {e}")
        
    return None


@task
def get_assets_jira(max_workers=10):
    """
    Busca os detalhes de múltiplos Assets (Objetos) de um workspace usando concorrência.

    Args:
        max_workers (int): Número máximo de threads simultâneas para as requisições.

    Returns:
        list: Uma lista de dicionários, onde cada dicionário contém os detalhes
              de um asset encontrado com sucesso. Retorna lista vazia se nenhum for encontrado
              ou em caso de erro na obtenção das credenciais.
    """

    query = "SELECT OBJECT_ID, COLUMN_NAME FROM `abc-ti-infra-provider-dev.DS_JIRA_CONTROLADO.TR_ASSETS_IDS`"

    df_ids = select_bq_jira(query)
    object_ids_list = df_ids['OBJECT_ID'].to_numpy().flatten().tolist()
   
    all_asset_details = []
    processed_ids = set() # Para evitar processar IDs duplicados na lista de entrada
    success_count = 0
    fail_count = 0
    ids_to_process = []

    for obj_id_raw in object_ids_list:    
         try:
            if obj_id_raw is None or pd.isna(obj_id_raw):
                continue # Ignora None/NaN
            obj_id_str = str(obj_id_raw).strip()
            
            if obj_id_str and obj_id_str not in processed_ids: # Ignora vazios e duplicados
                ids_to_process.append(obj_id_str)
                processed_ids.add(obj_id_str)
                
         except Exception as e:
            logging.warning(f"Não foi possível processar o ID '{obj_id_raw}': {e}")
    
    # Usando ThreadPoolExecutor para buscar em paralelo
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Mapeia o Future retornado pelo submit para o ID original
        future_to_id = {
            executor.submit(fetch_single_asset_detail, obj_id): obj_id
            for obj_id in ids_to_process 
        }

        # Processa os resultados conforme eles ficam prontos
        for future in as_completed(future_to_id):
            original_id = future_to_id[future]
            
            try:
                result_data = future.result() # Pega o resultado da função (dict ou None)
                if result_data:
                    all_asset_details.append(result_data)
                    success_count += 1
                else:
                    fail_count += 1 # O erro/aviso já foi impresso dentro da função fetch_single_asset_detail

            except Exception as exc:
                # Captura exceções que podem ter ocorrido DENTRO da função fetch_single_asset_detail
                # e não foram tratadas lá, ou exceções do próprio executor.
                print(f"🚨 Exceção gerada ao processar o future para o ID {original_id}: {exc}")
                fail_count += 1
                
    if all_asset_details:
        df_detalhes_assets = pd.json_normalize(all_asset_details, sep='_')
        
        merge_dfs = pd.merge(df_detalhes_assets, df_ids, left_on='id', right_on='OBJECT_ID', how='right')
        
    if not merge_dfs.empty:
        # Converte a lista de prioridades em um DataFrame com uma única coluna JSON_DATA
        df_assets = pd.DataFrame({"JSON_DATA": [json.dumps(asset) for asset in all_asset_details]})

        # Adiciona a nova coluna ao DataFrame com a chave do chamado
        df_assets['ID_ASSETS'] = merge_dfs['OBJECT_ID']

        # Adiciona a nova coluna ao DataFrame com a categoria do ID
        df_assets['CATEGORIA_ID'] = merge_dfs['COLUMN_NAME']

        # Adiciona a nova coluna ao DataFrame com o timestamp
        df_assets['API'] = 'ASSETS'

        # Adiciona a nova coluna ao DataFrame com a data de extracao
        df_assets = add_dt_extracao(df_assets)  

        # Seleciona apenas as colunas desejadas
        df = df_assets[['JSON_DATA', 'ID_ASSETS', 'API', 'CATEGORIA_ID' ,'DT_EXTRACAO']]  

        # Carregamento do DataFrame diretamente para a tabela do BigQuery do DS_JIRA
        cnx_bigquery_load(df)
        
    else:
        print("Nenhum dado de ASSETS encontrado.")
        
    return df