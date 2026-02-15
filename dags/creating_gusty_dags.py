from gusty import create_dags
import os

# Caminho completo da pasta onde estão os arquivos METADATA.yml e tasks
gusty_dags_path = os.path.join(os.path.dirname(__file__), "dags_gusty")

# Criação automática das DAGs
create_dags(
    dags_dir=gusty_dags_path,
    caller_env=globals(),  # necessário para o Airflow registrar as DAGs
    description="Loader do Gusty para criar as dags do projeto poc-airflow",
    default_args={"owner": "data-eng"},
    catchup=False,
    latest_only=False
)
