from google.cloud import bigquery

def create_table(table_id: str, schema: dict):
    """
    Cria uma tabela no BigQuery com o esquema fornecido.
    """

    client = bigquery.Client()
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)  # evita erro se já existir
    print(f"Tabela criada: {table.full_table_id}")
