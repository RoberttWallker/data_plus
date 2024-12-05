from pathlib import Path
import ijson
import re
import json
from sqlalchemy import inspect, Table, Column, Text, text


from .db_connector import (
    mysql_connection,
    postgresql_connection,
    load_config_file,
    load_db_config,
)


MODEL_PATH = Path(__file__).absolute().parent.parent
CONFIG_PATH = MODEL_PATH / "config"
TEMP_FILE_PATH = MODEL_PATH / "data/temp_file_data"


def obter_colunas(arquivo):
    with open(arquivo, "r", encoding="utf-8") as f:
        try:
            # Tenta acessar como uma lista de listas
            parser = ijson.items(f, "item.item")
            primeiro_dicionario = next(parser)
        except StopIteration:
            # Se falhar, volta ao início do arquivo e tenta como lista simples
            f.seek(0)  # Reinicia a leitura do arquivo
            parser = ijson.items(f, "item")
            primeiro_dicionario = next(parser)

        return primeiro_dicionario


def tabelas_e_colunas():
    tabela_e_colunas = []
    for file in TEMP_FILE_PATH.rglob("*.json"):
        file_name = file.name.split("Grid.")[0]
        table_name = re.sub(r"([a-z])([A-Z])", r"\1_\2", file_name).lower()
        colunas = obter_colunas(file)
        tabela_e_colunas.append((table_name, colunas))
    return tabela_e_colunas


def insert_into_db():
    for file in CONFIG_PATH.rglob("db_config/*.json"):
        db_configs = load_config_file(file)
        for config in db_configs:
            print(type(config))
            print(config)

            perfil = tabelas_e_colunas()

            if file.name == "db_config_mysql.json":
                conn = mysql_connection(
                    config["host"],
                    config["port"],
                    config["user"],
                    config["password"],
                    config["dbname"],
                )

                for tabela, dados in perfil:
                    first_row = dados
                    columns = [Column(column_name, Text) for column_name in first_row.keys()]

                    table = Table(tabela, conn.metadata, *columns)
                
                conn.metadata.create_all(conn.engine)


            elif file.name == "db_config_postgresql.json":
                conn = postgresql_connection(
                    config["host"],
                    config["port"],
                    config["user"],
                    config["password"],
                    config["dbname"],
                )

                for tabela, dados in perfil:
                    first_row = dados
                    columns = [Column(column_name, Text) for column_name in first_row.keys()]

                    table = Table(tabela, conn.metadata, *columns)
                
                conn.metadata.create_all(conn.engine)

            else:
                print(
                    "Arquivo fora do padrão, ou SGBD ainda não configurado na ferramenta!"
                )
                return
