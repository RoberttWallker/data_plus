from pathlib import Path
import ijson
import re
import json


from .db_connector import (
    mysql_connection,
    postgresql_connection,
    load_config_file,
    load_db_config,
)
from .classes import ConfigDB


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
    return json.dumps(tabela_e_colunas, indent=4)


def insert_into_db():
    for file in CONFIG_PATH.rglob("db_config/*.json"):
        db_configs = load_config_file(file)
        for config in db_configs:
            print(type(config))
            print(config)

            if file.name == "db_config_mysql.json":
                conn = mysql_connection(
                    config["host"],
                    config["port"],
                    config["user"],
                    config["password"],
                    config["dbname"],
                )

            elif file.name == "db_config_postgresql.json":
                conn = postgresql_connection(
                    config["host"],
                    config["port"],
                    config["user"],
                    config["password"],
                    config["dbname"],
                )

            else:
                print(
                    "Arquivo fora do padrão, ou SGBD ainda não configurado na ferramenta!"
                )
                return
