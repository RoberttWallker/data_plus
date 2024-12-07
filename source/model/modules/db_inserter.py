from pathlib import Path
import ijson
import re
import json
from sqlalchemy import inspect, Table, Column, Text, text
import time


from .db_connector import (
    mysql_connection,
    postgresql_connection,
    load_config_file,
    load_db_config,
)


MODEL_PATH = Path(__file__).absolute().parent.parent
CONFIG_PATH = MODEL_PATH / "config"
TEMP_FILE_PATH = MODEL_PATH / "data/temp_file_data"


def delete_temp_files():
    for item in TEMP_FILE_PATH.glob("*"):
        try:
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                for sub_item in item.glob("*"):
                    sub_item.unlink() if sub_item.is_file() else sub_item.rmdir()
                item.rmdir()
        except Exception as e:
            print(f"Erro ao remover {item}: {e}")

    print("Arquivos e pastas temporárias, removidos com sucesso.")


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


def tabelas_e_colunas(path):
    tabela_e_colunas = []
    for file in path.rglob("*.json"):
        file_name = file.name.split("Grid.")[0]
        table_name = re.sub(r"([a-z])([A-Z])", r"\1_\2", file_name).lower()
        colunas = obter_colunas(file)
        tabela_e_colunas.append((table_name, colunas))
    return tabela_e_colunas


def tabelas_e_dados(path):
    tabelas_e_dados = []
    for file in path.rglob("*.json"):
        file_name = file.name.split("Grid.")[0]
        table_name = re.sub(r"([a-z])([A-Z])", r"\1_\2", file_name).lower()
        tabelas_e_dados.append((table_name, file))
    return tabelas_e_dados


def insert_tables_metadata(conn):
    perfil_colunas = tabelas_e_colunas(TEMP_FILE_PATH)

    for tabela, colunas in perfil_colunas:
        if not colunas:
            print(f">>>Tabela '{tabela}' ignorada: dados do JSON estão vazios.\n{' '*11}{'-'*len(tabela)}")
            continue

        columns = [Column(column_name, Text) for column_name in colunas.keys()]

        table = Table(tabela, conn.metadata, *columns)

    conn.metadata.create_all(conn.engine)


def insert_data(conn):
    dados_completos = tabelas_e_dados(TEMP_FILE_PATH)

    connection = conn.connection
    connection_name = f"{conn.db_name} - {conn.dialect}"

    print(f"{'-'*44}{'-'*len(connection_name)}\nIniciando processo de inserção de dados em: {connection_name}\n{'-'*44}{'-'*len(connection_name)}\n")
    try:
        for tabela, dados in dados_completos:
            if dados.exists():
                if tabela not in conn.metadata.tables:
                    print(f">>>Tabela '{tabela}' não encontrada no metadata.\n{' '*11}{'-'*len(tabela)}")
                    continue

                table = conn.metadata.tables[tabela]
                with open(dados, "r", encoding="utf-8") as f:
                    try:

                        parser = ijson.items(f, "item")
                        lote_tam = 1000
                        lote = []

                        for item in parser:
                            if isinstance(item, list):
                                lote.extend(item)
                            else:
                                lote.append(item)

                        if len(lote) >= lote_tam:
                            print(f"Inserindo os dados na tabela: {tabela}")
                            try:
                                connection.execute(table.insert(), lote)
                                lote.clear()
                            except Exception as e:
                                print(
                                    f"Erro ao fazer o .execute() na tabela: {tabela}: {e}"
                                )

                        if lote:
                            connection.execute(table.insert(), lote)

                    except Exception as e:
                        print(f"Erro ao processar dados para '{tabela}': {e}")

                try:
                    connection.commit()
                except Exception as e:
                    print(f"\nErro ao fazer commit das alterações: {e}\n")
            else:
                print("Arquivo não existe.")
    finally:
        print(f"\n{'-'*37}{'-'*len(connection_name)}\nFechando conexão com banco de dados: {connection_name}\n{'-'*37}{'-'*len(connection_name)}\n")
        connection.close()


def insert_manager(conn):
    try:
        try:
            insert_tables_metadata(conn)
        except Exception as e:
            print(
                f"Ocorreu um erro ao inserir as tabelas no metadata. Erro: {e}",
            )

        try:
            insert_data(conn)
        except Exception as e:
            print(
                f"Ocorreu um erro ao inserir os dados no banco de dados. Erro: {e}",
            )

    except Exception as e:
        print(f"Ocorreu um erro: {e} na função insert_manager()")


def insert_into_db():
    controle = {}
    for file in CONFIG_PATH.rglob("db_config/*.json"):
        db_configs = load_config_file(file)
        for config in db_configs:

            print("\nIniciando...")
            if file.name == "db_config_mysql.json":
                time.sleep(1)
                print(f"\nEssas são as configuraçãoes do banco de dados:\n{config}\n")
                time.sleep(1)
                conn = mysql_connection(
                    config["host"],
                    config["port"],
                    config["user"],
                    config["password"],
                    config["dbname"],
                )

                if conn == None:
                    print("Conexão falhou. Não é possível construir as tabelas.")
                    controle[config["dbname"]] = False
                else:
                    insert_manager(conn)
                    controle[conn.db_name] = True

            elif file.name == "db_config_postgresql.json":
                print(f"\nEssas são as configuraçãoes do db:\n{config}\n")
                conn = postgresql_connection(
                    config["host"],
                    config["port"],
                    config["user"],
                    config["password"],
                    config["dbname"],
                )

                if conn == None:
                    print("Conexão falhou. Não é possível construir as tabelas.")
                    controle[config["dbname"]] = False
                else:
                    insert_manager(conn)
                    controle[conn.db_name] = True

            else:
                print(
                    "Arquivo fora do padrão, ou SGBD ainda não configurado na ferramenta!"
                )
                controle["Fora_padrao_ou_sgbd_nao_configurado"] = False

    if all(controle.values()):
        delete_temp_files()
    else:
        print("Nem todas as inserções foram bem-sucedidas. Arquivos mantidos.")
