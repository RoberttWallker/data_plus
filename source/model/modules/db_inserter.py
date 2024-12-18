from pathlib import Path
import ijson
import re
import traceback
from sqlalchemy import Table, Column, Text, Integer, Float, Boolean, Date, String
import time
from datetime import datetime
from collections import Counter


from .db_connector import (
    mysql_connection,
    postgresql_connection,
    load_config_file
)
from .aux_functions import delete_temp_files, conferir_tipo

MODEL_PATH = Path(__file__).absolute().parent.parent
CONFIG_PATH = MODEL_PATH / "config"
TEMP_FILE_PATH = MODEL_PATH / "data/temp_file_data"

# Função para converter os tipos de dados identificados em tipos de dados do SQLAlchemy
def tipo_para_sqlalchemy(tipo):
    if tipo == "int":
        return Integer
    elif tipo == "bool":
        return Boolean
    elif tipo == "float":
        return Float
    elif tipo == "date":
        return Date
    else:
        return String

def amostra_tabelas(arquivo, limite=1000):
    perfil_tabelas = []
    with open(arquivo, "r", encoding="utf-8") as f:
        parser = ijson.items(f, "lista.lista") # ijson retorna cada item dentro da chave(itera sobre a lista)
        for idx, dict in enumerate(parser):
            if idx >= limite:
                print("Limite de dados para perfil atingido.")
                break
            perfil_tabelas.append(dict)
    return perfil_tabelas


def tabelas_e_amostras(path):
    tabela_e_colunas = []
    for file in path.rglob("*.json"):
        try:
            file_name = file.name.split("Grid.")[0]
            table_name = re.sub(r"([a-z])([A-Z])", r"\1_\2", file_name).lower()
            perfil = amostra_tabelas(file)

            if not perfil:
                print(f"Arquivo {file.name} não contém dados válidos.")
                continue  # Ignora o arquivo se não houver dados válidos

        except Exception as e:
            print(f"Erro ao processar o arquivo {file}: {e}")
            continue  # Ignora o arquivo e continua o loop
        
        tabela_e_colunas.append((table_name, perfil))

    return tabela_e_colunas


def definir_tipo_colunas(amostra): # Retorna a coluna e tipo predominante dela
    tipos_por_chave = {}
    for chave, valor in amostra.items():
        tipo_valor = conferir_tipo(valor)
        if chave not in tipos_por_chave:
            tipos_por_chave[chave] = []
        tipos_por_chave[chave].append(tipo_valor)

    perfil = {}
    for chave, tipos in tipos_por_chave.items():
        contador = Counter(tipos)
        tipo_predominante = contador.most_common(1)[0][0]
        perfil[chave] = tipo_predominante

    return perfil


def insert_tables_metadata_TESTE(conn):
    amostras = tabelas_e_amostras(TEMP_FILE_PATH)  # Nome tabela e amostra de 1000

    for tabela, amostra in amostras:
        tipos_de_colunas = definir_tipo_colunas(amostra)

        # Criar a lista de colunas para a tabela
        columns = []
        for column_name, tipo in tipos_de_colunas.items():  # Correção de .item() para .items()
            tipo_sqlalchemy = tipo_para_sqlalchemy(tipo)
            columns.append(Column(column_name, tipo_sqlalchemy))

        # Criar a tabela no metadata
        table = Table(tabela, conn.metadata, *columns)

    # Criar todas as tabelas no banco de dados (após o loop)
    conn.metadata.create_all(conn.engine)


# Perfil das tabelas
def obter_colunas(arquivo):
    with open(arquivo, "r", encoding="utf-8") as f:
        try:
            # Tenta acessar como uma lista de listas
            parser = ijson.items(f, "item.item")
            primeiro_dicionario = next(parser)
        except StopIteration:
            # Se falhar, volta ao início do arquivo e tenta como lista simples
            f.seek(0)  # Reinicia a leitura do arquivo
            try:
                parser = ijson.items(f, "item")
                primeiro_dicionario = next(parser)
            except StopIteration:
                print(f"Erro: O arquivo {arquivo} não contém dados válidos.")
                return {}
            except Exception as e:
                print(f"Erro inesperado ao processar {arquivo}: {e}")
                return {}

        return primeiro_dicionario


def tabelas_e_colunas(path):
    tabela_e_colunas = []
    for file in path.rglob("*.json"):
        try:
            file_name = file.name.split("Grid.")[0]
            table_name = re.sub(r"([a-z])([A-Z])", r"\1_\2", file_name).lower()
            colunas = obter_colunas(file)

            if not colunas:
                print(f"Arquivo {file} não contém dados válidos.")
                continue  # Ignora o arquivo se não houver dados válidos

        except Exception as e:
            print(f"Erro ao processar o arquivo {file}: {e}")
            continue  # Ignora o arquivo e continua o loop
        
        tabela_e_colunas.append((table_name, colunas))

    return tabela_e_colunas


def tabelas_e_dados(path):
    tabelas_e_dados = []
    for file in path.rglob("*.json"):
        file_name = file.name.split("Grid.")[0]
        table_name = re.sub(r"([a-z])([A-Z])", r"\1_\2", file_name).lower()
        tabelas_e_dados.append((table_name, file))
    return tabelas_e_dados


# Inserção de dados
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
            traceback.print_exc()

        try:
            insert_data(conn)
        except Exception as e:
            print(
                f"Ocorreu um erro ao inserir os dados no banco de dados. Erro: {e}",
            )
            traceback.print_exc()

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
        delete_temp_files(TEMP_FILE_PATH)
    else:
        print("Nem todas as inserções foram bem-sucedidas. Arquivos mantidos.")
