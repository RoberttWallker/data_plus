from sqlalchemy import create_engine, MetaData, text
import time
from urllib.parse import quote
from model.modules.classes import DbMySql, DbPostgreSql
import json
from pathlib import Path
import psycopg2


MODEL_PATH = Path(__file__).absolute().parent.parent


def save_db_config(db_config, filename=None):
    if filename is None:
        filename = MODEL_PATH / "config/db_config/db_config.json"

    filename.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(filename, "r") as file:
            db_configs = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        db_configs = []

    db_configs.append(db_config)

    with open(filename, "w") as file:
        json.dump(db_configs, file, indent=4)


def load_db_config(filename=None):
    if filename is None:
        filename = MODEL_PATH / "config/db_config/db_config.json"

    try:
        with open(filename, "r") as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def mysql_connection(
    host,
    port,
    user,
    password,
    dbname,
    identificador_api,
    authorization,
    data_inicial,
):
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/")
    connection = engine.connect()
    metadata = MetaData()
    dialect = engine.dialect.name

    with connection.begin():
        connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {dbname};"))

    db_config = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "dbname": dbname,
        "identificador_api": identificador_api,
        "authorization": authorization,
        "data_inicial": data_inicial,
    }
    save_db_config(
        db_config=db_config,
        filename=MODEL_PATH / "config/db_config/db_config_mysql.json",
    )

    mysql_conn = DbMySql(
        engine=engine,
        connection=connection,
        metadata=metadata,
        identificador_api=identificador_api,
        authorization=authorization,
        data_inicial=data_inicial,
        db_name=dbname,
        dialect=dialect,
    )

    return mysql_conn


def postgresql_connection(
    host, port, user, password, dbname, identificador_api, authorization, data_inicial
):
    # Usar psycopg2 diretamente para conectar sem transações e criar o banco
    conn = psycopg2.connect(
        host=host, port=port, user=user, password=password, dbname="postgres"
    )
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    # Verificar se o banco de dados já existe
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname='{dbname}'")
    exists = cursor.fetchone()

    if not exists:
        # Criar o banco de dados se ele não existir
        cursor.execute(f"CREATE DATABASE {dbname};")

    cursor.close()
    conn.close()

    db_config = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "dbname": dbname,
        "identificador_api": identificador_api,
        "authorization": authorization,
        "data_inicial": data_inicial,
    }
    save_db_config(
        db_config=db_config,
        filename=MODEL_PATH / "config/db_config/db_config_postgresql.json",
    )

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    )
    connection = engine.connect()
    metadata = MetaData()
    dialect = engine.dialect.name

    postgresql_conn = DbPostgreSql(
        engine=engine,
        connection=connection,
        metadata=metadata,
        identificador_api=identificador_api,
        authorization=authorization,
        data_inicial=data_inicial,
        db_name=dbname,
        dialect=dialect,
    )

    return postgresql_conn


def create_connection_db():
    confirmado = False  # Variável de controle para encerrar o loop externo
    while not confirmado:
        # Coleta os dados uma única vez
        host = input("Endereço Host: ")
        port = int(input("Porta de acesso: "))
        user = input("Usuário: ")
        password = input("Senha: ")
        dbname = input("Nome do banco de dados: ")
        identificador_api = input("Identificador cliente SavWin: ")
        authorization = input("Authorization key: ")
        data_inicial = input("Data inicial da consulta às APIs (dd/mm/yyyy): ")
        time.sleep(1)

        while True:  # Inicia o loop de confirmação
            # Exibe os dados coletados para o usuário verificar
            print("\nVerifique os dados inseridos:\n")
            time.sleep(1)
            print(f"Endereço Host: {host}")
            print(f"Porta de acesso: {port}")
            print(f"Usuário: {user}")
            print(f"Senha: {password}")
            print(f"Nome do banco de dados: {dbname}")
            print(f"Identificador cliente SavWin: {identificador_api}")
            print(f"Authorization key: {authorization}")
            print(f"Data inicial da consulta: {data_inicial}")

            confirm = input("\nOs dados estão corretos? (s/n): ").lower()
            if confirm == "s":
                time.sleep(1)
                confirmado = True  # Define como True para encerrar o loop externo
                break  # Sai do loop de confirmação
            elif confirm == "n":
                print("\nPor favor, insira os dados novamente.")
                time.sleep(1)
                break  # Sai do loop de confirmação e volta ao início para redigitar os dados
            else:
                print(
                    "Opção inválida! Digite 's' para confirmar ou 'n' para corrigir os dados."
                )
                time.sleep(1)

    # Codifica a senha para evitar problemas com caracteres especiais
    encoded_password = quote(password, safe="")

    while True:
        escolha = input(
            """
#################################
|Qual SGBD você deseja utilizar?|
|                               |
| 1 - MySQL                     |
| 2 - PostgreSQL                |  
|                               |
| Q - Sair                      |
|                               |
| >>>                           |
#################################
"""
        )
        if escolha == "1":
            mysql_connection(
                host,
                port,
                user,
                encoded_password,
                dbname,
                identificador_api,
                authorization,
                data_inicial,
            )
            break
        elif escolha == "2":
            postgresql_connection(
                host,
                port,
                user,
                password,
                dbname,
                identificador_api,
                authorization,
                data_inicial,
            )
            break
        elif escolha == "Q":
            print("Encerrando o programa.")
            time.sleep(1)
            exit()
        else:
            print("Opção inválida, tente novamente.")
            time.sleep(1)
