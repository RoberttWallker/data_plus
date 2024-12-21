from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.exc import OperationalError
import time
from urllib.parse import quote
from model.modules.classes import DbMySql, DbPostgreSql, ConfigDB
import json
from pathlib import Path
import psycopg2


MODEL_PATH = Path(__file__).absolute().parent.parent
DB_CONFIG_PATH = MODEL_PATH / "config/db_config/"


# Funções de salvamento e carregamento de configurações de bancos de dados
def save_db_config(db_config, filename):

    filename.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(filename, "r") as file:
            db_configs = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        db_configs = []

    db_configs.append(db_config)

    with open(filename, "w") as file:
        json.dump(db_configs, file, indent=4)


def load_config_file(filename):

    try:
        with open(filename, "r") as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def load_db_config(db_configs):
    connections = []
    for config in db_configs:
        conn = ConfigDB(
            config.host,
            config.port,
            config.user,
            config.password,
            config.dbname,
        )
        connections.append(conn)
    return connections


def get_connecion_data():
    confirmado = False  # Variável de controle para encerrar o loop externo
    while not confirmado:
        # Coleta os dados uma única vez
        host = input("Endereço Host: ")
        port = int(input("Porta de acesso: "))
        user = input("Usuário: ")
        password = input("Senha: ")
        dbname = input("Nome do banco de dados: ")
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

    config_db = ConfigDB(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
    )
    return config_db


def mysql_configuration(
    host,
    port,
    user,
    password,
    dbname,
):
    try:
        # Codifica a senha para evitar caracteres especiais
        encoded_password = quote(password, safe="")

        engine = create_engine(
            f"mysql+pymysql://{user}:{encoded_password}@{host}:{port}/"
        )
        connection = engine.connect()

        with connection.begin():
            connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {dbname};"))

        db_config = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "dbname": dbname,
        }

        save_db_config(
            db_config=db_config,
            filename=MODEL_PATH / "config/db_config/db_config_mysql.json",
        )
    except OperationalError as e:
        numero_erro = e.orig.args[0] # type: ignore
        if numero_erro == 2003:
            print(f"Erro! Endereço do Host ou porta de acesso não está correto.")
            print(f"Erro completo: {e.orig.args}") # type: ignore
        elif numero_erro == 1045:
            print(f"Erro! Endereço usuário ou senha não está correto.")
            print(f"Erro completo: {e.orig.args}") # type: ignore
    except Exception as e:
        print(f"Ocorreu o seguinte erro: {e}")


def postgresql_configuration(
    host,
    port,
    user,
    password,
    dbname,
):
    try:
        # Usar psycopg2 diretamente para conectar sem transações e criar o banco
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname="postgres",
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
        }
        save_db_config(
            db_config=db_config,
            filename=MODEL_PATH / "config/db_config/db_config_postgresql.json",
        )

    except psycopg2.OperationalError as e:
        print(f"Erro! Porta de acesso não está correta.\n")
        print(f"Erro completo: {e}")
    except UnicodeDecodeError as e:
        print(f"Erro! Host, usuário ou senha de acesso não está correto.")
        print(f"Erro completo: {e}")
    except Exception as e:
        print(f"Ocorreu o seguinte erro: {e}")


def check_existing_db_config(config_db, sgbd_configuration, sgbd_configs_file):
    if sgbd_configs_file.exists():
        configs = load_config_file(sgbd_configs_file)
        config_exists = any(
            config['host'] == config_db.host and
            config['port'] == config_db.port and
            config['user'] == config_db.user and
            config['password'] == config_db.password and
            config['dbname'] == config_db.dbname 
            for config in configs
        )

        if config_exists:
            print(
                f"A configuração para o banco {config_db.dbname} já existe."
            )
            return

        else:
            sgbd_configuration(
                config_db.host,
                config_db.port,
                config_db.user,
                config_db.password,
                config_db.dbname,
            )
            return

    sgbd_configuration(
        config_db.host,
        config_db.port,
        config_db.user,
        config_db.password,
        config_db.dbname,
    )
    return

# Métodos de conexão a bancos de dados


def mysql_connection(host, port, user, password, dbname):
    # Codifica a senha para evitar caracteres especiais
    encoded_password = quote(password, safe="")

    try:
        engine = create_engine(
            f"mysql+pymysql://{user}:{encoded_password}@{host}:{port}/{dbname}"
        )
        connection = engine.connect()
        metadata = MetaData()
        dialect = engine.dialect.name

        mysql_conn = DbMySql(
            engine=engine,
            connection=connection,
            metadata=metadata,
            db_name=dbname,
            dialect=dialect,
        )

        return mysql_conn

    except OperationalError as e:
        numero_erro = e.orig.args[0] # type: ignore
        if numero_erro == 2003:
            print(f"Erro! Endereço do Host ou porta de acesso não está correto.")
            print(f"Erro completo: {e.orig.args}") # type: ignore
            return None
        elif numero_erro == 1045:
            print(f"Erro! Endereço usuário ou senha não está correto.")
            print(f"Erro completo: {e.orig.args}") # type: ignore
            return None
    except Exception as e:
        print(f"Ocorreu o seguinte erro: {e}")
        return None


def postgresql_connection(host, port, user, password, dbname):
    try:
        # Codifica a senha para evitar caracteres especiais
        encoded_password = quote(password, safe="")

        engine = create_engine(
            f"postgresql+psycopg2://{user}:{encoded_password}@{host}:{port}/{dbname}"
        )
        connection = engine.connect()
        metadata = MetaData()
        dialect = engine.dialect.name

        postgresql_conn = DbPostgreSql(
            engine=engine,
            connection=connection,
            metadata=metadata,
            db_name=dbname,
            dialect=dialect,
        )

        return postgresql_conn

    except OperationalError as e:
        print(f"Erro! Porta de acesso não está correta.\n")
        print(f"Erro completo: {e}")
        return None
    except UnicodeDecodeError as e:
        print(f"Erro! Host, usuário, senha ou banco de dados não está correto.\n")
        print(f"Erro completo: {e}")
        return None
    except Exception as e:
        print(f"Ocorreu o seguinte erro: {e}")
        return None


# Métodos de criação de banco de dados
def create_connection_db():

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
#################################
>>>"""
        )
        if escolha == "1":
            config_db = get_connecion_data()

            mysql_configs_file = DB_CONFIG_PATH / "db_config_mysql.json"

            check_existing_db_config(config_db, mysql_configuration, mysql_configs_file)
            

        elif escolha == "2":
            config_db = get_connecion_data()
            postgresql_configs_file = DB_CONFIG_PATH / "db_config_postgresql.json"

            check_existing_db_config(config_db, postgresql_configuration, postgresql_configs_file)

        elif escolha == "Q":
            print("Encerrando o programa.")
            time.sleep(1)
            exit()
        else:
            print("Opção inválida, tente novamente.")
            time.sleep(1)
