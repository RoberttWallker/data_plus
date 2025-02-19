import json
import os
import socket
import sys
from pathlib import Path

SRC_PATH = Path(__file__).absolute().parent.parent
sys.path.append(str(SRC_PATH))


from model.modules.api_connector import request_config, request_total_memory_saving, request_incremental_memory_saving, file_requests_config
from model.modules.db_connector import create_connection_db, init_a_database
from model.modules.db_inserter import insert_total_into_db, insert_increment_into_db
from model.modules.db_update import manager_update_date
from model.modules.aux_func_app import create_task_scheduler_windows, get_identifiers, create_token_file

local_host_name = socket.gethostname()
local_user_name = os.getlogin()

def init_creation_db():
    create_connection_db()

def init_creation_requests():
    request_config()

def total_data_requests():
    identificador = request_total_memory_saving()
    return identificador

def increment_data_resquests(identificador):
    request_incremental_memory_saving(identificador)

def total_inserter(identificador):   
    insert_total_into_db(identificador)

def increment_inserter(identificador):
    insert_increment_into_db(identificador)

def create_column_incremental():
    manager_update_date()

def init_incremental_update():
    with open(file_requests_config, "r") as file:
        requests_config = json.load(file)
        identifiers = get_identifiers(requests_config)
        for identifier in identifiers:
            increment_data_resquests(identifier)
            increment_inserter(identifier)

def create_scheduler_windows():
    create_task_scheduler_windows()

def manager_init_database():
    init_a_database()

def token_creation():
    create_token_file()