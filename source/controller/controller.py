import sys
from pathlib import Path

ROOT_PATH = Path.cwd()
sys.path.append(str(ROOT_PATH / "source"))

import socket
import os
from model.modules.api_connector import request_config, request_total_memory_saving, request_incremental_memory_saving
from model.modules.db_connector import create_connection_db
from model.modules.db_inserter import insert_total_into_db, insert_increment_into_db
from model.modules.db_update import manager_update_date, get_incremental_date, formatar_datas_incrementais


local_host_name = socket.gethostname()
local_user_name = os.getlogin()

def init_creation_db():
    create_connection_db()


def init_creation_requests():
    request_config()

def total_data_requests():
    request_total_memory_saving()

def increment_data_resquests():
    request_incremental_memory_saving()

def total_inserter():   
<<<<<<< HEAD
    insert_into_db()
    
=======
    insert_total_into_db()

def increment_inserter():
    insert_increment_into_db()

def create_column_incremental():
    manager_update_date()

def init_incremental_update():
    increment_data_resquests()
    increment_inserter()


>>>>>>> develop
