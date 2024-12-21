import sys
from pathlib import Path

ROOT_PATH = Path.cwd()
sys.path.append(str(ROOT_PATH / "source"))

import socket
import os
from model.modules.api_connector import request_config, request_memory_saving
from model.modules.db_connector import create_connection_db
from model.modules.db_inserter import insert_into_db

local_host_name = socket.gethostname()
local_user_name = os.getlogin()

def init_creation_db():
    create_connection_db()


def init_creation_requests():
    request_config()


def total_data_requests():
    request_memory_saving()


def total_inserter():   
    insert_into_db()

<<<<<<< HEAD
=======
from model.modules.db_update import manager_update_date

manager_update_date()

>>>>>>> 123aa947d75d40024462cd796a6cc8010a8f30db
