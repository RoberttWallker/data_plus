import sys
from pathlib import Path

ROOT_PATH = Path.cwd()
sys.path.append(str(ROOT_PATH / "source"))

import socket
import os
from model.modules.api_connector import request_config, request_memory_saving
from model.modules.db_connector import create_connection_db
from model.modules.db_inserter import insert_into_db, tabelas_e_colunas, tabelas_e_dados

local_host_name = socket.gethostname()
local_user_name = os.getlogin()

# create_connection_db()
# request_config()
# equest_memory_saving()
insert_into_db()
# lista = obter_nomes_arquivos()
# lista = tabelas_e_dados()
# print(lista)
