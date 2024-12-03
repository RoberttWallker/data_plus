import sys
from pathlib import Path

ROOT_PATH = Path.cwd()
sys.path.append(str(ROOT_PATH / "source"))

import socket
from model.data.requests_pool import request_config, request_memory_saving
from model.modules.db_conn import create_connection_db

host_name = socket.gethostname()

# create_connection_db()
# request_config()
request_memory_saving()
