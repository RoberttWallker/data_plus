import sys
from pathlib import Path
ROOT_PATH = Path.cwd()
sys.path.append(str(ROOT_PATH / 'source'))

import socket
from model.data.requests_pool import initiate_config, request_memory_saving

host_name = socket.gethostname()

#initiate_config()
request_memory_saving()