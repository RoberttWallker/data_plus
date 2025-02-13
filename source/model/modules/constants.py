from pathlib import Path

#PATH STRUCTURES
ROOT_PATH = Path.cwd()
SRC_PATH = ROOT_PATH / "source"

#Estrutura controller
CTRL_PATH = SRC_PATH / "controller"
#--controller/tasks
TASK_PATH = CTRL_PATH / "tasks"

#Estrura logs
LOGS_PATH = SRC_PATH / "logs"
APP_LOGS_PATH = LOGS_PATH / "application_logs"

#Estrutura model
MODEL_PATH = SRC_PATH / "model"
#--model/config
CONFIG_PATH = MODEL_PATH / "config"
DB_CONFIG_PATH = CONFIG_PATH / "db_config"
INCREMENT_CONFIG_PATH = CONFIG_PATH / "incremental_config"
REQUEST_CONFIG_PATH = CONFIG_PATH / "requests_config"
#--model/data
DATA_PATH = MODEL_PATH / "data"
TEMP_DATA_PATH = DATA_PATH / "temp_file_data"
#--model/modules
MODULES_PATH = MODEL_PATH / "modules"

#---------------------------------------------------------------------------

#TASKS FILES
task_incremental_bat_file = TASK_PATH / "task_exec_incremental.bat"
ghost_incremental_vbs_file = TASK_PATH / "ghost_exec_task.vbs"

#SCHEDULE FILES
file_incremental_schedule = CTRL_PATH / "incremental_schedule.py"

#REQUEST CONFIG FILES
file_requests_config = REQUEST_CONFIG_PATH / "requests_config.json"

#DB CONFIG FILES
file_db_config_mysql = DB_CONFIG_PATH / "db_config_mysql.json"
file_db_config_postgresql = DB_CONFIG_PATH / "db_config_postgresql.json"

#INCREMENTAL CONFIG FILES
file_incremental_config_mysql = INCREMENT_CONFIG_PATH / "incremental_config_mysql.json"
file_incremental_config_postgresql = INCREMENT_CONFIG_PATH / "incremental_config_postgresql.json"

#LOG FILES
file_application_logs = APP_LOGS_PATH / "update_logs.log"