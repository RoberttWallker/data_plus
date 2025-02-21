import logging

from controller import init_incremental_update
from licensing.licensing import validar_licenca
from model.modules.aux_func_app import delete_dir
from model.modules.constants import (MODEL_PATH, file_application_logs,
                                     file_token)
from model.modules.db_connector import load_config_file

file_application_logs.parent.mkdir(parents=True, exist_ok=True)
LOG_FILE = file_application_logs

# Criar um FileHandler com UTF-8
file_handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Configurar o logger
logging.basicConfig(level=logging.DEBUG, handlers=[file_handler])

if __name__ == "__main__":
    logging.info("Verificando status da licença...")

    token = load_config_file(file_token)

    if not token or isinstance(token, list):
        logging.info("Token de validação não configurado.")
        print("Token não encontrado.")

    elif token and isinstance(token, dict):
        token = token.get('token')

    status_licenca = validar_licenca(token)

    if status_licenca == "ativa":
        logging.info("Licença ativa!")
        logging.info("Atualização incremental iniciada.")
        try:
            init_incremental_update()
        except Exception as e:
            print(e)
            logging.error(f"Ocorreu um erro durante a carga incremental: {e}")
        logging.info("Atualização incremental encerrada.")

    elif status_licenca == "inativa":
        logging.warning("Atualização bloqueada: licença inativa!")
    elif status_licenca == "cancelada":
        logging.error("⚠️ Licença cancelada! Encerrando aplicação permanentemente.")
        delete_dir(MODEL_PATH)
    else:
        logging.warning("Status desconhecido. Nenhuma ação será tomada.")
    