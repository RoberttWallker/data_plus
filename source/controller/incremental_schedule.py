from controller import init_incremental_update
from pathlib import Path
import logging

ROOT_PATH = Path(__file__).resolve().parent.parent

LOG_DIR = ROOT_PATH / "logs/application_logs"
LOG_FILE = LOG_DIR / "update_logs.log"


LOG_DIR.mkdir(parents=True, exist_ok=True)


# Criar um FileHandler com UTF-8
file_handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Configurar o logger
logging.basicConfig(level=logging.DEBUG, handlers=[file_handler])

if __name__ == "__main__":
    logging.info("Atualização incremental inciada.")
    try:
        init_incremental_update()
    except Exception as e:
        print(e)
        logging.error(f"Ocorreu um erro durante a carga incremental: {e}")
    logging.info("Atualização incremental encerrada.")