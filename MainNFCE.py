import time, schedule, logging, os
from RequestsNFCE import executar_processo_requests_nfce
from DownloadsNFCE import executar_processo_downloads_nfce


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("schedule.log"),  # Salva logs em um arquivo
        logging.StreamHandler()  # Exibe logs no console
    ]
)
logger = logging.getLogger(__name__)

def excluir_solicitacoes_anteriores():
    caminho_json = os.path.join(os.path.dirname(__file__), 'json_files/solicitacoes.json')
    if os.path.exists(caminho_json): os.remove(caminho_json)

def agendar_tarefas():
    schedule.every(5).days.at("00:01").do(executar_processo_requests_nfce)
    schedule.every(5).days.at("12:00").do(executar_processo_downloads_nfce)

def executar_agendador():
    agendar_tarefas()
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    try: executar_agendador()
    except KeyboardInterrupt:
        logger.info("Programa interrompido pelo usuário.")
    except Exception as e:
        logger.error(f"Erro inesperado: {e}", exc_info=True)