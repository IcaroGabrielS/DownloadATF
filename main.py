import time, schedule
from util import (montar_lista_solicitacoes, remover_solicitacoes_anteriores)
from downloads import executar_processo_downloads_nfce
from requests import executar_processo_requests_nfce


def agendar_tarefas():
    schedule.every(5).days.at("00:01").do(remover_solicitacoes_anteriores)
    schedule.every(5).days.at("00:02").do(montar_lista_solicitacoes)

    schedule.every(5).days.at("00:03").do(executar_processo_requests_nfce)
    schedule.every(5).days.at("12:00").do(executar_processo_downloads_nfce)

    schedule.every(5).days.at("14:30").do(executar_processo_requests_nfce)
    schedule.every(5).days.at("18:10").do(executar_processo_downloads_nfce)

    schedule.every(5).days.at("20:30").do(executar_processo_requests_nfce)
    schedule.every(5).days.at("22:00").do(executar_processo_downloads_nfce)


def executar_agendador():
    agendar_tarefas()
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    try: executar_agendador()
    except KeyboardInterrupt: print("Programa interrompido pelo usuário.")
    except Exception as e: print(f"Erro inesperado: {e}", exc_info=True)