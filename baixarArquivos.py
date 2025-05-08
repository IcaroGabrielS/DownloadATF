import time, os, signal, sys
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
from loggingConfig import get_logger
from utils import (
    conectar_postgres,
    iniciar_navegador_selenoid,
    autenticar_sefaz,
    acessar_pagina,
    verificar_downloads_em_progresso,
    clicar_elemento
)

load_dotenv()
logger = get_logger(__name__)

# Configuração do diretório de downloads
DIRETORIO_DOWNLOADS = "/home/desenvolvimento/DownloadATF/NFCE_XML_TEMP/incoming"
os.makedirs(DIRETORIO_DOWNLOADS, exist_ok=True)
os.chmod(DIRETORIO_DOWNLOADS, 0o777)

# Controle de execução
RUNNING = True

def configurar_tratamento_sinais():
    """Configura o tratamento de sinais para finalização limpa"""
    def handler_signal(signum, frame):
        global RUNNING
        logger.info(f"Sinal {signum} recebido. Preparando para encerrar serviço...")
        RUNNING = False
        logger.info("Serviço finalizado pelo usuário.")
        sys.exit(0)

    signal.signal(signal.SIGINT, handler_signal)
    signal.signal(signal.SIGTERM, handler_signal)

def obter_solicitacoes_com_link():
    """Busca solicitações que têm link, anexo=true e ainda não foram baixadas"""
    conexao = conectar_postgres()
    if not conexao:
        return []

    try:
        cursor = conexao.cursor()
        cursor.execute("""
            SELECT id, inscricao_estadual, link
            FROM nfce.solicitacoes
            WHERE link IS NOT NULL AND link != '' AND baixado = 0 AND tipo = 'NFCE' AND anexo = true
            ORDER BY criado_em
        """)

        solicitacoes = []
        for id, inscricao_estadual, link in cursor.fetchall():
            solicitacoes.append({
                "id": id,
                "inscricao_estadual": inscricao_estadual,
                "link": link
            })

        cursor.close()
        conexao.close()

        if solicitacoes:
            logger.info(f"Encontradas {len(solicitacoes)} solicitações pendentes de download com anexo=true")
        return solicitacoes

    except Exception as erro:
        logger.error(f"Erro ao buscar solicitações: {erro}")
        if conexao:
            conexao.close()
        return []

def marcar_como_baixado(id_solicitacao):
    """Marca a solicitação como baixada no banco de dados"""
    conexao = conectar_postgres()
    if not conexao:
        logger.error(f"Falha ao conectar ao PostgreSQL para marcar solicitação {id_solicitacao}")
        return False

    try:
        cursor = conexao.cursor()
        cursor.execute("""
            UPDATE nfce.solicitacoes
            SET baixado = baixado + 1,
                atualizado_em = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (id_solicitacao,))

        conexao.commit()
        cursor.close()
        conexao.close()

        logger.info(f"Solicitação {id_solicitacao} marcada como baixada com sucesso")
        return True

    except Exception as erro:
        logger.error(f"Erro ao marcar solicitação {id_solicitacao} como baixada: {erro}")
        if conexao:
            conexao.rollback()
            conexao.close()
        return False

def realizar_download(navegador, solicitacao):
    """Acessa o link e inicia o download do arquivo"""
    try:
        logger.info(f"Iniciando download para IE {solicitacao['inscricao_estadual']} (ID: {solicitacao['id']})")

        # Acessa o link para download
        acessar_pagina(navegador, solicitacao["link"])

        # Clica nos elementos de download
        espera_curta = int(os.environ.get("ESPERA_CURTA", 2))
        if clicar_elemento(navegador, os.environ.get("XPATH_IMAGEM_ANEXO"), espera_curta) and \
           clicar_elemento(navegador, os.environ.get("XPATH_LINK_DOWNLOAD"), espera_curta):
            logger.info(f"Download iniciado para IE {solicitacao['inscricao_estadual']} (ID: {solicitacao['id']})")
            time.sleep(10)
            return True
        else:
            logger.error(f"Falha ao clicar nos elementos para download - IE {solicitacao['inscricao_estadual']} (ID: {solicitacao['id']})")
            return False
    except Exception as e:
        logger.error(f"Erro ao realizar download para IE {solicitacao['inscricao_estadual']} (ID: {solicitacao['id']}): {str(e)}")
        return False

def processar_downloads():
    """Processa todos os downloads pendentes"""
    # Verificar permissões do diretório
    try:
        test_file_path = os.path.join(DIRETORIO_DOWNLOADS, "test_write_permission.tmp")
        with open(test_file_path, 'w') as f:
            f.write("Teste de permissão de escrita")
        os.remove(test_file_path)
    except Exception as e:
        logger.error(f"ERRO DE PERMISSÃO no diretório {DIRETORIO_DOWNLOADS}: {str(e)}")
        return 0

    # Carrega as solicitações pendentes de download
    solicitacoes = obter_solicitacoes_com_link()

    if not solicitacoes:
        return 0

    navegador = None
    downloads_realizados = 0
    total_solicitacoes = len(solicitacoes)

    try:
        # Inicia o navegador com Selenoid
        logger.info(f"Iniciando navegador para processar {total_solicitacoes} downloads...")
        navegador = iniciar_navegador_selenoid(DIRETORIO_DOWNLOADS)

        if navegador and autenticar_sefaz(navegador):
            logger.info("Navegador inicializado e autenticado com sucesso")

            # Processa cada solicitação com link disponível
            for i, solicitacao in enumerate(solicitacoes, 1):
                # Log de progresso menos frequente
                if i == 1 or i == total_solicitacoes or i % 10 == 0:
                    logger.info(f"Processando solicitação {i}/{total_solicitacoes}")

                if realizar_download(navegador, solicitacao):
                    if marcar_como_baixado(solicitacao["id"]):
                        downloads_realizados += 1
                    else:
                        logger.warning(f"Download realizado mas falha ao marcar como baixado: ID {solicitacao['id']}")

                # Espera entre os downloads
                time.sleep(5)

            logger.info(f"Downloads concluídos: {downloads_realizados}/{total_solicitacoes}")
        else:
            logger.error("Falha na autenticação ou inicialização do navegador")

    except Exception as e:
        logger.error(f"Erro durante a execução dos downloads: {str(e)}")
    finally:
        # Aguarda downloads em andamento concluírem
        max_tentativas = int(os.environ.get("MAX_TENTATIVAS", 10))
        tentativa = 0

        while verificar_downloads_em_progresso(DIRETORIO_DOWNLOADS) and tentativa < max_tentativas:
            logger.debug(f"Aguardando conclusão de downloads em progresso... (tentativa {tentativa+1}/{max_tentativas})")
            time.sleep(2)
            tentativa += 1

        if navegador:
            try:
                navegador.quit()
                logger.info("Navegador fechado com sucesso")
            except Exception as e:
                logger.warning(f"Erro ao fechar navegador: {str(e)}")

        try:
            arquivos = os.listdir(DIRETORIO_DOWNLOADS)
            logger.info(f"Total de {len(arquivos)} arquivos no diretório de download")
        except Exception as e:
            logger.error(f"Erro ao listar arquivos: {str(e)}")

    return downloads_realizados

def monitorar_downloads_continuamente():
    """Função principal que monitora continuamente as solicitações para download"""
    global RUNNING

    execucao_id = f"DOWNLOAD-{time.strftime('%Y%m%d-%H%M%S')}"
    logger.info(f"=" * 50)
    logger.info(f"INICIANDO SERVIÇO DE MONITORAMENTO DE DOWNLOADS ({execucao_id})")
    logger.info(f"=" * 50)

    configurar_tratamento_sinais()

    intervalo_verificacao = int(os.environ.get("INTERVALO_VERIFICACAO", 60))
    ultimo_log_sem_downloads = 0
    intervalo_min_log_sem_downloads = 300  # 5 minutos

    while RUNNING:
        try:
            # Verifica se há solicitações com links para baixar
            solicitacoes = obter_solicitacoes_com_link()

            if not solicitacoes:
                # Log periódico sobre ausência de downloads
                agora = time.time()
                if agora - ultimo_log_sem_downloads > intervalo_min_log_sem_downloads:
                    logger.info("Não há solicitações pendentes para download. Aguardando...")
                    ultimo_log_sem_downloads = agora

                # Aguarda antes de verificar novamente
                time.sleep(intervalo_verificacao)
                continue

            # Se há solicitações, processa os downloads
            downloads_realizados = processar_downloads()

            if downloads_realizados > 0:
                logger.info(f"{downloads_realizados} downloads processados com sucesso")
                time.sleep(5)
            else:
                time.sleep(intervalo_verificacao)

        except Exception as e:
            logger.error(f"Erro durante o monitoramento de downloads: {e}")
            time.sleep(30)

    logger.info("Serviço de monitoramento de downloads encerrado normalmente")

if __name__ == "__main__":
    try:
        monitorar_downloads_continuamente()
    except KeyboardInterrupt:
        logger.info("Serviço de monitoramento de downloads interrompido pelo usuário")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Erro fatal no serviço de monitoramento de downloads: {str(e)}")
        sys.exit(1)