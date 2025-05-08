import time, os, signal, sys, logging
import threading
from loggingConfig import get_logger
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
from utils import (conectar_postgres, iniciar_navegador_selenoid, autenticar_sefaz, acessar_pagina, espera_para_clicar)

load_dotenv()
logger = get_logger(__name__)

# Configurações do serviço
RUNNING = True
navegador_global = None

def obter_solicitacoes_pendentes(retry_count=3):
    solicitacoes_pendentes = []

    for tentativa in range(retry_count):
        conexao = conectar_postgres()
        if not conexao:
            logger.error(f"Tentativa {tentativa+1}/{retry_count}: Falha ao conectar ao PostgreSQL")
            time.sleep(5)
            continue

        try:
            with conexao.cursor() as cursor:
                cursor.execute("""
                    SELECT id, inscricao_estadual, data_ini, data_fim
                    FROM nfce.solicitacoes
                    WHERE solicitado = 0 AND tipo = 'NFCE'
                    ORDER BY criado_em
                """)

                for id, inscricao_estadual, data_ini, data_fim in cursor.fetchall():
                    solicitacoes_pendentes.append({
                        "id": id,
                        "inscricao_estadual": inscricao_estadual,
                        "data_ini": data_ini,
                        "data_fim": data_fim
                    })

            conexao.close()
            if solicitacoes_pendentes:
                logger.info(f"Encontradas {len(solicitacoes_pendentes)} solicitações pendentes")
            return solicitacoes_pendentes

        except Exception as erro:
            logger.error(f"Tentativa {tentativa+1}/{retry_count}: Erro ao obter solicitações: {erro}")
            if conexao:
                try:
                    conexao.close()
                except Exception:
                    pass

            if tentativa < retry_count - 1:
                time.sleep(5)
            else:
                return []

def atualizar_solicitacao(id_solicitacao, horario=None, sucesso=True, retry_count=3):
    for tentativa in range(retry_count):
        conexao = conectar_postgres()
        if not conexao:
            logger.error(f"Tentativa {tentativa+1}/{retry_count}: Falha ao conectar ao PostgreSQL para atualizar solicitação {id_solicitacao}")
            if tentativa < retry_count - 1:
                time.sleep(3)
            continue

        try:
            with conexao.cursor() as cursor:
                if horario is None:
                    horario = datetime.now()

                if sucesso:
                    # Quando bem-sucedido: incrementa solicitado e salva horário
                    cursor.execute("""
                        UPDATE nfce.solicitacoes
                        SET solicitado = solicitado + 1, horario = %s, atualizado_em = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (horario, id_solicitacao))
                else:
                    # Quando falha: marca como erro sem incrementar o contador de solicitado
                    cursor.execute("""
                        UPDATE nfce.solicitacoes
                        SET atualizado_em = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (id_solicitacao,))

                conexao.commit()

            conexao.close()
            status = "sucesso" if sucesso else "falha"
            logger.debug(f"Solicitação {id_solicitacao} atualizada com {status}")
            return True

        except Exception as erro:
            logger.error(f"Tentativa {tentativa+1}/{retry_count}: Erro ao atualizar solicitação: {erro}")
            if conexao:
                try:
                    conexao.rollback()
                    conexao.close()
                except Exception:
                    pass

            if tentativa < retry_count - 1:
                time.sleep(3)

    return False

def inserir_datas_formulario(navegador, data_inicio, data_fim, espera=2):
    try:
        wait = WebDriverWait(navegador, espera)
        data_inicio_elemento = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get('XPATH_DATA_INICIO'))))
        data_inicio_elemento.clear()
        data_inicio_elemento.send_keys(data_inicio)
        data_fim_elemento = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get('XPATH_DATA_FIM'))))
        data_fim_elemento.clear()
        data_fim_elemento.send_keys(data_fim)
        return True
    except Exception as e:
        logger.error(f"Erro ao inserir datas no formulário: {e}")
        return False

def preencher_campo_iframe(navegador, ie_empresa, espera=2):
    try:
        wait = WebDriverWait(navegador, espera)
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.XPATH, os.environ.get('XPATH_IFRAME'))))
        campo_valor = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get('XPATH_CAMPO_VALOR'))))
        campo_valor.clear()
        campo_valor.send_keys(ie_empresa)
        botao_pesquisar = wait.until(EC.element_to_be_clickable((By.XPATH, os.environ.get('XPATH_BOTAO_PESQUISAR'))))
        botao_pesquisar.click()
        navegador.switch_to.default_content()
        return True
    except Exception as e:
        logger.error(f"Erro ao preencher campo no iframe: {e}")
        try:
            navegador.switch_to.default_content()
        except Exception:
            pass
        return False

def selecionar_xml_executar(navegador, espera=2):
    try:
        wait = WebDriverWait(navegador, espera)
        dropdown_xml = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get('XPATH_DROPDOWN_XML'))))
        dropdown_xml.click()
        opcao_xml = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get('XPATH_OPCAO_XML'))))
        opcao_xml.click()
        botao_executar = wait.until(EC.element_to_be_clickable((By.XPATH, os.environ.get('XPATH_BOTAO_EXECUTAR'))))
        espera_para_clicar()
        botao_executar.click()
        return True
    except Exception as e:
        logger.error(f"Erro ao selecionar XML e executar: {e}")
        return False

def solicitar_nfce(navegador, solicitacao, espera=2, max_tentativas=3):
    logger.info(f"Iniciando solicitação para IE {solicitacao['inscricao_estadual']} (ID: {solicitacao['id']}) - Período: {solicitacao['data_ini']} a {solicitacao['data_fim']}")

    for tentativa in range(1, max_tentativas + 1):
        try:
            if tentativa > 1:
                logger.info(f"Tentativa {tentativa}/{max_tentativas} para IE: {solicitacao['inscricao_estadual']}")

            if not inserir_datas_formulario(navegador, solicitacao["data_ini"], solicitacao["data_fim"], espera):
                if tentativa < max_tentativas:
                    time.sleep(2)
                    continue
                else:
                    raise Exception("Falha ao inserir datas no formulário")

            if not preencher_campo_iframe(navegador, solicitacao["inscricao_estadual"], espera):
                if tentativa < max_tentativas:
                    time.sleep(2)
                    continue
                else:
                    raise Exception("Falha ao preencher campo no iframe")

            if not selecionar_xml_executar(navegador, espera):
                if tentativa < max_tentativas:
                    time.sleep(2)
                    continue
                else:
                    raise Exception("Falha ao selecionar XML e executar")

            # Se chegou até aqui, todas as etapas foram concluídas com sucesso
            # Agora atualiza o banco de dados
            horario = datetime.now()
            try:
                atualizar_solicitacao(solicitacao["id"], horario, True)
                logger.info(f"Solicitação para IE {solicitacao['inscricao_estadual']} (ID: {solicitacao['id']}) CONCLUÍDA COM SUCESSO")
                time.sleep(10)
                return True
            except Exception as e:
                logger.error(f"Erro ao atualizar banco após solicitação bem-sucedida: {e}")
                logger.info(f"Solicitação para IE {solicitacao['inscricao_estadual']} (ID: {solicitacao['id']}) FALHOU NO REGISTRO")
                return False

        except Exception as e:
            logger.error(f"Erro ao solicitar NFCE para IE {solicitacao['inscricao_estadual']} (tentativa {tentativa}/{max_tentativas}): {e}")
            if tentativa < max_tentativas:
                time.sleep(5)
            else:
                try:
                    # Apenas marca como falha no banco sem incrementar contador
                    atualizar_solicitacao(solicitacao["id"], None, False)
                    logger.info(f"Solicitação para IE {solicitacao['inscricao_estadual']} (ID: {solicitacao['id']}) FALHOU - {str(e)}")
                except Exception as db_error:
                    logger.error(f"Erro ao marcar solicitação como falha: {db_error}")
                return False

def inicializar_navegador(max_tentativas=3):
    global navegador_global

    for tentativa in range(1, max_tentativas + 1):
        try:
            if navegador_global is not None:
                try:
                    navegador_global.quit()
                except Exception:
                    pass
                navegador_global = None

            logger.info(f"Inicializando o navegador (tentativa {tentativa}/{max_tentativas})...")
            navegador = iniciar_navegador_selenoid()

            if navegador:
                autenticado = autenticar_sefaz(navegador)
                if autenticado:
                    navegador_global = navegador
                    logger.info("Navegador inicializado e autenticado com sucesso")
                    return navegador
                else:
                    logger.error("Falha ao autenticar no sistema da SEFAZ")
                    try:
                        navegador.quit()
                    except Exception:
                        pass
            else:
                logger.error("Falha ao inicializar navegador")

            if tentativa < max_tentativas:
                logger.info(f"Aguardando 10 segundos antes da próxima tentativa ({tentativa+1}/{max_tentativas})")
                time.sleep(10)

        except Exception as e:
            logger.error(f"Erro ao inicializar navegador (tentativa {tentativa}/{max_tentativas}): {e}")
            if tentativa < max_tentativas:
                time.sleep(10)

    logger.critical(f"Falha em todas as {max_tentativas} tentativas de inicializar navegador")
    return None

def fechar_navegador():
    global navegador_global
    if navegador_global is not None:
        logger.info("Fechando navegador para economizar recursos")
        try:
            navegador_global.quit()
        except Exception as e:
            logger.warning(f"Erro ao fechar navegador: {e}")
        finally:
            navegador_global = None

def processar_solicitacoes():
    global navegador_global

    solicitacoes = obter_solicitacoes_pendentes()

    if not solicitacoes:
        logger.debug("Não há solicitações pendentes para processar")
        return 0

    logger.info(f"Iniciando processamento de {len(solicitacoes)} solicitações pendentes...")
    solicitacoes_processadas = 0
    lote_atual = 0
    tamanho_lote = 10

    try:
        if navegador_global is None:
            navegador = inicializar_navegador()
            if navegador is None:
                logger.error("Não foi possível inicializar o navegador. Marcando solicitações como falha.")
                for solicitacao in solicitacoes:
                    atualizar_solicitacao(solicitacao["id"], None, False)
                    logger.info(f"Solicitação para IE {solicitacao['inscricao_estadual']} (ID: {solicitacao['id']}) FALHOU - Navegador não disponível")
                return 0
        else:
            navegador = navegador_global

        link = os.environ.get('LINK_SEFAZ_NFCE')
        acessar_pagina(navegador, link)

        for indice, solicitacao in enumerate(solicitacoes, 1):
            novo_lote = (indice-1) // tamanho_lote
            if novo_lote > lote_atual:
                lote_atual = novo_lote
                logger.info(f"Processando lote {lote_atual+1} ({indice-1}-{min(indice+tamanho_lote-1, len(solicitacoes))}) de {len(solicitacoes)} solicitações")

            if solicitar_nfce(navegador, solicitacao):
                solicitacoes_processadas += 1
            else:
                logger.info(f"Falha ao processar solicitação {solicitacao['id']} - IE: {solicitacao['inscricao_estadual']}")

            # Atualiza a página a cada 10 solicitações ou a cada solicitação se necessário
            if indice % 10 == 0:
                try:
                    acessar_pagina(navegador, link)
                except Exception as e:
                    logger.error(f"Erro ao acessar página: {e}. Reinicializando navegador.")
                    navegador = inicializar_navegador()
                    if navegador is None:
                        break
                    acessar_pagina(navegador, link)
            else:
                acessar_pagina(navegador, link)

        logger.info(f"Processamento concluído: {solicitacoes_processadas} de {len(solicitacoes)} solicitações processadas com sucesso")
        return solicitacoes_processadas

    except Exception as e:
        logger.error(f"Erro durante o processamento de solicitações: {e}")
        fechar_navegador()
        return solicitacoes_processadas

def configurar_tratamento_sinais():
    def handler_signal(signum, frame):
        global RUNNING
        logger.info(f"Sinal {signum} recebido. Preparando para encerrar serviço...")
        RUNNING = False

        # Fecha o navegador corretamente
        if navegador_global:
            try:
                navegador_global.quit()
            except:
                pass

        # Usa sys.exit() para permitir finalização limpa
        logger.info("Serviço finalizado pelo usuário.")
        sys.exit(0)

    signal.signal(signal.SIGINT, handler_signal)
    signal.signal(signal.SIGTERM, handler_signal)

def monitorar_solicitacoes_continuamente():
    global RUNNING, navegador_global

    execucao_id = f"SOLICITADOR-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    logger.info(f"=" * 50)
    logger.info(f"INICIANDO SERVIÇO DE SOLICITAÇÃO DE XML ({execucao_id})")
    logger.info(f"=" * 50)

    configurar_tratamento_sinais()

    intervalo_verificacao = int(os.environ.get("INTERVALO_VERIFICACAO", 60))
    tempo_inatividade_fechar_navegador = int(os.environ.get("TEMPO_INATIVIDADE_NAVEGADOR", 300))

    ultima_atividade_navegador = None
    ultimo_log_sem_solicitacoes = 0
    intervalo_min_log_sem_solicitacoes = 300  # 5 minutos

    while RUNNING:
        try:
            # Verifica se há solicitações pendentes antes de iniciar o navegador
            solicitacoes = obter_solicitacoes_pendentes()

            # Se não houver solicitações e o navegador estiver aberto, fecha ele
            if not solicitacoes:
                if navegador_global:
                    logger.info("Não há solicitações pendentes. Fechando navegador para economizar recursos...")
                    fechar_navegador()

                # Log periódico sobre ausência de solicitações
                agora = time.time()
                if agora - ultimo_log_sem_solicitacoes > intervalo_min_log_sem_solicitacoes:
                    logger.info("Não há solicitações pendentes. Aguardando novas solicitações...")
                    ultimo_log_sem_solicitacoes = agora

                # Aguarda antes de verificar novamente
                time.sleep(intervalo_verificacao)
                continue

            # Se há solicitações, processa
            qtd_processada = processar_solicitacoes()

            if qtd_processada > 0:
                logger.info(f"{qtd_processada} solicitações processadas com sucesso")
                ultima_atividade_navegador = time.time()
                time.sleep(5)
            else:
                # Fecha navegador por inatividade
                if navegador_global and ultima_atividade_navegador:
                    tempo_inativo = time.time() - ultima_atividade_navegador
                    if tempo_inativo > tempo_inatividade_fechar_navegador:
                        fechar_navegador()

                time.sleep(intervalo_verificacao)

        except Exception as e:
            logger.error(f"Erro durante o monitoramento: {e}")
            time.sleep(30)

    fechar_navegador()
    logger.info("Serviço de solicitação XML encerrado normalmente")

if __name__ == "__main__":
    try:
        monitorar_solicitacoes_continuamente()
    except KeyboardInterrupt:
        logger.info("Serviço de solicitação interrompido pelo usuário")
        fechar_navegador()
    except Exception as e:
        logger.critical(f"Erro fatal no serviço de solicitação: {str(e)}")
        sys.exit(1)