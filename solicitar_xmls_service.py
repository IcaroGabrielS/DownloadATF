import time, logging, os, signal, sys, gc
import psutil
import threading
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
from utils import (conectar_postgres, iniciar_navegador_selenoid, autenticar_sefaz, acessar_pagina, espera_para_clicar)

load_dotenv()
os.makedirs("logs", exist_ok=True)

from logging.handlers import RotatingFileHandler
MAX_LOG_SIZE = 220 * 1024 * 1024  # 220 MB
logging.getLogger().setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_file = "/home/desenvolvimento/DownloadATF/logs/solicitar_xmls_service.log"
file_handler = RotatingFileHandler(log_file, maxBytes=MAX_LOG_SIZE, backupCount=5, encoding='utf-8')
file_handler.setFormatter(log_formatter)
logging.getLogger().addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logging.getLogger().addHandler(console_handler)

# Variáveis globais para controle do serviço
RUNNING = True
WATCHDOG_TIMEOUT = 600  # 10 minutos em segundos
last_heartbeat = time.time()
watchdog_timer = None
service_pid = os.getpid()
navegador_global = None

# Limites para controle de memória (em porcentagem)
MEMORY_WARNING_THRESHOLD = 70.0  # Aviso quando atingir 70% da memória
MEMORY_RESTART_THRESHOLD = 85.0  # Reinicia quando atingir 85% da memória
MEMORY_CHECK_INTERVAL = 30  # Verifica a cada 30 segundos

# Contador de ciclos para reinicialização preventiva
CYCLE_RESET_COUNT = 20  # Reinicia após 20 ciclos completos

def reset_watchdog():
    """Reseta o watchdog timer"""
    global last_heartbeat, watchdog_timer
    
    # Cancela o timer atual se existir
    if watchdog_timer is not None:
        watchdog_timer.cancel()
    
    # Atualiza o último heartbeat
    last_heartbeat = time.time()
    
    # Cria novo timer
    watchdog_timer = threading.Timer(WATCHDOG_TIMEOUT, watchdog_expired)
    watchdog_timer.daemon = True
    watchdog_timer.start()

def watchdog_expired():
    """Função chamada quando o watchdog expira (serviço travado)"""
    global service_pid, navegador_global
    
    logging.critical(f"WATCHDOG EXPIROU! O serviço parece estar travado há {WATCHDOG_TIMEOUT} segundos")
    logging.critical(f"Forçando reinicialização do serviço (PID: {service_pid})")
    
    # Registra informações de diagnóstico
    process = psutil.Process(service_pid)
    memoria_percentual = process.memory_percent()
    memoria_info = process.memory_info()
    
    logging.critical(f"Uso de memória antes da reinicialização: {memoria_percentual:.1f}% ({memoria_info.rss / (1024*1024):.1f} MB)")
    
    # Tenta fechar o navegador se estiver aberto
    try:
        if navegador_global is not None:
            navegador_global.quit()
    except:
        pass
    
    # Tenta fechar conexões e liberar recursos
    try:
        gc.collect()  # Força coleta de lixo
        logging.info("Coleta de lixo executada")
    except:
        pass
    
    # Encerra o processo atual, o systemd (ou outro supervisor) irá reiniciá-lo
    os._exit(1)

def check_memory_usage():
    """Verifica o uso de memória e toma ações se necessário"""
    try:
        # Obtém o processo atual
        process = psutil.Process(os.getpid())
        
        # Obtém o percentual de uso de memória
        memoria_percentual = process.memory_percent()
        memoria_rss_mb = process.memory_info().rss / (1024 * 1024)
        
        # Registra estatísticas de uso de memória
        if memoria_percentual > MEMORY_WARNING_THRESHOLD:
            logging.warning(f"ALERTA DE MEMÓRIA: Uso atual {memoria_percentual:.1f}% ({memoria_rss_mb:.1f} MB)")
            
            # Executa coleta de lixo
            gc.collect()
            
            # Se ultrapassou o limite de reinicialização
            if memoria_percentual > MEMORY_RESTART_THRESHOLD:
                logging.critical(f"LIMITE DE MEMÓRIA EXCEDIDO: {memoria_percentual:.1f}% ({memoria_rss_mb:.1f} MB)")
                logging.critical("Iniciando reinicialização controlada do serviço")
                
                # Tenta fechar o navegador se estiver aberto
                global navegador_global
                if navegador_global is not None:
                    try:
                        navegador_global.quit()
                    except:
                        pass
                    navegador_global = None
                
                # Define a flag global para encerrar o loop principal
                global RUNNING
                RUNNING = False
                
                # Encerra o processo após um pequeno atraso para finalizar atividades
                threading.Timer(3.0, lambda: os._exit(2)).start()
                
        return memoria_percentual
    except Exception as e:
        logging.error(f"Erro ao verificar uso de memória: {e}")
        return 0.0

def obter_solicitacoes_pendentes(limite=20):
    """Busca solicitações pendentes no banco de dados PostgreSQL com limite"""
    solicitacoes_pendentes = []

    conexao = conectar_postgres()
    if not conexao:
        logging.error("Falha ao conectar ao PostgreSQL")
        return []

    try:
        cursor = conexao.cursor()
        # Busca solicitações com contador de tentativas = 0
        cursor.execute("""
            SELECT id, inscricao_estadual, data_ini, data_fim
            FROM solicitacoes
            WHERE solicitado = 0 AND tipo = 'NFCE'
            ORDER BY data_solicitacao
            LIMIT %s
        """, (limite,))

        for id, inscricao_estadual, data_ini, data_fim in cursor.fetchall():
            solicitacoes_pendentes.append({
                "id": id,
                "inscricao_estadual": inscricao_estadual,
                "data_ini": data_ini,
                "data_fim": data_fim
            })

        cursor.close()
        conexao.close()

        if solicitacoes_pendentes:
            logging.info(f"Encontradas {len(solicitacoes_pendentes)} solicitações pendentes")
        return solicitacoes_pendentes

    except Exception as erro:
        logging.error(f"Erro ao obter solicitações pendentes: {erro}")
        if conexao:
            conexao.close()
        return []

def atualizar_solicitacao(id_solicitacao, horario=None, sucesso=True):
    """Atualiza a solicitação no banco de dados, incrementando o contador"""
    conexao = conectar_postgres()
    if not conexao:
        logging.error(f"Falha ao conectar ao PostgreSQL para atualizar solicitação {id_solicitacao}")
        return False

    try:
        cursor = conexao.cursor()
        
        # Se não foi fornecido horário (caso de falha), usar o horário atual
        if horario is None:
            horario = datetime.now()

        cursor.execute("""
            UPDATE solicitacoes
            SET solicitado = solicitado + 1, horario = %s, atualizado_em = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (horario, id_solicitacao))

        conexao.commit()
        cursor.close()
        conexao.close()

        status = "sucesso" if sucesso else "falha"
        logging.info(f"Solicitação {id_solicitacao} atualizada com {status}")
        return True

    except Exception as erro:
        logging.error(f"Erro ao atualizar solicitação: {erro}")
        if conexao:
            conexao.rollback()
            conexao.close()
        return False

def inserir_datas_formulario(navegador, data_inicio, data_fim, espera=2):
    reset_watchdog()  # Reset watchdog durante interação com o navegador
    wait = WebDriverWait(navegador, espera)
    data_inicio_elemento = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get('XPATH_DATA_INICIO'))))
    data_inicio_elemento.clear()
    data_inicio_elemento.send_keys(data_inicio)
    data_fim_elemento = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get('XPATH_DATA_FIM'))))
    data_fim_elemento.clear()
    data_fim_elemento.send_keys(data_fim)

def preencher_campo_iframe(navegador, ie_empresa, espera=2):
    reset_watchdog()  # Reset watchdog durante interação com o navegador
    wait = WebDriverWait(navegador, espera)
    wait.until(EC.frame_to_be_available_and_switch_to_it((By.XPATH, os.environ.get('XPATH_IFRAME'))))
    campo_valor = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get('XPATH_CAMPO_VALOR'))))
    campo_valor.clear()
    campo_valor.send_keys(ie_empresa)
    botao_pesquisar = wait.until(EC.element_to_be_clickable((By.XPATH, os.environ.get('XPATH_BOTAO_PESQUISAR'))))
    botao_pesquisar.click()
    navegador.switch_to.default_content()

def selecionar_xml_executar(navegador, espera=2):
    reset_watchdog()  # Reset watchdog durante interação com o navegador
    wait = WebDriverWait(navegador, espera)
    dropdown_xml = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get('XPATH_DROPDOWN_XML'))))
    dropdown_xml.click()
    opcao_xml = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get('XPATH_OPCAO_XML'))))
    opcao_xml.click()
    botao_executar = wait.until(EC.element_to_be_clickable((By.XPATH, os.environ.get('XPATH_BOTAO_EXECUTAR'))))
    espera_para_clicar()  # Função importante mantida conforme observação
    botao_executar.click()

def solicitar_nfce(navegador, solicitacao, espera=2):
    try:
        reset_watchdog()  # Reset watchdog antes de começar a solicitação
        logging.info(f"Solicitando NFC-e para IE: {solicitacao['inscricao_estadual']}")
        inserir_datas_formulario(navegador, solicitacao["data_ini"], solicitacao["data_fim"], espera)
        preencher_campo_iframe(navegador, solicitacao["inscricao_estadual"], espera)
        selecionar_xml_executar(navegador, espera)

        # Capturar data/hora atual
        horario = datetime.now()

        # Atualizar banco de dados
        sucesso = atualizar_solicitacao(solicitacao["id"], horario, True)

        # Esperar download iniciar
        time.sleep(10)
        return sucesso
    except Exception as e:
        logging.error(f"Erro ao solicitar NFCE para IE {solicitacao['inscricao_estadual']}: {e}")
        # Importante: atualizar o contador mesmo em caso de falha para evitar loop infinito
        atualizar_solicitacao(solicitacao["id"], None, False)
        return False

def inicializar_navegador():
    """Inicializa o navegador e faz login"""
    global navegador_global
    
    try:
        # Fechar navegador existente se houver
        if navegador_global is not None:
            try:
                navegador_global.quit()
            except:
                pass
            navegador_global = None
        
        # Inicializar novo navegador
        logging.info("Inicializando o navegador...")
        browser_type = os.environ.get("SELENOID_BROWSER", "chrome")
        navegador = iniciar_navegador_selenoid(browser_type=browser_type)
        
        if navegador and autenticar_sefaz(navegador):
            navegador_global = navegador
            logging.info("Navegador inicializado e autenticado com sucesso")
            return navegador
        else:
            if navegador:
                try:
                    navegador.quit()
                except:
                    pass
            logging.error("Falha ao autenticar no sistema da SEFAZ")
            return None
    except Exception as e:
        logging.error(f"Erro ao inicializar navegador: {e}")
        return None

def fechar_navegador():
    """Fecha o navegador global se estiver aberto"""
    global navegador_global
    
    if navegador_global is not None:
        logging.info("Fechando navegador por inatividade")
        try:
            navegador_global.quit()
        except Exception as e:
            logging.warning(f"Erro ao fechar navegador: {e}")
        finally:
            navegador_global = None
            gc.collect()  # Forçar coleta de lixo

def processar_lote_solicitacoes(solicitacoes, limite=20):
    """Processa um lote de solicitações pendentes"""
    global navegador_global
    reset_watchdog()  # Reset watchdog ao iniciar o processo
    
    if not solicitacoes:
        return 0

    logging.info(f"Iniciando solicitações de NFC-e para {len(solicitacoes)} registros...")
    solicitacoes_processadas = 0

    try:
        # Usar navegador global ou inicializar um novo
        if navegador_global is None:
            navegador = inicializar_navegador()
            if navegador is None:
                # Falha na inicialização do navegador, marcar todas as solicitações
                logging.error("Falha na inicialização do navegador")
                for solicitacao in solicitacoes:
                    atualizar_solicitacao(solicitacao["id"], None, False)
                return 0
        else:
            navegador = navegador_global

        # Navegar para a página de solicitação
        link = os.environ.get('LINK_SEFAZ_NFCE')
        acessar_pagina(navegador, link)

        for solicitacao in solicitacoes:
            # Verificar memória antes de processar cada solicitação
            memoria_percentual = check_memory_usage()
            if memoria_percentual > MEMORY_RESTART_THRESHOLD:
                logging.warning("Limite de memória atingido durante processamento. Finalizando ciclo atual.")
                break

            reset_watchdog()  # Reset watchdog antes de processar cada solicitação
            if solicitar_nfce(navegador, solicitacao):
                logging.info(f"Solicitação {solicitacao['id']} processada com sucesso")
                solicitacoes_processadas += 1
            else:
                logging.warning(f"Falha ao processar solicitação {solicitacao['id']}")

            # Recarregar página para próxima solicitação
            acessar_pagina(navegador, link)

        logging.info(f"Processo concluído: {solicitacoes_processadas} solicitações processadas com sucesso")
        return solicitacoes_processadas
        
    except Exception as e:
        logging.error(f"Erro durante o processamento do lote: {e}")
        # Em caso de exceção geral, marcar todas as solicitações restantes
        for solicitacao in solicitacoes:
            atualizar_solicitacao(solicitacao["id"], None, False)
        
        # Fechar navegador em caso de erro para garantir estado limpo
        fechar_navegador()
        return solicitacoes_processadas

def configurar_tratamento_sinais():
    """Configura tratamento de sinais para encerramento adequado do serviço"""
    def handler_signal(signum, frame):
        global RUNNING
        logging.info(f"Sinal {signum} recebido. Preparando para encerrar serviço...")
        RUNNING = False
    
    # Registrar handlers para sinais SIGINT (Ctrl+C) e SIGTERM
    signal.signal(signal.SIGINT, handler_signal)
    signal.signal(signal.SIGTERM, handler_signal)

def heartbeat_log(modo="ativo"):
    """Registra sinal de vida do serviço no log"""
    mem_percent = check_memory_usage()
    mem_info = psutil.Process(os.getpid()).memory_info()
    mem_mb = mem_info.rss / (1024 * 1024)
    
    status_navegador = "com navegador" if navegador_global is not None else "sem navegador"
    logging.info(f"HEARTBEAT: Serviço {modo} {status_navegador}. Uso de memória: {mem_percent:.1f}% ({mem_mb:.1f} MB)")

def executar_servico():
    """Função principal que executa o serviço em loop"""
    global RUNNING, navegador_global
    
    logging.info("=" * 80)
    logging.info("INICIANDO SERVIÇO DE MONITORAMENTO E SOLICITAÇÃO DE ARQUIVOS XML")
    logging.info("=" * 80)
    
    # Configurar tratamento de sinais
    configurar_tratamento_sinais()
    
    # Iniciar watchdog
    reset_watchdog()
    
    # Obter configurações do ambiente
    limite_solicitacoes = int(os.environ.get("LIMITE_SOLICITACOES", 20))
    intervalo_verificacao = int(os.environ.get("INTERVALO_VERIFICACAO", 60))  # segundos
    tempo_inatividade_fechar_navegador = int(os.environ.get("TEMPO_INATIVIDADE_NAVEGADOR", 300))  # 5 minutos
    
    logging.info(f"Serviço iniciado com limite de {limite_solicitacoes} solicitações por ciclo")
    logging.info(f"Intervalo de verificação: {intervalo_verificacao} segundos")
    logging.info(f"Tempo de inatividade para fechar navegador: {tempo_inatividade_fechar_navegador} segundos")
    logging.info(f"Monitoramento de memória: alerta em {MEMORY_WARNING_THRESHOLD}%, reinício em {MEMORY_RESTART_THRESHOLD}%")
    
    # Registrar PID para possível uso por scripts de monitoramento externos
    pid_file = "/home/desenvolvimento/DownloadATF/solicitar_xmls_service.pid"
    try:
        with open(pid_file, 'w') as f:
            f.write(str(os.getpid()))
        logging.info(f"PID {os.getpid()} registrado em {pid_file}")
    except Exception as e:
        logging.warning(f"Não foi possível criar arquivo PID: {e}")
    
    # Loop principal do serviço
    ultima_execucao_com_resultados = None
    ultima_verificacao_memoria = time.time()
    ciclo_contador = 0
    ultima_heartbeat_log = time.time()
    ultima_atividade_navegador = None
    
    while RUNNING:
        try:
            # Reset o watchdog a cada iteração
            reset_watchdog()
            
            # Verificar memória periodicamente
            agora = time.time()
            if agora - ultima_verificacao_memoria > MEMORY_CHECK_INTERVAL:
                check_memory_usage()
                ultima_verificacao_memoria = agora
            
            # Log de heartbeat periódico
            if agora - ultima_heartbeat_log > 1800:  # 30 minutos
                heartbeat_log("monitoramento" if navegador_global is None else "ativo")
                ultima_heartbeat_log = agora
            
            # Verificar se há solicitações pendentes
            solicitacoes = obter_solicitacoes_pendentes(limite_solicitacoes)
            
            # Se temos solicitações para processar
            if solicitacoes:
                # Processar o lote de solicitações
                qtd_processada = processar_lote_solicitacoes(solicitacoes, limite_solicitacoes)
                ciclo_contador += 1
                
                # Atualizar timestamp da última atividade do navegador
                if qtd_processada > 0 and navegador_global is not None:
                    ultima_atividade_navegador = time.time()
                
                # Reiniciar preventivamente após um certo número de ciclos
                if ciclo_contador >= CYCLE_RESET_COUNT:
                    logging.info(f"Reinicialização preventiva após {ciclo_contador} ciclos")
                    logging.info("Esta é uma medida de segurança para evitar problemas de memória a longo prazo")
                    # Encerrar o processo - será reiniciado pelo systemd ou supervisor
                    os._exit(0)
                
                if qtd_processada > 0:
                    logging.info(f"Ciclo {ciclo_contador} concluído - {qtd_processada} solicitações processadas")
                    ultima_execucao_com_resultados = datetime.now()
                    # Dormir apenas um curto tempo antes de verificar novamente por mais solicitações
                    time.sleep(5)
                else:
                    # Falha no processamento, esperar um pouco antes de tentar novamente
                    logging.warning("Falha no processamento do lote, esperando antes de tentar novamente")
                    time.sleep(30)
            else:
                # Se não tiver nada para processar, fechar o navegador após tempo de inatividade
                if navegador_global is not None:
                    if ultima_atividade_navegador is not None:
                        tempo_inativo = time.time() - ultima_atividade_navegador
                        if tempo_inativo > tempo_inatividade_fechar_navegador:
                            logging.info(f"Navegador inativo por {tempo_inativo:.1f} segundos. Fechando para economizar recursos...")
                            fechar_navegador()
                            ultima_atividade_navegador = None
                
                # Registrar status periodicamente quando não há solicitações
                now = datetime.now()
                if ultima_execucao_com_resultados is None or (now - ultima_execucao_com_resultados).total_seconds() > 3600:
                    logging.info(f"Serviço em monitoramento - Ciclo {ciclo_contador} - Sem solicitações pendentes")
                    ultima_execucao_com_resultados = now
                
                # Dormir pelo intervalo de verificação completo
                time.sleep(intervalo_verificacao)
        
        except Exception as e:
            logging.error(f"Erro no ciclo de verificação do serviço: {e}", exc_info=True)
            # Dormir um pouco antes de tentar novamente
            time.sleep(30)
    
    # Cancelar watchdog antes de encerrar
    if watchdog_timer is not None:
        watchdog_timer.cancel()
    
    # Fechar navegador se estiver aberto
    fechar_navegador()
    
    logging.info("Serviço de solicitação XML encerrado normalmente")

if __name__ == "__main__":
    # Verificar se psutil está disponível
    if 'psutil' not in sys.modules:
        logging.critical("Módulo psutil não encontrado! Ele é necessário para monitoramento de memória.")
        logging.critical("Instale com: pip install psutil")
        sys.exit(1)
        
    executar_servico()
