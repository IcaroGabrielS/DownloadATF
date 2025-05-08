import re, os, time, sys, signal
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from dotenv import load_dotenv
from utils import (conectar_postgres, iniciar_navegador_selenoid, autenticar_sefaz, acessar_pagina)
from loggingConfig import get_logger

load_dotenv()
os.makedirs("logs", exist_ok=True)
logger = get_logger(__name__)

ULTIMO_LOG_SEM_SOLICITACOES = 0
INTERVALO_MIN_LOG_SEM_SOLICITACOES = 300

# Variável global para armazenar referência ao navegador
navegador_global = None

# Função para lidar com sinais (como Ctrl+C)
def manipulador_sinal(sig, frame):
    global navegador_global
    logger.info("Sinal de interrupção recebido (Ctrl+C). Encerrando serviço...")

    if navegador_global:
        try:
            logger.info("Fechando navegador Selenoid...")
            navegador_global.quit()
            logger.info("Navegador Selenoid fechado com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao fechar navegador: {str(e)}")

    logger.info("Serviço de monitoramento encerrado pelo usuário.")
    sys.exit(0)

def acessar_pagina_com_timeout_estendido(navegador, url, timeout_maximo=300):
    """
    Função wrapper para acessar página com timeout estendido até 5 minutos
    """
    inicio = time.time()
    logger.info(f"Tentando acessar {url} com timeout estendido de {timeout_maximo} segundos...")

    tentativas = 0
    while time.time() - inicio < timeout_maximo:
        tentativas += 1
        try:
            acessar_pagina(navegador, url)
            tempo_decorrido = time.time() - inicio
            logger.info(f"Página carregada com sucesso após {tempo_decorrido:.1f} segundos (tentativa {tentativas})")
            return True
        except Exception as e:
            tempo_decorrido = time.time() - inicio
            tempo_restante = timeout_maximo - tempo_decorrido

            if tempo_restante <= 0:
                logger.error(f"Tempo máximo excedido ({timeout_maximo}s) ao tentar acessar a página")
                raise

            logger.warning(f"Falha ao acessar página (tentativa {tentativas}): {str(e)}. "
                          f"Tempo decorrido: {tempo_decorrido:.1f}s. Tentando novamente... "
                          f"(restam {tempo_restante:.1f}s)")
            # Pequena pausa entre tentativas
            time.sleep(min(5, tempo_restante))

    # Se chegou aqui é porque o timeout foi excedido
    logger.error(f"Não foi possível acessar a página após {timeout_maximo} segundos")
    raise TimeoutException(f"Timeout excedido ({timeout_maximo}s) ao acessar {url}")

def obter_solicitacoes_solicitadas():
    logger.info("Iniciando consulta por solicitações pendentes no banco de dados...")
    conexao = conectar_postgres()
    if not conexao:
        logger.info("Não foi possível conectar ao banco de dados para obter solicitações")
        return []

    try:
        cursor = conexao.cursor()
        cursor.execute("""
            SELECT id, inscricao_estadual, horario, criado_em
            FROM nfce.solicitacoes
            WHERE solicitado > 0 AND (link IS NULL OR link = '') AND baixado = 0 AND tipo = 'NFCE'
            AND (mensagens < 4 OR mensagens IS NULL)
            ORDER BY criado_em
        """)
        logger.info("Consulta SQL executada, processando resultados...")

        solicitacoes = []
        total_solicitacoes = 0

        for id, inscricao_estadual, horario, criado_em in cursor.fetchall():
            total_solicitacoes += 1

            horario_str = None
            horario_dt = None
            if horario:
                horario_str = horario.strftime("%d/%m/%Y %H:%M:%S")
                horario_dt = horario

            solicitacoes.append({
                "id": id,
                "inscricao_estadual": inscricao_estadual,
                "horario": horario_str,
                "horario_dt": horario_dt  # Mantém o objeto datetime original
            })

        cursor.close()
        conexao.close()

        if len(solicitacoes) > 0:
            logger.info(f"Encontradas {len(solicitacoes)} solicitações válidas aguardando links")
        else:
            logger.info("Nenhuma solicitação válida aguardando links foi encontrada")

        return solicitacoes

    except Exception as erro:
        logger.error(f"Erro ao buscar solicitações: {erro}")
        if conexao:
            conexao.close()
        return []

def atualizar_link_solicitacao(id_solicitacao, link):
    logger.info(f"Atualizando link para solicitação {id_solicitacao}")
    conexao = conectar_postgres()
    if not conexao:
        logger.error(f"Falha ao conectar ao PostgreSQL para atualizar link da solicitação {id_solicitacao}")
        return False

    try:
        cursor = conexao.cursor()
        cursor.execute("""
            UPDATE nfce.solicitacoes
            SET link = %s, atualizado_em = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (link, id_solicitacao))

        conexao.commit()
        cursor.close()
        conexao.close()
        logger.info(f"Link atualizado com sucesso para solicitação {id_solicitacao}")
        return True

    except Exception as erro:
        logger.error(f"Erro ao atualizar link da solicitação {id_solicitacao}: {erro}")
        if conexao:
            conexao.rollback()
            conexao.close()
        return False

def atualizar_status_anexo(id_solicitacao, tem_anexo):
    """Atualiza o status do anexo no banco de dados."""
    conexao = conectar_postgres()
    if not conexao:
        logger.error(f"Falha ao conectar ao PostgreSQL para atualizar status do anexo da solicitação {id_solicitacao}")
        return False

    try:
        cursor = conexao.cursor()
        cursor.execute("""
            UPDATE nfce.solicitacoes
            SET anexo = %s, atualizado_em = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (tem_anexo, id_solicitacao))

        conexao.commit()
        cursor.close()
        conexao.close()
        status_texto = "Com" if tem_anexo else "Sem"
        logger.info(f"{status_texto} anexo - Solicitação {id_solicitacao}")
        return True

    except Exception as erro:
        logger.error(f"Erro ao atualizar status de anexo da solicitação {id_solicitacao}: {erro}")
        if conexao:
            conexao.rollback()
            conexao.close()
        return False

def atualizar_quantidade_mensagens(id_solicitacao, quantidade_mensagens):
    """Atualiza a quantidade de mensagens no banco de dados."""
    conexao = conectar_postgres()
    if not conexao:
        logger.error(f"Falha ao conectar ao PostgreSQL para atualizar quantidade de mensagens da solicitação {id_solicitacao}")
        return False

    try:
        cursor = conexao.cursor()
        cursor.execute("""
            UPDATE nfce.solicitacoes
            SET mensagens = %s, atualizado_em = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (quantidade_mensagens, id_solicitacao))

        conexao.commit()
        cursor.close()
        conexao.close()
        logger.info(f"Quantidade de mensagens atualizada para {quantidade_mensagens} - Solicitação {id_solicitacao}")
        return True

    except Exception as erro:
        logger.error(f"Erro ao atualizar quantidade de mensagens da solicitação {id_solicitacao}: {erro}")
        if conexao:
            conexao.rollback()
            conexao.close()
        return False

def processar_links_disponíveis(navegador, solicitacoes):
    logger.info("Procurando links de download disponíveis")

    # Aumentado o timeout do WebDriverWait para 30 segundos
    wait = WebDriverWait(navegador, 30)
    try:
        logger.info("Aguardando carregamento da tabela de resultados...")
        wait.until(EC.presence_of_element_located((By.XPATH, "//table/tbody/tr")))
        logger.info("Tabela carregada, processando linhas...")
    except:
        logger.warning("Timeout ao aguardar carregamento da tabela")

    try:
        # Agora obtemos todas as linhas da tabela, sem filtrar por anexo inicialmente
        linhas = navegador.find_elements(By.XPATH, "//table/tbody/tr")
        total_linhas = len(linhas)
        logger.info(f"Encontradas {total_linhas} linhas para processar")
    except Exception as e:
        logger.error(f"Erro ao localizar linhas da tabela: {str(e)}")
        return 0

    links_encontrados = 0
    processadas = 0
    anexos_atualizados = 0
    solicitacoes_por_horario = {item["horario"]: item for item in solicitacoes if item["horario"]}
    logger.info(f"Classificadas {len(solicitacoes_por_horario)} solicitações por horário para correspondência")

    inicio = time.time()
    for linha in linhas:
        try:
            processadas += 1
            if processadas % 50 == 0:
                tempo_decorrido = time.time() - inicio
                logger.info(f"Progresso: {processadas}/{total_linhas} linhas ({(processadas/total_linhas*100):.1f}%) em {tempo_decorrido:.1f}s")

            # PASSO 1: Verificar se o texto na quarta coluna começa com "FIS_1484"
            try:
                coluna4 = linha.find_element(By.XPATH, "./td[4]/a")
                if not coluna4.text.strip().startswith("FIS_1484"):
                    continue
            except:
                continue

            # PASSO 2: Extrair quantidade de mensagens
            quantidade_mensagens = None
            try:
                coluna_mensagens = linha.find_element(By.XPATH, "./td[4]/a/i")
                texto_mensagens = coluna_mensagens.text.strip()

                # Extrair o número de mensagens (normalmente entre 1 e 9)
                match = re.search(r'(\d)', texto_mensagens)
                if match:
                    quantidade_mensagens = int(match.group(1))
            except:
                pass

            # PASSO 3: Verificar se tem anexo
            tem_anexo = False
            try:
                anexo = linha.find_element(By.XPATH, "./td[3]/a/img[@alt='Anexo']")
                if anexo:
                    tem_anexo = True
            except:
                tem_anexo = False

            try:
                link = linha.find_element(By.XPATH, "./td[6]/a")
                href = link.get_attribute("href")
                link_text = link.text.strip()
            except:
                continue

            match = re.match(r"javascript:abrirFilhas\('(\d+)',(\d+)\)", href)
            if not match:
                continue

            mensagem_id = match.group(1)
            url = f"https://www4.sefaz.pb.gov.br/atf/seg/SEGf_MinhasMensagens.do?hidsqMensagem={mensagem_id}"

            item_encontrado = None
            # Verifica se o horário corresponde exatamente a alguma solicitação
            if link_text in solicitacoes_por_horario:
                item_encontrado = solicitacoes_por_horario[link_text]
            else:
                # Verifica com tolerância de tempo
                try:
                    link_time = datetime.strptime(link_text, "%d/%m/%Y %H:%M:%S")
                    for item in solicitacoes:
                        if item["horario"] and abs((datetime.strptime(item["horario"], "%d/%m/%Y %H:%M:%S") - link_time).total_seconds()) <= 10:
                            item_encontrado = item
                            break
                except ValueError:
                    logger.warning(f"Formato de data inválido: {link_text}")
                    continue

            if item_encontrado:
                # Processa todas as solicitações independentemente do número de mensagens
                # Atualiza o link da solicitação
                atualizar_link_solicitacao(item_encontrado["id"], url)
                links_encontrados += 1

                # Atualiza o status do anexo
                atualizar_status_anexo(item_encontrado["id"], tem_anexo)
                anexos_atualizados += 1

                # Atualiza a quantidade de mensagens se disponível
                if quantidade_mensagens is not None:
                    atualizar_quantidade_mensagens(item_encontrado["id"], quantidade_mensagens)

                status_anexo = "com anexo" if tem_anexo else "sem anexo"
                logger.info(f"Link adicionado para solicitação {item_encontrado['id']} (horário {link_text}) - {status_anexo}, {quantidade_mensagens} mensagens")

        except Exception as e:
            logger.debug(f"Erro ao processar linha: {str(e)}")
            continue

    tempo_total = time.time() - inicio
    if links_encontrados > 0:
        logger.info(f"Processamento concluído: {links_encontrados} links encontrados ({anexos_atualizados} atualizados com status de anexo) em {processadas}/{total_linhas} linhas ({tempo_total:.1f}s)")
    else:
        logger.info(f"Processamento concluído: nenhum link encontrado em {processadas}/{total_linhas} linhas ({tempo_total:.1f}s)")

    return links_encontrados

def verificar_necessidade_renovar_sessao(navegador, ultima_verificacao):
    TEMPO_MAXIMO_SESSAO = 1800
    tempo_atual = time.time()
    tempo_decorrido = tempo_atual - ultima_verificacao

    if tempo_decorrido > TEMPO_MAXIMO_SESSAO:
        logger.info(f"Renovando sessão do navegador após {tempo_decorrido:.0f}s de uso...")
        try:
            # Usando a nova função com timeout estendido
            acessar_pagina_com_timeout_estendido(navegador, os.environ.get('URL_CAIXA_DOWNLOADS'), 300)
            logger.info("Sessão renovada com sucesso")
            return tempo_atual, True
        except:
            logger.error("Falha ao renovar sessão do navegador")
            return tempo_atual, False

    return ultima_verificacao, True

def monitorar_solicitacoes_continuamente():
    global ULTIMO_LOG_SEM_SOLICITACOES
    global navegador_global

    execucao_id = f"MONITOR-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    logger.info(f"=" * 50)
    logger.info(f"INICIANDO SERVIÇO DE MONITORAMENTO CONTÍNUO ({execucao_id})")
    logger.info(f"=" * 50)

    navegador = None
    ultima_verificacao = 0
    ciclos_sem_link = 0
    ciclos_sem_solicitacao = 0
    ciclos_totais = 0

    while True:
        ciclos_totais += 1
        hora_atual = datetime.now().strftime("%H:%M:%S")
        logger.info(f"Iniciando ciclo #{ciclos_totais} de verificação às {hora_atual}")

        try:
            # Primeiro verifica se há solicitações pendentes antes de iniciar o navegador
            solicitacoes = obter_solicitacoes_solicitadas()

            # Se não houver solicitações e o navegador estiver aberto, fecha ele
            if not solicitacoes:
                if navegador:
                    logger.info("Não há solicitações pendentes. Fechando navegador para economizar recursos...")
                    try:
                        navegador.quit()
                        navegador_global = None
                        logger.info("Navegador fechado com sucesso")
                    except:
                        logger.warning("Falha ao fechar navegador")
                    navegador = None

                # Log periódico sobre ausência de solicitações
                agora = time.time()
                if agora - ULTIMO_LOG_SEM_SOLICITACOES > INTERVALO_MIN_LOG_SEM_SOLICITACOES:
                    logger.info("Não há solicitações pendentes. Aguardando novas solicitações...")
                    ULTIMO_LOG_SEM_SOLICITACOES = agora

                # Tempo adaptativo para verificar novas solicitações
                ciclos_sem_solicitacao += 1
                tempo_espera = min(30 * (ciclos_sem_solicitacao // 5 + 1), 300)  # 30s, 60s, 90s... até máx 300s
                logger.info(f"Aguardando {tempo_espera}s até o próximo ciclo (ciclos sem solicitação: {ciclos_sem_solicitacao})")
                time.sleep(tempo_espera)
                continue

            # Reinicia contador de ciclos sem solicitação
            ciclos_sem_solicitacao = 0
            logger.info(f"Processando {len(solicitacoes)} solicitações pendentes")

            # Se há solicitações mas o navegador não está aberto, inicia ele
            sessao_valida = False
            if navegador:
                ultima_verificacao, sessao_valida = verificar_necessidade_renovar_sessao(navegador, ultima_verificacao)

            if not navegador or not sessao_valida:
                if navegador:
                    try:
                        logger.info("Reinicializando navegador...")
                        navegador.quit()
                        navegador_global = None
                    except:
                        logger.warning("Falha ao fechar navegador antigo")

                logger.info("Encontradas solicitações pendentes. Iniciando navegador...")
                navegador = iniciar_navegador_selenoid()
                navegador_global = navegador  # Atualiza a referência global
                if not navegador or not autenticar_sefaz(navegador):
                    logger.error("Falha na autenticação ou inicialização do navegador. Tentando novamente em 60 segundos...")
                    time.sleep(60)
                    continue

                ultima_verificacao = time.time()
                logger.info("Navegador iniciado com sucesso e autenticação realizada")

            # Processa as solicitações com tempo de espera estendido (até 5 minutos)
            logger.info("Acessando página de caixa de downloads...")
            try:
                # Usando a nova função com timeout estendido de 5 minutos (300 segundos)
                acessar_pagina_com_timeout_estendido(navegador, os.environ.get('URL_CAIXA_DOWNLOADS'), 300)
                links_encontrados = processar_links_disponíveis(navegador, solicitacoes)
            except TimeoutException:
                logger.error("Tempo esgotado ao tentar acessar a caixa de downloads. Tentando novamente no próximo ciclo.")
                time.sleep(60)
                continue
            except Exception as e:
                logger.error(f"Erro ao acessar caixa de downloads: {str(e)}")
                time.sleep(60)
                continue

            if links_encontrados > 0:
                logger.info(f"Encontrados {links_encontrados} links neste ciclo")
                ciclos_sem_link = 0
                tempo_espera = 15
            else:
                ciclos_sem_link += 1
                tempo_espera = min(60 * (ciclos_sem_link // 3 + 1), 300)
                logger.info(f"Nenhum link encontrado (ciclo {ciclos_sem_link}). Próxima verificação em {tempo_espera}s")

            time.sleep(tempo_espera)

        except Exception as e:
            logger.error(f"Erro durante o monitoramento: {str(e)}")
            if navegador:
                try:
                    navegador.quit()
                    navegador_global = None
                    logger.info("Navegador fechado após erro")
                except:
                    logger.warning("Falha ao fechar navegador após erro")
                navegador = None
            logger.info("Aguardando 60 segundos para tentar novamente após erro...")
            time.sleep(60)

if __name__ == "__main__":
    # Registra o manipulador de sinal para SIGINT (Ctrl+C)
    signal.signal(signal.SIGINT, manipulador_sinal)

    try:
        logger.info(f"Iniciando módulo de monitoramento de links na data {datetime.now().strftime('%d/%m/%Y')}")
        logger.info("Pressione Ctrl+C para encerrar o serviço de forma segura.")
        monitorar_solicitacoes_continuamente()
    except KeyboardInterrupt:
        # Este bloco não deve ser executado devido ao manipulador de sinal
        # Mas mantemos como fallback
        logger.info("Serviço de monitoramento interrompido pelo usuário")
        if navegador_global:
            try:
                logger.info("Fechando navegador Selenoid...")
                navegador_global.quit()
                logger.info("Navegador Selenoid fechado com sucesso.")
            except Exception as e:
                logger.error(f"Erro ao fechar navegador: {str(e)}")
    except Exception as e:
        logger.critical(f"Erro fatal no serviço de monitoramento: {str(e)}")
        if navegador_global:
            try:
                navegador_global.quit()
                logger.info("Navegador fechado após erro fatal.")
            except:
                pass
        sys.exit(1)