import re, logging, os, time
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
from utils import (conectar_postgres, iniciar_navegador_selenoid, autenticar_sefaz, acessar_pagina)

load_dotenv()

os.makedirs("logs", exist_ok=True)

from logging.handlers import RotatingFileHandler
MAX_LOG_SIZE = 220 * 1024 * 1024  # 220 MB
logging.getLogger().setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_file = "logs/localizar_links.log"
file_handler = RotatingFileHandler(log_file, maxBytes=MAX_LOG_SIZE, backupCount=5, encoding='utf-8')
file_handler.setFormatter(log_formatter)
logging.getLogger().addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logging.getLogger().addHandler(console_handler)

def obter_solicitacoes_solicitadas():
    """Busca solicitações que foram solicitadas mas ainda não foram baixadas"""
    conexao = conectar_postgres()
    if not conexao:
        return []
    
    try:
        cursor = conexao.cursor()
        # Buscar solicitações com solicitado > 0 (já solicitadas) e baixado = false (não baixadas)
        cursor.execute("""
            SELECT id, inscricao_estadual, horario
            FROM solicitacoes 
            WHERE solicitado > 0 AND (link IS NULL OR link = '') AND baixado = FALSE AND tipo = 'NFCE'
            ORDER BY data_solicitacao
        """)
        
        solicitacoes = []
        for id, inscricao_estadual, horario in cursor.fetchall():
            # Converter o objeto datetime para string formatada, se existir
            horario_str = None
            if horario:
                horario_str = horario.strftime("%d/%m/%Y %H:%M:%S")
                
            solicitacoes.append({
                "id": id,
                "inscricao_estadual": inscricao_estadual,
                "horario": horario_str
            })
        
        cursor.close()
        conexao.close()
        
        logging.info(f"Encontradas {len(solicitacoes)} solicitações aguardando links")
        return solicitacoes
    
    except Exception as erro:
        logging.error(f"Erro ao buscar solicitações: {erro}")
        if conexao:
            conexao.close()
        return []

def atualizar_link_solicitacao(id_solicitacao, link):
    """Atualiza o link de download da solicitação no banco de dados"""
    conexao = conectar_postgres()
    if not conexao:
        logging.error(f"Falha ao conectar ao PostgreSQL para atualizar link da solicitação {id_solicitacao}")
        return False
    
    try:
        cursor = conexao.cursor()
        cursor.execute("""
            UPDATE solicitacoes 
            SET link = %s, atualizado_em = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (link, id_solicitacao))
        
        conexao.commit()
        cursor.close()
        conexao.close()
        
        logging.info(f"Link atualizado para solicitação {id_solicitacao}")
        return True
    
    except Exception as erro:
        logging.error(f"Erro ao atualizar link da solicitação {id_solicitacao}: {erro}")
        if conexao:
            conexao.rollback()
            conexao.close()
        return False

def processar_links_disponíveis(navegador, solicitacoes):
    """Extrai e atualiza links de download da página"""
    logging.info("Procurando links de download disponíveis")
    
    # Adicionar timeout explícito para carregamento da tabela
    wait = WebDriverWait(navegador, 10)
    try:
        wait.until(EC.presence_of_element_located((By.XPATH, "//table/tbody/tr")))
        logging.info("Tabela carregada, processando linhas...")
    except:
        logging.warning("Timeout ao aguardar carregamento da tabela")
    
    try:
        # Obter apenas linhas com anexos para processar
        linhas = navegador.find_elements(By.XPATH, "//table/tbody/tr[td[3]/a/img[@alt='Anexo']]")
        total_linhas = len(linhas)
        logging.info(f"Encontradas {total_linhas} linhas com anexos para processar")
    except Exception as e:
        logging.error(f"Erro ao localizar linhas com anexos: {str(e)}")
        return 0
    
    links_encontrados = 0
    processadas = 0
    
    # Criar um dicionário de solicitações por horário para busca mais rápida
    solicitacoes_por_horario = {item["horario"]: item for item in solicitacoes if item["horario"]}
    
    inicio = time.time()
    for linha in linhas:
        try:
            processadas += 1
            if processadas % 10 == 0:
                tempo_decorrido = time.time() - inicio
                logging.info(f"Progresso: {processadas}/{total_linhas} linhas processadas ({(processadas/total_linhas*100):.1f}%) em {tempo_decorrido:.1f}s")
                
            # Verificar se o texto na coluna 4 começa com "FIS_1484"
            try:
                coluna4 = linha.find_element(By.XPATH, "./td[4]/a")
                if not coluna4.text.strip().startswith("FIS_1484"):
                    continue
            except:
                continue
                
            # Obtém o link na coluna 6
            try:
                link = linha.find_element(By.XPATH, "./td[6]/a")
                href = link.get_attribute("href")
                link_text = link.text.strip()
            except:
                continue
            
            # Processa o link JavaScript
            match = re.match(r"javascript:abrirFilhas\('(\d+)',(\d+)\)", href)
            if match:
                mensagem_id = match.group(1)
                url = f"https://www4.sefaz.pb.gov.br/atf/seg/SEGf_MinhasMensagens.do?hidsqMensagem={mensagem_id}"
                
                # Verifica se o horário está no nosso dicionário para correspondência rápida
                if link_text in solicitacoes_por_horario:
                    item = solicitacoes_por_horario[link_text]
                    atualizar_link_solicitacao(item["id"], url)
                    links_encontrados += 1
                    logging.info(f"Link adicionado para solicitação {item['id']} (horário {link_text})")
                    continue
                
                # Se não encontrou correspondência exata, faz a busca por proximidade
                try:
                    link_time = datetime.strptime(link_text, "%d/%m/%Y %H:%M:%S")
                    for item in solicitacoes:
                        if item["horario"] and abs((datetime.strptime(item["horario"], "%d/%m/%Y %H:%M:%S") - link_time).total_seconds()) <= 10:
                            atualizar_link_solicitacao(item["id"], url)
                            links_encontrados += 1
                            logging.info(f"Link adicionado para solicitação {item['id']} (horário {link_text})")
                            break
                except ValueError:
                    logging.warning(f"Formato de data inválido: {link_text}")
                    continue
                    
        except Exception as e:
            logging.debug(f"Erro ao processar linha: {str(e)}")
            continue
    
    tempo_total = time.time() - inicio
    logging.info(f"Processamento concluído: {processadas}/{total_linhas} linhas processadas em {tempo_total:.1f}s")
    logging.info(f"Total de {links_encontrados} links encontrados e atualizados")
    return links_encontrados

def executar_processo_busca_links():
    """Função principal que busca e salva os links no banco de dados"""
    logging.info("Iniciando processo de busca de links de download")
    
    # Carrega as solicitações pendentes de links do banco de dados
    solicitacoes = obter_solicitacoes_solicitadas()
    
    if not solicitacoes:
        logging.info("Não há solicitações aguardando links")
        return
    
    navegador = None
    try:
        # Usa Selenoid em vez de Firefox
        browser_type = os.environ.get("SELENOID_BROWSER", "chrome") 
        navegador = iniciar_navegador_selenoid(browser_type=browser_type)
        
        if navegador and autenticar_sefaz(navegador):
            # Acessa a caixa de mensagens
            acessar_pagina(navegador, os.environ.get('URL_CAIXA_DOWNLOADS'))
            
            # Processa e salva os links
            links_encontrados = processar_links_disponíveis(navegador, solicitacoes)
            
            if links_encontrados > 0:
                logging.info(f"Processo concluído com sucesso: {links_encontrados} links encontrados e salvos")
            else:
                logging.info("Nenhum link correspondente encontrado")
        else:
            logging.error("Falha na autenticação ou inicialização do navegador")
            
    except Exception as e:
        logging.error(f"Erro durante a execução: {str(e)}")
    finally:
        if navegador:
            navegador.quit()
            
        logging.info("Processo finalizado")
    
if __name__ == "__main__":
    logging.info("=" * 80)
    logging.info("INICIANDO PROCESSO DE BUSCA DE LINKS DE DOWNLOAD")
    logging.info("=" * 80)
    executar_processo_busca_links()