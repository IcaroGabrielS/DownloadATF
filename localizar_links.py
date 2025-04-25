import re
import logging
import os
from datetime import datetime
from selenium.webdriver.common.by import By
from dotenv import load_dotenv
from utils import (
    conectar_postgres,
    iniciar_navegador_firefox,
    autenticar_sefaz,
    acessar_pagina
)

# Load environment variables
load_dotenv()

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    linhas = navegador.find_elements(By.XPATH, "//table/tbody/tr")
    links_encontrados = 0
    
    for linha in linhas:
        try:
            # Verifica se é um anexo (imagem na coluna 3)
            imagem = linha.find_element(By.XPATH, "./td[3]/a/img")
            if imagem.get_attribute("alt") != "Anexo":
                continue
                
            # Verifica se o texto na coluna 4 começa com "FIS_1484"
            coluna4 = linha.find_element(By.XPATH, "./td[4]/a")
            if not coluna4.text.strip().startswith("FIS_1484"):
                continue
                
            # Obtém o link na coluna 6
            link = linha.find_element(By.XPATH, "./td[6]/a")
            href = link.get_attribute("href")
            link_text = link.text.strip()
            
            # Processa o link JavaScript
            match = re.match(r"javascript:abrirFilhas\('(\d+)',(\d+)\)", href)
            if match:
                mensagem_id = match.group(1)
                url = f"https://www4.sefaz.pb.gov.br/atf/seg/SEGf_MinhasMensagens.do?hidsqMensagem={mensagem_id}"
                
                # Faz a correspondência por horário
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
            logging.debug(f"Linha ignorada: {str(e)}")
            continue
    
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
        navegador = iniciar_navegador_firefox()
        
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