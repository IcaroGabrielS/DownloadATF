import re
import logging
import os
import sys
import mysql.connector
import psycopg2
from selenium import webdriver
from datetime import datetime
from mysql.connector import Error
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ============================================
# FUNÇÕES DE DIRETÓRIO
# ============================================

def obter_diretorio_execucao():
    """Retorna o diretório onde o programa está sendo executado"""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))

# ============================================
# CONSTANTES
# ============================================

# Diretórios importantes
DIRETORIO_EXECUCAO = obter_diretorio_execucao()

# Constantes de utilidade
UTIL = {
    "MYSQL": {
        "HOST": "10.0.100.37",
        "USER": "externo",
        "PASSWORD": "externo",
        "DATABASE": "transmissoes"
    },
    "POSTGRES": {
        "HOST": "localhost",
        "USER": "postgres",
        "PASSWORD": "root",
        "DATABASE": "xmlsnfceatf",
        "PORT": "5432"
    },
    "URLS": {
        "LOGIN": "https://www4.sefaz.pb.gov.br/atf/seg/SEGf_Login.jsp"
    },
    "XPATHS": {
        "LOGIN": {
            "CAMPO_LOGIN": "//*[@id='login']",
            "CAMPO_SENHA": "/html/body/table/tbody/tr[2]/td/table[1]/tbody/tr[4]/td[2]/input",
            "BOTAO_AVANCAR": "/html/body/table/tbody/tr[2]/td/table[1]/tbody/tr[5]/td[2]/input[2]"
        }
    }
}

# Constantes para download
DOWNLOAD = {
    "URL_CAIXA_DOWNLOADS": "https://www4.sefaz.pb.gov.br/atf/seg/SEGf_MinhasMensagens.do?limparSessao=true",
    "ESPERAS": {
        "CURTA": 2,
        "LONGA": 20
    }
}

# ============================================
# FUNÇÕES DE BANCO DE DADOS
# ============================================

def conectar_mysql():
    """Estabelece conexão com o banco de dados MySQL"""
    try:
        conexao = mysql.connector.connect(
            host=UTIL["MYSQL"]["HOST"],
            user=UTIL["MYSQL"]["USER"],
            password=UTIL["MYSQL"]["PASSWORD"],
            database=UTIL["MYSQL"]["DATABASE"]
        )
        if conexao.is_connected():
            logging.info("Conexão ao MySQL estabelecida com sucesso")
        return conexao
    except Error as erro:
        logging.error(f"Erro na conexão com o MySQL: {str(erro)}")
        return None

def conectar_postgres():
    """Estabelece conexão com o banco de dados PostgreSQL"""
    try:
        conexao = psycopg2.connect(
            host=UTIL["POSTGRES"]["HOST"],
            user=UTIL["POSTGRES"]["USER"],
            password=UTIL["POSTGRES"]["PASSWORD"],
            dbname=UTIL["POSTGRES"]["DATABASE"],
            port=UTIL["POSTGRES"]["PORT"]
        )
        logging.info("Conexão ao PostgreSQL estabelecida com sucesso")
        return conexao
    except psycopg2.Error as erro:
        logging.error(f"Erro na conexão com o PostgreSQL: {str(erro)}")
        return None

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
    
    except psycopg2.Error as erro:
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
    
    except psycopg2.Error as erro:
        logging.error(f"Erro ao atualizar link da solicitação {id_solicitacao}: {erro}")
        if conexao:
            conexao.rollback()
            conexao.close()
        return False

# ============================================
# FUNÇÕES DE NAVEGADOR E CONEXÃO
# ============================================

def iniciar_navegador_firefox():
    """Inicia o navegador Firefox"""
    logging.info("Iniciando navegador Firefox")
    
    options = Options()
    
    try:
        navegador = webdriver.Firefox(options=options)
        navegador.get(UTIL["URLS"]["LOGIN"])
        return navegador
    except Exception as e:
        logging.error(f"Erro ao iniciar o navegador: {str(e)}")
        return None

def obter_credenciais_banco():
    """Obtém credenciais de login do banco de dados"""
    conexao = conectar_mysql()
    if conexao and conexao.is_connected():
        cursor = conexao.cursor()
        cursor.execute("SELECT login_atf, senha_atf FROM transmissoes.configuracoes_analytics")
        resultado = cursor.fetchone()
        cursor.close()
        conexao.close()
        return resultado[0], resultado[1]
    return None, None

def autenticar_sefaz(navegador, espera=2):
    """Autentica no sistema da SEFAZ"""
    usuario, senha = obter_credenciais_banco()
    if not usuario or not senha:
        logging.error("Falha ao obter credenciais do banco de dados")
        return False
    
    try:
        wait = WebDriverWait(navegador, espera)
        campo_login = wait.until(EC.presence_of_element_located((By.XPATH, UTIL["XPATHS"]["LOGIN"]["CAMPO_LOGIN"])))
        campo_senha = wait.until(EC.presence_of_element_located((By.XPATH, UTIL["XPATHS"]["LOGIN"]["CAMPO_SENHA"])))
        botao_avancar = wait.until(EC.element_to_be_clickable((By.XPATH, UTIL["XPATHS"]["LOGIN"]["BOTAO_AVANCAR"])))
        
        campo_login.send_keys(usuario)
        campo_senha.send_keys(senha)
        botao_avancar.click()
        logging.info("Autenticação realizada com sucesso")
        return True
    except Exception as e:
        logging.error(f"Erro durante autenticação: {str(e)}")
        return False

def acessar_pagina(navegador, link):
    """Acessa uma página específica no navegador"""
    logging.info(f"Acessando página: {link}")
    navegador.get(link)
    return navegador

# ============================================
# FUNÇÕES DE PROCESSAMENTO DE LINKS
# ============================================

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

# ============================================
# FUNÇÃO PRINCIPAL
# ============================================

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
            acessar_pagina(navegador, DOWNLOAD["URL_CAIXA_DOWNLOADS"])
            
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