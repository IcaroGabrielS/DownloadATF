import time
import logging
import os
import sys
import mysql.connector
import psycopg2
from selenium import webdriver
from datetime import datetime
from mysql.connector import Error
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
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
DIRETORIO_DOWNLOADS = os.path.join(DIRETORIO_EXECUCAO, "NFCE_XML_TEMP")

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
    },
    "DIR": {
        "DIRETORIO_DOWNLOADS": DIRETORIO_DOWNLOADS,
        "DIRETORIO_FINAL": "Z:\\Fiscal\\18. Agendador de Downloads\\NFCE"
    }
}

# Constantes para download
DOWNLOAD = {
    "XPATHS": {
        "IMAGEM_ANEXO": "//img[@alt='Anexo']",
        "LINK_DOWNLOAD": "//a[@href='javascript:mostrarArquivo(0)']"
    },
    "ESPERAS": {
        "CURTA": 2,
        "LONGA": 20
    }
}

# ============================================
# FUNÇÕES AUXILIARES
# ============================================

def garantir_diretorios():
    """Garante que os diretórios necessários existam"""
    os.makedirs(DIRETORIO_DOWNLOADS, exist_ok=True)
    logging.info(f"Diretório de downloads verificado: {DIRETORIO_DOWNLOADS}")

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

def obter_solicitacoes_com_link():
    """Busca solicitações que têm link mas ainda não foram baixadas"""
    conexao = conectar_postgres()
    if not conexao:
        return []
    
    try:
        cursor = conexao.cursor()
        # Buscar solicitações com link disponível mas não baixadas
        cursor.execute("""
            SELECT id, inscricao_estadual, link
            FROM solicitacoes 
            WHERE link IS NOT NULL AND link != '' AND baixado = FALSE AND tipo = 'NFCE'
            ORDER BY data_solicitacao
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
        
        logging.info(f"Encontradas {len(solicitacoes)} solicitações com link pendentes de download")
        return solicitacoes
    
    except psycopg2.Error as erro:
        logging.error(f"Erro ao buscar solicitações: {erro}")
        if conexao:
            conexao.close()
        return []

def marcar_como_baixado(id_solicitacao):
    """Marca a solicitação como baixada no banco de dados"""
    conexao = conectar_postgres()
    if not conexao:
        logging.error(f"Falha ao conectar ao PostgreSQL para marcar solicitação {id_solicitacao} como baixada")
        return False
    
    try:
        cursor = conexao.cursor()
        cursor.execute("""
            UPDATE solicitacoes 
            SET baixado = TRUE, atualizado_em = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (id_solicitacao,))
        
        conexao.commit()
        cursor.close()
        conexao.close()
        
        logging.info(f"Solicitação {id_solicitacao} marcada como baixada")
        return True
    
    except psycopg2.Error as erro:
        logging.error(f"Erro ao marcar solicitação {id_solicitacao} como baixada: {erro}")
        if conexao:
            conexao.rollback()
            conexao.close()
        return False

# ============================================
# FUNÇÕES DE NAVEGADOR E CONEXÃO
# ============================================

def iniciar_navegador_firefox():
    """Inicia o navegador Firefox com as configurações adequadas para download"""
    logging.info("Iniciando navegador Firefox para downloads")
    
    # Configurar opções do Firefox
    options = Options()
    
    # Definir diretório de download
    download_dir = UTIL["DIR"]["DIRETORIO_DOWNLOADS"]
    logging.info(f"Configurando downloads para o diretório: {download_dir}")
    
    options.set_preference("browser.download.dir", download_dir)
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.useDownloadDir", True)
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", 
                          "application/pdf, application/x-pdf, application/octet-stream, application/xml")
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference("browser.download.manager.focusWhenStarting", False)
    options.set_preference("browser.download.manager.closeWhenDone", True)

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

def clicar_elemento(navegador, xpath):
    """Clica em um elemento na página pelo seu XPath"""
    try:
        elemento = WebDriverWait(navegador, DOWNLOAD["ESPERAS"]["CURTA"]).until(
            EC.visibility_of_element_located((By.XPATH, xpath))
        )
        elemento.click()
        return True
    except TimeoutException:
        logging.warning(f"Tempo esgotado ao esperar pelo elemento com XPath: {xpath}")
        return False
    except Exception as e:
        logging.error(f"Erro ao clicar no elemento com XPath {xpath}: {str(e)}")
        return False

# ============================================
# FUNÇÕES DE PROCESSAMENTO DE DOWNLOADS
# ============================================

def verificar_downloads_em_progresso(diretorio_download):
    """Verifica se existem downloads em andamento"""
    try:
        if os.path.exists(diretorio_download):
            downloads_em_progresso = any(arquivo.endswith('.part') for arquivo in os.listdir(diretorio_download))
            if downloads_em_progresso:
                logging.info(f"Existem downloads em andamento no diretório {diretorio_download}")
            else:
                logging.info(f"Nenhum download em andamento detectado no diretório {diretorio_download}")
            return downloads_em_progresso
        else:
            logging.warning(f"Diretório de download não existe: {diretorio_download}")
            return False
    except Exception as e:
        logging.error(f"Erro ao verificar downloads em progresso: {str(e)}")
        return False

def realizar_download(navegador, solicitacao):
    """Acessa o link e inicia o download do arquivo"""
    try:
        # Acessa o link para download
        acessar_pagina(navegador, solicitacao["link"])
        
        # Clica nos elementos de download
        if clicar_elemento(navegador, DOWNLOAD["XPATHS"]["IMAGEM_ANEXO"]) and clicar_elemento(navegador, DOWNLOAD["XPATHS"]["LINK_DOWNLOAD"]):
            logging.info(f"Download iniciado para solicitação {solicitacao['id']}")
            # Aguarda um pouco para o download iniciar
            time.sleep(5)
            return True
        else:
            logging.error(f"Falha ao clicar nos elementos para download da solicitação {solicitacao['id']}")
            return False
    except Exception as e:
        logging.error(f"Erro ao realizar download para solicitação {solicitacao['id']}: {str(e)}")
        return False

# ============================================
# FUNÇÃO PRINCIPAL
# ============================================

def executar_processo_downloads():
    """Função principal que executa o processo de download dos arquivos"""
    logging.info("Iniciando processo de download dos arquivos")
    
    # Garante que os diretórios existam
    garantir_diretorios()
    
    # Carrega as solicitações pendentes de download do banco de dados
    solicitacoes = obter_solicitacoes_com_link()
    
    if not solicitacoes:
        logging.info("Não há solicitações pendentes de download")
        return
    
    navegador = None
    try:
        navegador = iniciar_navegador_firefox()
        
        if navegador and autenticar_sefaz(navegador):
            downloads_realizados = 0
            
            # Processa cada solicitação com link disponível
            for solicitacao in solicitacoes:
                if realizar_download(navegador, solicitacao):
                    # Se o download foi iniciado com sucesso, marca como baixado no banco
                    if marcar_como_baixado(solicitacao["id"]):
                        downloads_realizados += 1
                        logging.info(f"Solicitação {solicitacao['id']} marcada como baixada no banco")
                    else:
                        logging.error(f"Falha ao atualizar status no banco para solicitação {solicitacao['id']}")
                
                # Espera um pouco entre os downloads para evitar sobrecarga
                time.sleep(10)
            
            logging.info(f"Processo de downloads concluído. {downloads_realizados} arquivos baixados.")
        else:
            logging.error("Falha na autenticação ou inicialização do navegador")
            
    except Exception as e:
        logging.error(f"Erro durante a execução: {str(e)}")
    finally:
        diretorio_temp = UTIL["DIR"]["DIRETORIO_DOWNLOADS"]
        tentativa, max_tentativas = 0, 10
        
        # Aguarda downloads em andamento concluírem
        while verificar_downloads_em_progresso(diretorio_temp) and tentativa < max_tentativas:
            time.sleep(2)
            tentativa += 1
            
        if tentativa >= max_tentativas:
            logging.warning("Tempo máximo de espera atingido, alguns downloads podem não ter sido concluídos")
            
        if navegador:
            navegador.quit()
            
        logging.info("Processo de download finalizado")
    
if __name__ == "__main__":
    logging.info("=" * 80)
    logging.info("INICIANDO PROCESSO DE DOWNLOAD DE ARQUIVOS XML")
    logging.info("=" * 80)
    executar_processo_downloads()