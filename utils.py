import logging, os, sys, mysql.connector, psycopg2, xml.etree.ElementTree as ET
from mysql.connector import Error
from datetime import datetime
from dateutil import parser
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv

load_dotenv()
os.makedirs("logs", exist_ok=True)

from logging.handlers import RotatingFileHandler
MAX_LOG_SIZE = 220 * 1024 * 1024  # 220 MB
logging.getLogger().setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_file = "/home/desenvolvimento/DownloadATF/logs/utils.log"
file_handler = RotatingFileHandler(log_file, maxBytes=MAX_LOG_SIZE, backupCount=5, encoding='utf-8')
file_handler.setFormatter(log_formatter)
logging.getLogger().addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logging.getLogger().addHandler(console_handler)

# ============================================
# DIRECTORY FUNCTIONS
# ============================================

def obter_diretorio_execucao():
    """Retorna o diretório onde o programa está sendo executado"""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))

def garantir_diretorios():
    """Garante que os diretórios necessários existam"""
    diretorio_downloads = os.path.join(obter_diretorio_execucao(), os.environ.get("DIRETORIO_DOWNLOADS"))
    os.makedirs(diretorio_downloads, exist_ok=True)
    logging.info(f"Diretório de downloads verificado: {diretorio_downloads}")
    return diretorio_downloads

# ============================================
# DATABASE FUNCTIONS
# ============================================

def conectar_mysql():
    """Estabelece conexão com o banco de dados MySQL."""
    try:
        conexao = mysql.connector.connect(
            host=os.environ.get("MYSQL_HOST"),
            user=os.environ.get("MYSQL_USER"),
            password=os.environ.get("MYSQL_PASSWORD"),
            database=os.environ.get("MYSQL_DATABASE")
        )
        if conexao.is_connected():
            logging.info("Conexão ao MySQL estabelecida com sucesso")
        return conexao
    except Error as erro:
        logging.error(f"Erro ao conectar ao MySQL: {erro}")
        return None

def conectar_postgres():
    """Estabelece conexão com o banco de dados PostgreSQL."""
    try:
        conexao = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST"),
            user=os.environ.get("POSTGRES_USER"),
            password=os.environ.get("POSTGRES_PASSWORD"),
            dbname=os.environ.get("POSTGRES_DATABASE"),
            port=os.environ.get("POSTGRES_PORT")
        )
        logging.info("Conexão ao PostgreSQL estabelecida com sucesso")
        return conexao
    except psycopg2.Error as erro:
        logging.error(f"Erro ao conectar ao PostgreSQL: {erro}")
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
    logging.error("Falha ao obter credenciais do banco")
    return None, None

def listar_empresas():
    """Lista empresas do banco de dados MySQL"""
    logging.info("Listando empresas do banco de dados")
    conexao = conectar_mysql()
    if conexao and conexao.is_connected():
        cursor = conexao.cursor()
        cursor.execute(os.environ.get("QUERY_LISTAR_EMPRESAS"))
        resultados = cursor.fetchall()
        colunas = [desc[0] for desc in cursor.description]
        cursor.close()
        conexao.close()
        dados = [dict(zip(colunas, linha)) for linha in resultados]
        logging.info(f"{len(dados)} empresas encontradas")
        return dados
    logging.error("Falha ao conectar ao banco de dados")
    return []

# ============================================
# XML FUNCTIONS
# ============================================

def extrair_dado_xml(xml_path, tipo):
    """
    Extrai a Inscrição Estadual (IE) ou a Data de Emissão de um XML.

    Parâmetros:
        xml_path (str): Caminho do arquivo XML.
        tipo (str): Tipo de dado a ser extraído. Pode ser "ie" ou "data".

    Retorna:
        str: Inscrição Estadual (IE) ou Data de Emissão no formato datetime.
        None: Caso ocorra um erro ou o dado não seja encontrado.
    """
    logging.info(f"Utils - Extraindo dado do XML: {xml_path}, Tipo: {tipo}")
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        namespace = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        if tipo == "ie":
            ie_empresa = root.find('.//nfe:emit/nfe:IE', namespace)
            return ie_empresa.text if ie_empresa is not None else None
        elif tipo == "data":
            dh_emi = root.find('.//nfe:ide/nfe:dhEmi', namespace)
            if dh_emi is not None:
                return parser.isoparse(dh_emi.text)
            else:
                dh_emi = root.find('.//nfe:ide/nfe:dEmi', namespace)
                if dh_emi is not None:
                    return parser.parse(dh_emi.text)
                else:
                    return None
        else:
            logging.error(f"Utils - Tipo inválido: {tipo}. Use 'ie' ou 'data'.")
            return None
    except Exception as e:
        logging.error(f"Utils - Erro ao processar o XML {xml_path}: {e}")
        return None

# ============================================
# BROWSER FUNCTIONS
# ============================================

def iniciar_navegador_selenoid(download_dir=None, browser_type="chrome"):
    """Inicia um navegador usando Selenoid com as configurações adequadas"""
    logging.info(f"Iniciando navegador {browser_type} com Selenoid")
    
    # Seleciona o tipo de opções com base no navegador
    if browser_type.lower() == "firefox":
        options = FirefoxOptions()
    else:  # chrome é o padrão
        options = ChromeOptions()
        
    # Adicionar capacidades específicas do Selenoid
    capabilities = {
        "browserName": browser_type.lower(),
        "browserVersion": os.environ.get("SELENOID_VERSION", "latest"),
        "selenoid:options": {
            "enableVNC": True,
            "enableVideo": False,
            "sessionTimeout": "3m"
        }
    }
    
    # Adicionar configurações de download se especificado
    if download_dir:
        if browser_type.lower() == "firefox":
            # Configurações para Firefox
            options.set_preference("browser.download.dir", download_dir)
            options.set_preference("browser.download.folderList", 2)
            options.set_preference("browser.download.useDownloadDir", True)
            options.set_preference("browser.helperApps.neverAsk.saveToDisk", 
                                 "application/pdf,application/x-pdf,application/octet-stream,application/xml")
            options.set_preference("browser.download.manager.showWhenStarting", False)
            options.set_preference("browser.download.manager.focusWhenStarting", False)
            options.set_preference("browser.download.manager.closeWhenDone", True)
        else:
            # Configurações para Chrome
            prefs = {
                "download.default_directory": download_dir,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": False
            }
            options.add_experimental_option("prefs", prefs)
    
    # Adicionar argumentos extras
    if browser_type.lower() == "chrome":
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
    
    try:
        # Conectar ao Selenoid
        selenoid_url = os.environ.get("SELENOID_URL", "http://localhost:4444/wd/hub")
        logging.info(f"Conectando ao Selenoid em: {selenoid_url}")
        
        # Combinar opções e capacidades
        for key, value in capabilities.items():
            options.set_capability(key, value)
        
        navegador = webdriver.Remote(
            command_executor=selenoid_url,
            options=options
        )
        
        # Configurar timeouts
        navegador.set_page_load_timeout(60)
        navegador.set_script_timeout(60)
        
        # Acessar URL de login
        navegador.get(os.environ.get("URL_LOGIN"))
        return navegador
    except Exception as e:
        logging.error(f"Erro ao iniciar o navegador com Selenoid: {str(e)}")
        return None

def autenticar_sefaz(navegador, espera=2):
    """Autentica no sistema da SEFAZ"""
    logging.info("Autenticando no SEFAZ")
    usuario, senha = obter_credenciais_banco()
    if not usuario or not senha:
        logging.error("Credenciais não encontradas")
        return False
    
    try:
        wait = WebDriverWait(navegador, espera)
        campo_login = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get("XPATH_CAMPO_LOGIN"))))
        campo_senha = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get("XPATH_CAMPO_SENHA"))))
        botao_avancar = wait.until(EC.element_to_be_clickable((By.XPATH, os.environ.get("XPATH_BOTAO_AVANCAR"))))
        
        campo_login.send_keys(usuario)
        campo_senha.send_keys(senha)
        botao_avancar.click()
        logging.info("Autenticação bem-sucedida")
        return True
    except Exception as e:
        logging.error(f"Erro ao realizar login: {e}")
        return False

def acessar_pagina(navegador, link):
    """Acessa uma página específica no navegador"""
    logging.info(f"Acessando página: {link}")
    navegador.get(link)
    return navegador

def verificar_downloads_em_progresso(diretorio_download):
    """Verifica se existem downloads em andamento"""
    try:
        if os.path.exists(diretorio_download):
            downloads_em_progresso = any(arquivo.endswith('.part') or arquivo.endswith('.crdownload') 
                                       for arquivo in os.listdir(diretorio_download))
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

def espera_para_clicar():
    """Espera até o próximo segundo específico (1 ou 31) para evitar problemas de sincronização"""
    # Função específica para o ATF
    import time
    now = time.localtime()
    segundo = now.tm_sec
    if segundo < 1: tempo_espera = 1
    elif segundo < 31: tempo_espera = 31
    else:
        time.sleep(60 - segundo)
        now = time.localtime()
        segundo = now.tm_sec
        tempo_espera = 1
    time.sleep(max(0, tempo_espera - segundo))

def clicar_elemento(navegador, xpath, espera=2):
    """Clica em um elemento na página pelo seu XPath"""
    try:
        elemento = WebDriverWait(navegador, espera).until(
            EC.visibility_of_element_located((By.XPATH, xpath))
        )
        elemento.click()
        return True
    except Exception as e:
        logging.error(f"Erro ao clicar no elemento com XPath {xpath}: {str(e)}")
        return False