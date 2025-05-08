import os, sys, mysql.connector, psycopg2, xml.etree.ElementTree as ET
from loggingConfig import get_logger
from mysql.connector import Error
from datetime import datetime
from dateutil import parser
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv

load_dotenv()
logger = get_logger(__name__)

def minha_funcao():
    logger.info("Mensagem informativa")
    logger.error("Ocorreu um erro")

def obter_diretorio_execucao():
    return os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))

def garantir_diretorios():
    diretorio_downloads = os.environ.get("DIRETORIO_DOWNLOADS")
    os.makedirs(diretorio_downloads, exist_ok=True)
    logger.info(f"Diretório de downloads verificado: {diretorio_downloads}")
    return diretorio_downloads

def conectar_mysql():
    try:
        conexao = mysql.connector.connect(host=os.environ.get("MYSQL_HOST"), user=os.environ.get("MYSQL_USER"),
            password=os.environ.get("MYSQL_PASSWORD"), database=os.environ.get("MYSQL_DATABASE"))
        if conexao.is_connected(): logger.info("Conexão ao MySQL estabelecida com sucesso")
        return conexao
    except Error as erro:
        logger.error(f"Erro ao conectar ao MySQL: {erro}")
        return None

def conectar_postgres():
    try:
        required_vars = ["POSTGRES_HOST", "POSTGRES_USER", "POSTGRES_PASSWORD"]
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        if missing_vars:
            logger.error(f"Variáveis de ambiente faltando: {', '.join(missing_vars)}")
            return None
        conn_params = {"host": os.environ.get("POSTGRES_HOST"), "user": os.environ.get("POSTGRES_USER"),
            "password": os.environ.get("POSTGRES_PASSWORD"), "dbname": "analytics", "port": os.environ.get("POSTGRES_PORT", "5432")}
        for key, value in conn_params.items():
            if value and isinstance(value, bytes):
                try: conn_params[key] = value.decode('utf-8')
                except UnicodeDecodeError:
                    try: conn_params[key] = value.decode('latin-1')
                    except Exception as e:
                        logger.error(f"Erro ao decodificar valor para {key}: {e}")
                        return None
        conexao = psycopg2.connect(**conn_params)
        logger.info("Conexão ao PostgreSQL estabelecida com sucesso")
        return conexao
    except psycopg2.OperationalError as erro: logger.error(f"Erro operacional ao conectar ao PostgreSQL: {erro}")
    except Exception as erro: logger.error(f"Erro inesperado ao conectar ao PostgreSQL: {erro}")
    return None

def obter_credenciais_banco():
    conexao = conectar_mysql()
    if conexao and conexao.is_connected():
        cursor = conexao.cursor()
        cursor.execute("SELECT login_atf, senha_atf FROM transmissoes.configuracoes_analytics")
        resultado = cursor.fetchone()
        cursor.close()
        conexao.close()
        return resultado[0], resultado[1]
    logger.error("Falha ao obter credenciais do banco")
    return None, None

def listar_empresas():
    logger.info("Listando empresas do banco de dados")
    conexao = conectar_mysql()
    if conexao and conexao.is_connected():
        cursor = conexao.cursor()
        cursor.execute(os.environ.get("QUERY_LISTAR_EMPRESAS"))
        resultados = cursor.fetchall()
        colunas = [desc[0] for desc in cursor.description]
        cursor.close()
        conexao.close()
        dados = [dict(zip(colunas, linha)) for linha in resultados]
        logger.info(f"{len(dados)} empresas encontradas")
        return dados
    logger.error("Falha ao conectar ao banco de dados")
    return []

def extrair_dado_xml(xml_path, tipo):
    logger.info(f"Utils - Extraindo dado do XML: {xml_path}, Tipo: {tipo}")
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        namespace = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        if tipo == "ie":
            ie_empresa = root.find('.//nfe:emit/nfe:IE', namespace)
            return ie_empresa.text if ie_empresa is not None else None
        elif tipo == "data":
            dh_emi = root.find('.//nfe:ide/nfe:dhEmi', namespace)
            if dh_emi is not None: return parser.isoparse(dh_emi.text)
            else:
                dh_emi = root.find('.//nfe:ide/nfe:dEmi', namespace)
                return parser.parse(dh_emi.text) if dh_emi is not None else None
        else:
            logger.error(f"Utils - Tipo inválido: {tipo}. Use 'ie' ou 'data'.")
            return None
    except Exception as e: logger.error(f"Utils - Erro ao processar o XML {xml_path}: {e}")
    return None

def iniciar_navegador_selenoid(download_dir=None):
    logger.info("Iniciando navegador Chrome com Selenoid")
    options = ChromeOptions()
    container_download_path = "/home/selenium/Downloads"
    capabilities = {"browserName": "chrome", "browserVersion": os.environ.get("SELENOID_VERSION", "latest"),
        "selenoid:options": {"enableVNC": True, "enableVideo": False, "sessionTimeout": "3m"}}
    if download_dir:
        if not os.path.isabs(download_dir): download_dir = os.path.abspath(download_dir)
        capabilities["selenoid:options"]["env"] = ["ENABLE_FILE_UPLOAD=true"]
        capabilities["selenoid:options"]["labels"] = {"manual": "true"}
        capabilities["selenoid:options"]["hostEntries"] = []
        capabilities["selenoid:options"]["volumes"] = [f"{download_dir}:{container_download_path}"]
        logger.info(f"Configurando mapeamento de volume: {download_dir} -> {container_download_path}")
        prefs = {"download.default_directory": container_download_path, "download.prompt_for_download": False,
            "download.directory_upgrade": True, "safebrowsing.enabled": False, "plugins.always_open_pdf_externally": True}
        options.add_experimental_option("prefs", prefs)
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    try:
        selenoid_url = os.environ.get("SELENOID_URL", "http://localhost:4444/wd/hub")
        logger.info(f"Conectando ao Selenoid em: {selenoid_url}")
        for key, value in capabilities.items(): options.set_capability(key, value)
        navegador = webdriver.Remote(command_executor=selenoid_url, options=options)
        navegador.set_page_load_timeout(60)
        navegador.set_script_timeout(60)
        navegador.get(os.environ.get("URL_LOGIN"))
        return navegador
    except Exception as e: logger.error(f"Erro ao iniciar o navegador com Selenoid: {str(e)}")
    return None

def autenticar_sefaz(navegador, espera=2):
    logger.info("Autenticando no SEFAZ")
    usuario, senha = obter_credenciais_banco()
    if not usuario or not senha:
        logger.error("Credenciais não encontradas")
        return False
    try:
        wait = WebDriverWait(navegador, espera)
        campo_login = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get("XPATH_CAMPO_LOGIN"))))
        campo_senha = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get("XPATH_CAMPO_SENHA"))))
        botao_avancar = wait.until(EC.element_to_be_clickable((By.XPATH, os.environ.get("XPATH_BOTAO_AVANCAR"))))
        campo_login.send_keys(usuario)
        campo_senha.send_keys(senha)
        botao_avancar.click()
        logger.info("Autenticação bem-sucedida")
        return True
    except Exception as e: logger.error(f"Erro ao realizar login: {e}")
    return False

def acessar_pagina(navegador, link):
    logger.info(f"Acessando página: {link}")
    navegador.get(link)
    return navegador

def verificar_downloads_em_progresso(diretorio_download):
    try:
        if os.path.exists(diretorio_download):
            downloads_em_progresso = any(arquivo.endswith('.part') or arquivo.endswith('.crdownload') for arquivo in os.listdir(diretorio_download))
            if downloads_em_progresso: logger.info(f"Existem downloads em andamento no diretório {diretorio_download}")
            else: logger.info(f"Nenhum download em andamento detectado no diretório {diretorio_download}")
            return downloads_em_progresso
        else:
            logger.warning(f"Diretório de download não existe: {diretorio_download}")
            return False
    except Exception as e: logger.error(f"Erro ao verificar downloads em progresso: {str(e)}")
    return False

def espera_para_clicar():
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
    try:
        elemento = WebDriverWait(navegador, espera).until(EC.visibility_of_element_located((By.XPATH, xpath)))
        elemento.click()
        return True
    except Exception as e: logger.error(f"Erro ao clicar no elemento com XPath {xpath}: {str(e)}")
    return False