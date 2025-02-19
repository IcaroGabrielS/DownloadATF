import os, time, json, logging, mysql.connector
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
from mysql.connector import Error
from selenium import webdriver

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

with open("json_files/constantes.json", "r", encoding="utf-8") as arquivo_constantes:
    constantes = json.load(arquivo_constantes)
    DOWNLOAD = constantes["DOWNLOAD"]
    NFCE = constantes["NFCE"]
    UTIL = constantes["UTIL"]
    
def obter_periodo_datas():
    hoje = datetime.now()
    cinco_dias_atras = hoje - timedelta(days=5)
    ontem = hoje - timedelta(days=1)
    return cinco_dias_atras.strftime('%d/%m/%Y'), ontem.strftime('%d/%m/%Y')

def conectar_banco():
    try:
        conexao = mysql.connector.connect(
            host=UTIL["DATABASE"]["HOST"],
            user=UTIL["DATABASE"]["USER"],
            password=UTIL["DATABASE"]["PASSWORD"],
            database=UTIL["DATABASE"]["DATABASE"]
        )
        return conexao
    except Error as erro:
        logging.error(f"Erro ao conectar ao MySQL: {erro}")
        return None

def obter_credenciais_banco():
    conexao = conectar_banco()
    if conexao and conexao.is_connected():
        cursor = conexao.cursor()
        cursor.execute(UTIL["QUERIES"]["OBTER_CREDENCIAIS"])
        resultado = cursor.fetchone()
        cursor.close()
        conexao.close()
        return resultado[0], resultado[1]
    return None, None

def obter_empresas_banco():
    conexao = conectar_banco()
    if conexao and conexao.is_connected():
        cursor = conexao.cursor()
        cursor.execute(UTIL["QUERIES"]["LISTAR_EMPRESAS"])
        resultados = cursor.fetchall()
        colunas = [desc[0] for desc in cursor.description]
        cursor.close()
        conexao.close()
        return [dict(zip(colunas, linha)) for linha in resultados]
    return []

def capturar_data_hora():
    horario_atual = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    return horario_atual

def espera_para_clicar():
    now = time.localtime()
    segundo = now.tm_sec
    if segundo < 1:
        tempo_espera = 1
    elif segundo < 31:
        tempo_espera = 31
    else:
        time.sleep(60 - segundo)
        now = time.localtime()
        segundo = now.tm_sec
        tempo_espera = 1
    time.sleep(max(0, tempo_espera - segundo))

def autenticar_sefaz(navegador, espera=2):
    usuario, senha = obter_credenciais_banco()
    if not usuario or not senha:
        return False
    try:
        wait = WebDriverWait(navegador, espera)
        campo_login = wait.until(EC.presence_of_element_located((By.XPATH, UTIL["XPATHS"]["LOGIN"]["CAMPO_LOGIN"])))
        campo_senha = wait.until(EC.presence_of_element_located((By.XPATH, UTIL["XPATHS"]["LOGIN"]["CAMPO_SENHA"])))
        botao_avancar = wait.until(EC.element_to_be_clickable((By.XPATH, UTIL["XPATHS"]["LOGIN"]["BOTAO_AVANCAR"])))
        campo_login.send_keys(usuario)
        campo_senha.send_keys(senha)
        botao_avancar.click()
        return True
    except Exception as e:
        logging.error(f"Erro ao realizar login: {e}")
        return False

def acessar_pagina(navegador, link):
    navegador.get(link)
    return navegador

def clicar_elemento(navegador, xpath):
    try:
        elemento = WebDriverWait(navegador, DOWNLOAD["ESPERAS"]["CURTA"]).until(EC.visibility_of_element_located((By.XPATH, xpath)))
        elemento.click()
        return True
    except TimeoutException:
        return False

def iniciar_navegador_firefox():
    options = Options()
    options.set_preference("browser.download.dir", r"C:\NFCE_XML_TEMP")
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.useDownloadDir", True)
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/pdf, application/x-pdf, application/octet-stream")

    try:
        navegador = webdriver.Firefox(options=options)
        navegador.get(UTIL["URLS"]["LOGIN"])
        return navegador
    except Exception as e:
        logging.error(f"Erro ao inicializar o navegador: {e}")
        return

def obter_datas_solicitacoes():
    caminho_json = os.path.join(os.path.dirname(__file__), "json_files/solicitacoes.json")
    with open(caminho_json, 'r', encoding='utf-8') as f: dados = json.load(f)
    primeira_solicitacao = dados[0]
    data_ini = primeira_solicitacao.get("data_ini")
    data_fim = primeira_solicitacao.get("data_fim")
    data_ini = datetime.strptime(data_ini, '%d/%m/%Y').strftime('%Y%m%d')
    data_fim = datetime.strptime(data_fim, '%d/%m/%Y').strftime('%Y%m%d')
    return data_ini, data_fim

def montar_lista_solicitacoes(tipo):
    conexao = conectar_banco()
    if conexao and conexao.is_connected():
        cursor = conexao.cursor()
        cursor.execute(UTIL["QUERIES"]["LISTAR_EMPRESAS"])
        resultados = cursor.fetchall()
        colunas = [desc[0] for desc in cursor.description]
        cursor.close()
        conexao.close()
        dados = [dict(zip(colunas, linha)) for linha in resultados]
        data_ini, data_fim = obter_periodo_datas()
        solicitacoes = [
            {
                "inscricao_estadual": item["inscricao_estadual"],
                "data_ini": data_ini,
                "data_fim": data_fim,
                "tipo": tipo,
                "horario": None,
                "link": None,
                "solicitado": False,
                "baixado": False
            }
            for item in dados]
        os.makedirs("json_files", exist_ok=True)
        with open("json_files/solicitacoes.json", "w", encoding="utf-8") as f:
            json.dump(solicitacoes, f, indent=4, ensure_ascii=False)
    return []

def remover_solicitacoes_anteriores():
    caminho_json = os.path.join(os.path.dirname(__file__), 'json_files/solicitacoes.json')
    if os.path.exists(caminho_json): os.remove(caminho_json)