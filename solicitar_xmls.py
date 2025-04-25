import time, logging, os, mysql.connector, psycopg2
from mysql.connector import Error
from psycopg2 import sql
from selenium import webdriver
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define constants
NFCE = {
    "LINK_SEFAZ_NFCE": "https://www4.sefaz.pb.gov.br/atf/fis/FISf_ConsultaGenericaEmitenteNFCe.do?limparSessao=true",
    "XPATH_DATA_INICIO": "/html/body/table/tbody/tr[2]/td/form/table/tbody/tr[2]/td[2]/input[1]",
    "XPATH_DATA_FIM": "/html/body/table/tbody/tr[2]/td/form/table/tbody/tr[2]/td[2]/input[2]",
    "XPATH_IFRAME": "/html/body/table/tbody/tr[2]/td/form/table/tbody/tr[8]/td/table/tbody/tr[2]/td/iframe",
    "XPATH_CAMPO_VALOR": "/html/body/div/table/tbody/tr/td/form/table/tbody/tr[1]/td[2]/input",
    "XPATH_BOTAO_PESQUISAR": "/html/body/div/table/tbody/tr/td/form/table/tbody/tr[1]/td[3]/input",
    "XPATH_DROPDOWN_XML": "/html/body/table/tbody/tr[2]/td/form/table/tbody/tr[11]/td[2]/select[1]",
    "XPATH_OPCAO_XML": "//option[text()=\"XML\"]",
    "XPATH_BOTAO_EXECUTAR": "/html/body/table/tbody/tr[2]/td/form/table/tbody/tr[11]/td[2]/button"
}

# Database and other utilities
CONFIG = {
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
    "MAX_TENTATIVAS": 3  # Número máximo de tentativas para solicitações
}

def iniciar_navegador_firefox():
    options = Options()
    options.set_preference("browser.download.dir", r"C:\NFCE_XML_TEMP")
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.useDownloadDir", True)
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/pdf, application/x-pdf, application/octet-stream")

    try:
        navegador = webdriver.Firefox(options=options)
        navegador.get(CONFIG["URLS"]["LOGIN"])
        return navegador
    except Exception as e:
        logging.error(f"Erro ao inicializar o navegador: {e}")
        return None

def conectar_mysql():
    try:
        conexao = mysql.connector.connect(
            host=CONFIG["MYSQL"]["HOST"],
            user=CONFIG["MYSQL"]["USER"],
            password=CONFIG["MYSQL"]["PASSWORD"],
            database=CONFIG["MYSQL"]["DATABASE"]
        )
        return conexao
    except Error as erro:
        logging.error(f"Erro ao conectar ao MySQL: {erro}")
        return None

def conectar_postgres():
    try:
        conexao = psycopg2.connect(
            host=CONFIG["POSTGRES"]["HOST"],
            user=CONFIG["POSTGRES"]["USER"],
            password=CONFIG["POSTGRES"]["PASSWORD"],
            dbname=CONFIG["POSTGRES"]["DATABASE"],
            port=CONFIG["POSTGRES"]["PORT"]
        )
        return conexao
    except psycopg2.Error as erro:
        logging.error(f"Erro ao conectar ao PostgreSQL: {erro}")
        return None

def obter_credenciais_banco():
    conexao = conectar_mysql()
    if conexao and conexao.is_connected():
        cursor = conexao.cursor()
        cursor.execute("SELECT login_atf, senha_atf FROM transmissoes.configuracoes_analytics")
        resultado = cursor.fetchone()
        cursor.close()
        conexao.close()
        print(resultado[0], resultado[1])
        return resultado[0], resultado[1]
    logging.error("Falha ao obter credenciais do banco")
    return None, None

def autenticar_sefaz(navegador, espera=2):
    logging.info("Autenticando no SEFAZ")
    usuario, senha = obter_credenciais_banco()
    if not usuario or not senha:
        logging.error("Credenciais não encontradas")
        return False
    try:
        wait = WebDriverWait(navegador, espera)
        campo_login = wait.until(EC.presence_of_element_located((By.XPATH, CONFIG["XPATHS"]["LOGIN"]["CAMPO_LOGIN"])))
        campo_senha = wait.until(EC.presence_of_element_located((By.XPATH, CONFIG["XPATHS"]["LOGIN"]["CAMPO_SENHA"])))
        botao_avancar = wait.until(EC.element_to_be_clickable((By.XPATH, CONFIG["XPATHS"]["LOGIN"]["BOTAO_AVANCAR"])))
        
        campo_login.send_keys(usuario)
        campo_senha.send_keys(senha)
        botao_avancar.click()
        logging.info("Autenticação bem-sucedida")
        return True
    except Exception as e:
        logging.error(f"Erro ao realizar login: {e}")
        return False

def acessar_pagina(navegador, link):
    navegador.get(link)
    return navegador

def obter_solicitacoes_pendentes():
    """Busca solicitações pendentes no banco de dados PostgreSQL"""
    solicitacoes_pendentes = []
    max_tentativas = CONFIG["MAX_TENTATIVAS"]
    
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
        """)
        
        for id, inscricao_estadual, data_ini, data_fim in cursor.fetchall():
            solicitacoes_pendentes.append({
                "id": id,
                "inscricao_estadual": inscricao_estadual,
                "data_ini": data_ini,
                "data_fim": data_fim
            })
        
        cursor.close()
        conexao.close()
        
        logging.info(f"Encontradas {len(solicitacoes_pendentes)} solicitações pendentes")
        return solicitacoes_pendentes
        
    except psycopg2.Error as erro:
        logging.error(f"Erro ao obter solicitações pendentes: {erro}")
        if conexao:
            conexao.close()
        return []

def atualizar_solicitacao(id_solicitacao, horario):
    """Atualiza a solicitação no banco de dados, incrementando o contador"""
    conexao = conectar_postgres()
    if not conexao:
        logging.error(f"Falha ao conectar ao PostgreSQL para atualizar solicitação {id_solicitacao}")
        return False
    
    try:
        cursor = conexao.cursor()
        
        cursor.execute("""
            UPDATE solicitacoes 
            SET solicitado = solicitado + 1, horario = %s, atualizado_em = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (horario, id_solicitacao))
        
        conexao.commit()
        cursor.close()
        conexao.close()
        
        logging.info(f"Solicitação {id_solicitacao} atualizada com sucesso")
        return True
        
    except psycopg2.Error as erro:
        logging.error(f"Erro ao atualizar solicitação: {erro}")
        if conexao:
            conexao.rollback()
            conexao.close()
        return False

def inserir_datas_formulario(navegador, data_inicio, data_fim, espera=2):
    wait = WebDriverWait(navegador, espera)
    data_inicio_elemento = wait.until(EC.presence_of_element_located((By.XPATH, NFCE["XPATH_DATA_INICIO"])))
    data_inicio_elemento.clear()
    data_inicio_elemento.send_keys(data_inicio)
    data_fim_elemento = wait.until(EC.presence_of_element_located((By.XPATH, NFCE["XPATH_DATA_FIM"])))
    data_fim_elemento.clear()
    data_fim_elemento.send_keys(data_fim)

def preencher_campo_iframe(navegador, ie_empresa, espera=2):
    wait = WebDriverWait(navegador, espera)
    wait.until(EC.frame_to_be_available_and_switch_to_it((By.XPATH, NFCE["XPATH_IFRAME"])))
    campo_valor = wait.until(EC.presence_of_element_located((By.XPATH, NFCE["XPATH_CAMPO_VALOR"])))
    campo_valor.clear()
    campo_valor.send_keys(ie_empresa)
    botao_pesquisar = wait.until(EC.element_to_be_clickable((By.XPATH, NFCE["XPATH_BOTAO_PESQUISAR"])))
    botao_pesquisar.click()
    navegador.switch_to.default_content()

def espera_para_clicar():
    # Espera até o próximo segundo específico (1 ou 31) para evitar problemas de sincronização
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

def selecionar_xml_executar(navegador, espera=2):
    wait = WebDriverWait(navegador, espera)
    dropdown_xml = wait.until(EC.presence_of_element_located((By.XPATH, NFCE["XPATH_DROPDOWN_XML"])))
    dropdown_xml.click()
    opcao_xml = wait.until(EC.presence_of_element_located((By.XPATH, NFCE["XPATH_OPCAO_XML"])))
    opcao_xml.click()
    botao_executar = wait.until(EC.element_to_be_clickable((By.XPATH, NFCE["XPATH_BOTAO_EXECUTAR"])))
    espera_para_clicar()  # Função importante mantida conforme observação
    botao_executar.click()

def solicitar_nfce(navegador, solicitacao, espera=2):
    try:
        logging.info(f"Solicitando NFC-e para IE: {solicitacao['inscricao_estadual']}")
        inserir_datas_formulario(navegador, solicitacao["data_ini"], solicitacao["data_fim"], espera)
        preencher_campo_iframe(navegador, solicitacao["inscricao_estadual"], espera)
        selecionar_xml_executar(navegador, espera)
        
        # Capturar data/hora atual
        horario = datetime.now()
        
        # Atualizar banco de dados
        sucesso = atualizar_solicitacao(solicitacao["id"], horario)
        
        # Esperar download iniciar
        time.sleep(10)
        return sucesso
    except Exception as e:
        logging.error(f"Erro ao solicitar NFCE para IE {solicitacao['inscricao_estadual']}: {e}")
        return False

def executar_processo_requests_nfce():
    """Executa o processo de solicitação de XMLs para todas as solicitações pendentes"""
    solicitacoes = obter_solicitacoes_pendentes()
    
    if not solicitacoes:
        logging.info("Não há solicitações pendentes para processar")
        return
    
    logging.info(f"Iniciando solicitações de NFC-e para {len(solicitacoes)} registros...")
    navegador = None
    
    try:
        navegador = iniciar_navegador_firefox()
        if navegador and autenticar_sefaz(navegador):
            link = NFCE["LINK_SEFAZ_NFCE"]
            acessar_pagina(navegador, link)
            
            for solicitacao in solicitacoes:
                if solicitar_nfce(navegador, solicitacao):
                    logging.info(f"Solicitação {solicitacao['id']} processada com sucesso")
                else:
                    logging.warning(f"Falha ao processar solicitação {solicitacao['id']}")
                
                # Recarregar página para próxima solicitação
                acessar_pagina(navegador, link)
            
            logging.info("Processo concluído com sucesso")
        else:
            logging.error("Falha na autenticação ou inicialização do navegador")
    except Exception as e: 
        logging.error(f"Erro durante a execução: {e}")
    finally:
        if navegador:
            navegador.quit()

if __name__ == "__main__":
    logging.info("=" * 80)
    logging.info("INICIANDO PROCESSO DE SOLICITAÇÃO DE ARQUIVOS XML")
    logging.info("=" * 80)
    executar_processo_requests_nfce()