import time, json, logging, os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from util import (
    listar_empresas, salvar_horario_json, verificar_registro_json,
    iniciar_navegador, aguardar_tempo_para_clicar, realizar_login,
    acessar_link, definir_datas_por_tipo
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("schedule.log"),  # Salva logs em um arquivo
        logging.StreamHandler()  # Exibe logs no console
    ]
)
logger = logging.getLogger(__name__)

with open("json_files/constantes.json", "r", encoding="utf-8") as arquivo_constantes:
    constantes = json.load(arquivo_constantes)
    NFCE = constantes["NFCE"]

def preencher_datas(navegador, data_inicio, data_fim, espera=2):
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

def selecionar_xml_e_executar(navegador, espera=2):
    wait = WebDriverWait(navegador, espera)
    dropdown_xml = wait.until(EC.presence_of_element_located((By.XPATH, NFCE["XPATH_DROPDOWN_XML"])))
    dropdown_xml.click()
    opcao_xml = wait.until(EC.presence_of_element_located((By.XPATH, NFCE["XPATH_OPCAO_XML"])))
    opcao_xml.click()
    botao_executar = wait.until(EC.element_to_be_clickable((By.XPATH, NFCE["XPATH_BOTAO_EXECUTAR"])))
    aguardar_tempo_para_clicar()
    botao_executar.click()

def solicitacoes_nfce(navegador, ie_empresa, data_inicio, data_fim, espera=2):
    if verificar_registro_json(ie_empresa, data_inicio, data_fim, NFCE["ARQUIVO_JSON"], "NFCE"): 
        return False
    try:
        preencher_datas(navegador, data_inicio, data_fim, espera)
        preencher_campo_iframe(navegador, ie_empresa, espera)
        selecionar_xml_e_executar(navegador, espera)
        salvar_horario_json(ie_empresa, data_inicio, data_fim, NFCE["ARQUIVO_JSON"], "NFCE")
        time.sleep(10)
        return True
    except Exception as e:
        logging.error(f"Erro ao solicitar NFCE: {e}")
        return False

def excluir_solicitacoes_anteriores():
    caminho_json = os.path.join(os.path.dirname(__file__), 'json_files/solicitacoes.json')
    if os.path.exists(caminho_json): os.remove(caminho_json)

def executar_processo_requests_nfce():

    navegador = None
    logging.info("Iniciando processo de solicitações de NFC-e...")
    try:
        navegador = iniciar_navegador()
        if navegador and realizar_login(navegador):
            link, lista_empresas = NFCE["LINK_SEFAZ_NFCE"], listar_empresas()
            data_inicio, data_fim = definir_datas_por_tipo("NFCE")
            acessar_link(navegador, link)
            
            for empresa in lista_empresas:
                ie_empresa = empresa['inscricao_estadual']
                if solicitacoes_nfce(navegador, ie_empresa, data_inicio, data_fim):
                    acessar_link(navegador, link)
                else:
                    logging.info(f"Combinação já existe no JSON: IE {ie_empresa}, Data Início {data_inicio}, Data Fim {data_fim}")
    
    except Exception as e:
        logging.error(f"Erro durante a execução: {e}")
    
    finally:
        if navegador:
            navegador.quit()

if __name__ == "__main__":
    executar_processo_requests_nfce()