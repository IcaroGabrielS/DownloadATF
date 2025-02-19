import time, json, logging, os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from util import (
    capturar_horario, iniciar_navegador, aguardar_tempo_para_clicar, 
    realizar_login, acessar_link, criar_lista_solicitacoes
)

# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("schedule.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Carregar constantes do JSON
with open("json_files/constantes.json", "r", encoding="utf-8") as arquivo_constantes:
    constantes = json.load(arquivo_constantes)
    NFCE = constantes["NFCE"]

def carregar_solicitacoes():
    """Carrega o JSON de solicitações, garantindo que seja uma lista."""
    caminho_json = "json_files/solicitacoes.json"
    if os.path.exists(caminho_json):
        with open(caminho_json, "r", encoding="utf-8") as arquivo:
            return json.load(arquivo)
    return []

def salvar_solicitacoes(solicitacoes):
    """Salva o JSON de solicitações atualizado."""
    caminho_json = "json_files/solicitacoes.json"
    with open(caminho_json, "w", encoding="utf-8") as arquivo:
        json.dump(solicitacoes, arquivo, indent=4, ensure_ascii=False)

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

def solicitacoes_nfce(navegador, empresa, espera=2):
    """Executa o processo de solicitação da NFC-e e atualiza os dados no JSON."""
    try:
        preencher_datas(navegador, empresa["data_ini"], empresa["data_fim"], espera)
        preencher_campo_iframe(navegador, empresa["inscricao_estadual"], espera)
        selecionar_xml_e_executar(navegador, espera)

        # Captura o horário e atualiza no JSON
        horario_capturado = capturar_horario()
        empresa["horario"] = horario_capturado
        empresa["solicitado"] = True

        time.sleep(10)
        return True
    except Exception as e:
        logging.error(f"Erro ao solicitar NFCE: {e}")
        return False

def excluir_solicitacoes_anteriores():
    """Remove o arquivo de solicitações, se existir."""
    caminho_json = "json_files/solicitacoes.json"
    if os.path.exists(caminho_json):
        os.remove(caminho_json)

def executar_processo_requests_nfce():
    """Executa todo o processo de solicitação de NFC-e usando os dados do JSON."""
    excluir_solicitacoes_anteriores()
    criar_lista_solicitacoes("NFCE")  # Gera novo JSON
    solicitacoes = carregar_solicitacoes()  # Carrega o novo JSON

    navegador = None
    logging.info("Iniciando processo de solicitações de NFC-e...")
    
    try:
        navegador = iniciar_navegador()
        if navegador and realizar_login(navegador):
            link = NFCE["LINK_SEFAZ_NFCE"]
            acessar_link(navegador, link)

            for empresa in solicitacoes:
                if not empresa["solicitado"]:
                    if solicitacoes_nfce(navegador, empresa):
                        empresa["solicitado"] = True  # Marca como solicitado
                        salvar_solicitacoes(solicitacoes)  # Salva atualização no JSON
                    acessar_link(navegador, link)
                else:
                    logging.info(f"Já solicitado: IE {empresa['inscricao_estadual']}, Data Início {empresa['data_ini']}, Data Fim {empresa['data_fim']}")

    except Exception as e:
        logging.error(f"Erro durante a execução: {e}")

    finally:
        if navegador:
            navegador.quit()

if __name__ == "__main__":
    executar_processo_requests_nfce()
