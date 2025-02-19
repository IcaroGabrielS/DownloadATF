import time, json, logging, os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from util import (capturar_data_hora, iniciar_navegador_firefox, autenticar_sefaz, 
                  acessar_pagina, montar_lista_solicitacoes, remover_solicitacoes_anteriores)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

with open("json_files/constantes.json", "r", encoding="utf-8") as arquivo_constantes:
    constantes = json.load(arquivo_constantes)
    NFCE = constantes["NFCE"]

def carregar_solicitacoes():
    caminho_json = "json_files/solicitacoes.json"
    if os.path.exists(caminho_json):
        with open(caminho_json, "r", encoding="utf-8") as arquivo:
            return json.load(arquivo)
    return []

def salvar_solicitacoes(solicitacoes):
    caminho_json = "json_files/solicitacoes.json"
    with open(caminho_json, "w", encoding="utf-8") as arquivo:
        json.dump(solicitacoes, arquivo, indent=4, ensure_ascii=False)

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
    espera_para_clicar()
    botao_executar.click()

def solicitar_nfce(navegador, empresa, espera=2):
    try:
        inserir_datas_formulario(navegador, empresa["data_ini"], empresa["data_fim"], espera)
        preencher_campo_iframe(navegador, empresa["inscricao_estadual"], espera)
        selecionar_xml_executar(navegador, espera)
        horario_capturado = capturar_data_hora()
        empresa["horario"] = horario_capturado
        empresa["solicitado"] = True
        time.sleep(10)
        return True
    except Exception as e:
        logging.error(f"Erro ao solicitar NFCE: {e}")
        return False

def executar_processo_requests_nfce():

    #remover_solicitacoes_anteriores(), montar_lista_solicitacoes("NFCE")
    solicitacoes = carregar_solicitacoes()  # Carrega o novo JSON
    logging.info("Iniciando processo de solicitações de NFC-e...")

    try:
        navegador = iniciar_navegador_firefox()
        if navegador and autenticar_sefaz(navegador):
            link = NFCE["LINK_SEFAZ_NFCE"]
            acessar_pagina(navegador, link)
            for empresa in solicitacoes:
                if not empresa["solicitado"]:
                    if solicitar_nfce(navegador, empresa):
                        empresa["solicitado"] = True
                        salvar_solicitacoes(solicitacoes)
                    acessar_pagina(navegador, link)
                else:
                    logging.info(f"Já solicitado: IE {empresa['inscricao_estadual']}, Data Início {empresa['data_ini']}, Data Fim {empresa['data_fim']}")

    except Exception as e:
        logging.error(f"Erro durante a execução: {e}")

    finally:
        if navegador:
            navegador.quit()

if __name__ == "__main__":
    executar_processo_requests_nfce()