import time, json, re, logging, os
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from ManageFilesNFCE import executar_processo_manage_files_nfce
from util import (iniciar_navegador, realizar_login, acessar_link)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("schedule.log"),  # Salva logs em um arquivo
        logging.StreamHandler()  # Exibe logs no console
    ]
)

with open("constantes.json", "r", encoding="utf-8") as arquivo_constantes:
    constantes = json.load(arquivo_constantes)
    DOWNLOAD = constantes["DOWNLOAD"]

with open("solicitacoes.json", "r", encoding="utf-8") as arquivo_solicitacoes:
    dados_json = json.load(arquivo_solicitacoes)

def salvar_links(navegador, dados_json):
    linhas = navegador.find_elements(By.XPATH, "//table/tbody/tr")
    for linha in linhas:
        try:
            imagem = linha.find_element(By.XPATH, "./td[3]/a/img")
            if imagem.get_attribute("alt") == "Anexo":
                link = linha.find_element(By.XPATH, "./td[6]/a")
                href = link.get_attribute("href")
                link_text = link.text.strip()
                match = re.match(r"javascript:abrirFilhas\('(\d+)',(\d+)\)", href)
                if match:
                    mensagem_id = match.group(1)
                    url = f"https://www4.sefaz.pb.gov.br/atf/seg/SEGf_MinhasMensagens.do?hidsqMensagem={mensagem_id}"
                    # Adiciona o link ao JSON se o link_text for correspondente dentro de uma tolerância de 10 segundos
                    try:
                        link_time = datetime.strptime(link_text, "%d/%m/%Y %H:%M:%S")
                        for item in dados_json:
                            item_time = datetime.strptime(item.get("horario"), "%d/%m/%Y %H:%M:%S")
                            if abs((item_time - link_time).total_seconds()) <= 10:
                                item["link"] = url
                                break
                    except ValueError: continue
        except: continue
    with open("solicitacoes.json", "w", encoding="utf-8") as arquivo_solicitacoes:
        json.dump(dados_json, arquivo_solicitacoes, ensure_ascii=False, indent=4)
    return dados_json

def ir_para_anexo(navegador):
    try:
        elemento = WebDriverWait(navegador, DOWNLOAD["ESPERAS"]["CURTA"]).until(EC.visibility_of_element_located((By.XPATH, DOWNLOAD["XPATHS"]["IMAGEM_ANEXO"])))
        elemento.click()
        return True
    except TimeoutException: return False

def clicar_no_download(navegador):
    try:
        elemento = WebDriverWait(navegador, DOWNLOAD["ESPERAS"]["CURTA"]).until(EC.visibility_of_element_located((By.XPATH, DOWNLOAD["XPATHS"]["LINK_DOWNLOAD"])))
        elemento.click()
        return True
    except TimeoutException: return False

def verificar_downloads_em_andamento(diretorio_download):
    for arquivo in os.listdir(diretorio_download):
        if arquivo.endswith('.part'):  # Arquivos temporários de download no Firefox
            return True
    return False

def executar_processo_downloads_nfce():
    navegador = None
    logging.info("Iniciando processo de download de NFC-e...")
    try:
        navegador = iniciar_navegador()
        if navegador and realizar_login(navegador):
            acessar_link(navegador, DOWNLOAD["URL_CAIXA_DOWNLOADS"])
            salvar_links(navegador, dados_json)

        # Inicializa o campo "baixado" como False para todos os itens que não o possuem
        for item in dados_json:
            if "baixado" not in item:
                item["baixado"] = False

        for item in dados_json:
            link = item.get("link")
            baixado = item.get("baixado")

            if baixado:
                logging.info(f"Download já realizado anteriormente para o link: {link}")
                continue

            if link:
                acessar_link(navegador, link)
                if ir_para_anexo(navegador) and clicar_no_download(navegador):
                    item["baixado"] = True
                else:
                    logging.error(f"Falha ao realizar o download para o link: {link}")
                    item["baixado"] = False

                # Salva o JSON após cada iteração para garantir que o progresso não seja perdido
                with open("solicitacoes.json", "w", encoding="utf-8") as arquivo_solicitacoes:
                    json.dump(dados_json, arquivo_solicitacoes, ensure_ascii=False, indent=4)

                time.sleep(10)

    except Exception as e:
        logging.error(f"Erro durante a execução: {e}")
    
    finally:
        max_tentativas = 10
        tentativa = 0
        while verificar_downloads_em_andamento(r"C:\NFCE_XML_TEMP") and tentativa < max_tentativas:
            time.sleep(2)
            tentativa += 1
        if navegador:
            navegador.quit()
        executar_processo_manage_files_nfce()
        #criar a lista dos não baixados

if __name__ == "__main__":
    executar_processo_downloads_nfce()