import time, json, re, logging, os
from datetime import datetime
from selenium.webdriver.common.by import By
from util import (iniciar_navegador_firefox, autenticar_sefaz, acessar_pagina, clicar_elemento)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

with open("json_files/constantes.json", "r", encoding="utf-8") as arquivo_constantes:
    constantes = json.load(arquivo_constantes)
    DOWNLOAD = constantes["DOWNLOAD"]

with open("json_files/solicitacoes.json", "r", encoding="utf-8") as arquivo_solicitacoes:
    dados_json = json.load(arquivo_solicitacoes)

def verificar_downloads_em_progresso(diretorio_download):
    return any(arquivo.endswith('.part') for arquivo in os.listdir(diretorio_download))

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
    with open("json_files/solicitacoes.json", "w", encoding="utf-8") as arquivo_solicitacoes:
        json.dump(dados_json, arquivo_solicitacoes, ensure_ascii=False, indent=4)
    return dados_json

def filtrar_pendentes(dados_json):
    solicitacoes_pendentes = []
    for solicitacao in dados_json:
        if not solicitacao.get('baixado', False):
            solicitacao['link'] = None
            solicitacao['horario'] = None
            solicitacao['solicitado'] = False
            solicitacoes_pendentes.append(solicitacao)
    with open("json_files/solicitacoes.json", "w", encoding="utf-8") as arquivo_solicitacoes:
        json.dump(solicitacoes_pendentes, arquivo_solicitacoes, ensure_ascii=False, indent=4)
    return solicitacoes_pendentes

def executar_processo_downloads_nfce():
    navegador = None
    logging.info("Iniciando processo de download de NFC-e...")
    try:
        navegador = iniciar_navegador_firefox()
        if navegador and autenticar_sefaz(navegador):
            acessar_pagina(navegador, DOWNLOAD["URL_CAIXA_DOWNLOADS"])
            salvar_links(navegador, dados_json)
        for item in dados_json:
            link, baixado = item.get("link"), item.get("baixado")
            if baixado: continue
            if link:
                acessar_pagina(navegador, link)
                if clicar_elemento(navegador, DOWNLOAD["XPATHS"]["IMAGEM_ANEXO"]) and clicar_elemento(navegador, DOWNLOAD["XPATHS"]["LINK_DOWNLOAD"]):
                    item["baixado"] = True
                else: logging.error(f"Falha ao realizar o download para o link: {link}")
                with open("json_files/solicitacoes.json", "w", encoding="utf-8") as arquivo_solicitacoes:
                    json.dump(dados_json, arquivo_solicitacoes, ensure_ascii=False, indent=4)
                time.sleep(10)
    except Exception as e: logging.error(f"Erro durante a execução: {e}")
    finally:
        tentativa, max_tentativas = 0, 10
        while verificar_downloads_em_progresso(r"C:\NFCE_XML_TEMP") and tentativa < max_tentativas:
            time.sleep(2)
            tentativa += 1
        if navegador: navegador.quit()
        
        #executar_processo_manage_files_nfce()
    

if __name__ == "__main__":
    filtrar_pendentes(dados_json)