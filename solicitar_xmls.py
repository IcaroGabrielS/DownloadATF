import time, logging
import os
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
from utils import (
    conectar_postgres,
    iniciar_navegador_firefox,
    autenticar_sefaz,
    acessar_pagina,
    espera_para_clicar
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def obter_solicitacoes_pendentes():
    """Busca solicitações pendentes no banco de dados PostgreSQL"""
    solicitacoes_pendentes = []
    max_tentativas = int(os.environ.get('MAX_TENTATIVAS', 3))
    
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
        
    except Exception as erro:
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
        
    except Exception as erro:
        logging.error(f"Erro ao atualizar solicitação: {erro}")
        if conexao:
            conexao.rollback()
            conexao.close()
        return False

def inserir_datas_formulario(navegador, data_inicio, data_fim, espera=2):
    wait = WebDriverWait(navegador, espera)
    data_inicio_elemento = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get('XPATH_DATA_INICIO'))))
    data_inicio_elemento.clear()
    data_inicio_elemento.send_keys(data_inicio)
    data_fim_elemento = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get('XPATH_DATA_FIM'))))
    data_fim_elemento.clear()
    data_fim_elemento.send_keys(data_fim)

def preencher_campo_iframe(navegador, ie_empresa, espera=2):
    wait = WebDriverWait(navegador, espera)
    wait.until(EC.frame_to_be_available_and_switch_to_it((By.XPATH, os.environ.get('XPATH_IFRAME'))))
    campo_valor = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get('XPATH_CAMPO_VALOR'))))
    campo_valor.clear()
    campo_valor.send_keys(ie_empresa)
    botao_pesquisar = wait.until(EC.element_to_be_clickable((By.XPATH, os.environ.get('XPATH_BOTAO_PESQUISAR'))))
    botao_pesquisar.click()
    navegador.switch_to.default_content()

def selecionar_xml_executar(navegador, espera=2):
    wait = WebDriverWait(navegador, espera)
    dropdown_xml = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get('XPATH_DROPDOWN_XML'))))
    dropdown_xml.click()
    opcao_xml = wait.until(EC.presence_of_element_located((By.XPATH, os.environ.get('XPATH_OPCAO_XML'))))
    opcao_xml.click()
    botao_executar = wait.until(EC.element_to_be_clickable((By.XPATH, os.environ.get('XPATH_BOTAO_EXECUTAR'))))
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
            link = os.environ.get('LINK_SEFAZ_NFCE')
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