import time, os, signal, sys
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
from loggingConfig import get_logger
from utils import (
    conectar_postgres,
    iniciar_navegador_selenoid,
    autenticar_sefaz,
    acessar_pagina,
    verificar_downloads_em_progresso,
    clicar_elemento
)

load_dotenv()
logger = get_logger(__name__)

# Configuração do diretório de downloads
DIRETORIO_DOWNLOADS = "/home/desenvolvimento/DownloadATF/NFCE_XML_TEMP/incoming"
os.makedirs(DIRETORIO_DOWNLOADS, exist_ok=True)
os.chmod(DIRETORIO_DOWNLOADS, 0o777)

# Controle de execução
RUNNING = True

def configurar_tratamento_sinais():
    """Configura o tratamento de sinais para finalização limpa"""
    def handler_signal(signum, frame):
        global RUNNING
        logger.info(f"Sinal {signum} recebido. Preparando para encerrar serviço...")
        RUNNING = False
        logger.info("Serviço finalizado pelo usuário.")
        sys.exit(0)

    signal.signal(signal.SIGINT, handler_signal)
    signal.signal(signal.SIGTERM, handler_signal)

def obter_solicitacoes_com_link():
    """Busca solicitações que têm link, anexo=true e ainda não foram baixadas"""
    conexao = conectar_postgres()
    if not conexao:
        return []

    try:
        cursor = conexao.cursor()
        cursor.execute("""
            SELECT id, inscricao_estadual, link
            FROM nfce.solicitacoes
            WHERE link IS NOT NULL AND link != '' AND baixado = 0 AND tipo = 'NFCE' AND anexo = true
            ORDER BY criado_em
        """)

        solicitacoes = []
        for id, inscricao_estadual, link in cursor.fetchall():
            solicitacoes.append({
                "id": id,
                "inscricao_estadual": inscricao_estadual,
                "link": link
            })

        cursor.close()
        conexao.close()

        if solicitacoes:
            logger.info(f"Encontradas {len(solicitacoes)} solicitações pendentes de download com anexo=true")
        return solicitacoes

    except Exception as erro:
        logger.error(f"Erro ao buscar solicitações: {erro}")
        if conexao:
            conexao.close()
        return []

def marcar_como_baixado(id_solicitacao):
    """Marca a solicitação como baixada no banco de dados"""
    conexao = conectar_postgres()
    if not conexao:
        logger.error(f"Falha ao conectar ao PostgreSQL para marcar solicitação {id_solicitacao}")
        return False

    try:
        cursor = conexao.cursor()
        cursor.execute("""
            UPDATE nfce.solicitacoes
            SET baixado = baixado + 1,
                atualizado_em = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (id_solicitacao,))

        conexao.commit()
        cursor.close()
        conexao.close()

        logger.info(f"Solicitação {id_solicitacao} marcada como baixada com sucesso")
        return True

    except Exception as erro:
        logger.error(f"Erro ao marcar solicitação {id_solicitacao} como baixada: {erro}")
        if conexao:
            conexao.rollback()
            conexao.close()
        return False

def realizar_download(navegador, solicitacao):
    """Acessa o link e inicia o download do arquivo"""
    try:
        logger.info(f"Iniciando download para IE {solicitacao['inscricao_estadual']} (ID: {solicitacao['id']})")

        # Acessa o link para download
        acessar_pagina(navegador, solicitacao["link"])

        # Clica nos elementos de download
        espera_curta = int(os.environ.get("ESPERA_CURTA", 2))
        if clicar_elemento(navegador, os.environ.get("XPATH_IMAGEM_ANEXO"), espera_curta) and \
           clicar_elemento(navegador, os.environ.get("XPATH_LINK_DOWNLOAD"), espera_curta):
            logger.info(f"Download iniciado para IE {solicitacao['inscricao_estadual']} (ID: {solicitacao['id']})")
            time.sleep(10)
            return True
        else:
            logger.error(f"Falha ao clicar nos elementos para download - IE {solicitacao['inscricao_estadual']} (ID: {solicitacao['id']})")
            return False
    except Exception as e:
        logger.error(f"Erro ao realizar download para IE {solicitacao['inscricao_estadual']} (ID: {solicitacao['id']}): {str(e)}")
        return False

def processar_downloads():
    """Processa todos os downloads pendentes"""
    # Verificar permissões do diretório
    try:
        test_file_path = os.path.join(DIRETORIO_DOWNLOADS, "test_write_permission.tmp")
        with open(test_file_path, 'w') as f:
            f.write("Teste de permissão de escrita")
        os.remove(test_file_path)
    except Exception as e:
        logger.error(f"ERRO DE PERMISSÃO no diretório {DIRETORIO_DOWNLOADS}: {str(e)}")
        return 0

    # Carrega as solicitações pendentes de download
    solicitacoes = obter_solicitacoes_com_link()

    if not solicitacoes:
        return 0

    navegador = None
    downloads_realizados = 0
    total_solicitacoes = len(solicitacoes)

    try:
        # Inicia o navegador com Selenoid
        logger.info(f"Iniciando navegador para processar {total_solicitacoes} downloads...")
        navegador = iniciar_navegador_selenoid(DIRETORIO_DOWNLOADS)

        if navegador and autenticar_sefaz(navegador):
            logger.info("Navegador inicializado e autenticado com sucesso")

            # Processa cada solicitação com link disponível
            for i, solicitacao in enumerate(solicitacoes, 1):
                # Log de progresso menos frequente
                if i == 1 or i == total_solicitacoes or i % 10 == 0:
                    logger.info(f"Processando solicitação {i}/{total_solicitacoes}")

                if realizar_download(navegador, solicitacao):
                    if marcar_como_baixado(solicitacao["id"]):
                        downloads_realizados += 1
                    else:
                        logger.warning(f"Download realizado mas falha ao marcar como baixado: ID {solicitacao['id']}")

                # Espera entre os downloads
                time.sleep(5)

            logger.info(f"Downloads concluídos: {downloads_realizados}/{total_solicitacoes}")
        else:
            logger.error("Falha na autenticação ou inicialização do navegador")

    except Exception as e:
        logger.error(f"Erro durante a execução dos downloads: {str(e)}")
    finally:
        # Aguarda downloads em andamento concluírem
        max_tentativas = int(os.environ.get("MAX_TENTATIVAS", 10))
        tentativa = 0

        while verificar_downloads_em_progresso(DIRETORIO_DOWNLOADS) and tentativa < max_tentativas:
            logger.debug(f"Aguardando conclusão de downloads em progresso... (tentativa {tentativa+1}/{max_tentativas})")
            time.sleep(2)
            tentativa += 1

        if navegador:
            try:
                navegador.quit()
                logger.info("Navegador fechado com sucesso")
            except Exception as e:
                logger.warning(f"Erro ao fechar navegador: {str(e)}")

        try:
            arquivos = os.listdir(DIRETORIO_DOWNLOADS)
            logger.info(f"Total de {len(arquivos)} arquivos no diretório de download")
        except Exception as e:
            logger.error(f"Erro ao listar arquivos: {str(e)}")

    return downloads_realizados

def monitorar_downloads_continuamente():
    """Função principal que monitora continuamente as solicitações para download"""
    global RUNNING

    execucao_id = f"DOWNLOAD-{time.strftime('%Y%m%d-%H%M%S')}"
    logger.info(f"=" * 50)
    logger.info(f"INICIANDO SERVIÇO DE MONITORAMENTO DE DOWNLOADS ({execucao_id})")
    logger.info(f"=" * 50)

    configurar_tratamento_sinais()

    intervalo_verificacao = int(os.environ.get("INTERVALO_VERIFICACAO", 60))
    ultimo_log_sem_downloads = 0
    intervalo_min_log_sem_downloads = 300  # 5 minutos

    while RUNNING:
        try:
            # Verifica se há solicitações com links para baixar
            solicitacoes = obter_solicitacoes_com_link()

            if not solicitacoes:
                # Log periódico sobre ausência de downloads
                agora = time.time()
                if agora - ultimo_log_sem_downloads > intervalo_min_log_sem_downloads:
                    logger.info("Não há solicitações pendentes para download. Aguardando...")
                    ultimo_log_sem_downloads = agora

                # Aguarda antes de verificar novamente
                time.sleep(intervalo_verificacao)
                continue

            # Se há solicitações, processa os downloads
            downloads_realizados = processar_downloads()

            if downloads_realizados > 0:
                logger.info(f"{downloads_realizados} downloads processados com sucesso")
                time.sleep(5)
            else:
                time.sleep(intervalo_verificacao)

        except Exception as e:
            logger.error(f"Erro durante o monitoramento de downloads: {e}")
            time.sleep(30)

    logger.info("Serviço de monitoramento de downloads encerrado normalmente")

if __name__ == "__main__":
    try:
        monitorar_downloads_continuamente()
    except KeyboardInterrupt:
        logger.info("Serviço de monitoramento de downloads interrompido pelo usuário")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Erro fatal no serviço de monitoramento de downloads: {str(e)}")
        sys.exit(1)
desenvolvimento@srvms02:~/DownloadATF$ ls
logs  NFCE_XML_TEMP  venv  atualizacaoManual.py  baixarArquivos.py  gerenciarArquivos.py  localizarLinks.py  loggingConfig.py  solicitarXmls.py  solicitarXmlsFalhos.py  startList.py  utils.py
desenvolvimento@srvms02:~/DownloadATF$ cat gerenciarArquivos.py
import os, re, shutil, time, json, uuid, zipfile, signal, sys
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import xml.etree.ElementTree as ET
from mysql.connector import Error
from datetime import datetime
from dateutil import parser
import mysql.connector
from dotenv import load_dotenv
from loggingConfig import get_logger

# Carregar variáveis de ambiente
load_dotenv()

# Configurar logger
logger = get_logger(__name__)

# ============================================
# CONFIGURAÇÕES
# ============================================

DATABASE_CONFIG = {
    "HOST": os.environ.get("MYSQL_HOST"),
    "USER": os.environ.get("MYSQL_USER"),
    "PASSWORD": os.environ.get("MYSQL_PASSWORD"),
    "DATABASE": os.environ.get("MYSQL_DATABASE")
}

QUERY_LISTAR_EMPRESAS = os.environ.get("QUERY_LISTAR_EMPRESAS")

# ============================================
# UTILITY FUNCTIONS
# ============================================

def obter_diretorio_execucao():
    """Retorna o diretório onde o programa está sendo executado"""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))

# Diretórios
DIRETORIO_EXECUCAO = os.environ.get("DIRETORIO_EXECUCAO") or obter_diretorio_execucao()
DIRETORIO_BASE = os.path.join(DIRETORIO_EXECUCAO, "NFCE_XML_TEMP")
DIRETORIO_FINAL = os.environ.get("DIRETORIO_FINAL")

# Estrutura de diretórios de trabalho
ESTRUTURA_DIRETORIOS = {
    "incoming": os.environ.get("DIRETORIO_DOWNLOADS") or os.path.join(DIRETORIO_BASE, "incoming"),
    "processing": os.path.join(DIRETORIO_BASE, "processing"),
    "failed": os.path.join(DIRETORIO_BASE, "failed")
}

# Estados possíveis de processamento
ESTADOS = {
    "INIT": "Inicializado",
    "EXTRACTING": "Extraindo arquivos",
    "EXTRACTED": "Arquivos extraídos",
    "ANALYZING": "Analisando XMLs",
    "RENAMING": "Renomeando pasta",
    "RENAMED": "Pasta renomeada",
    "MOVING": "Movendo para destino final",
    "COMPLETED": "Processamento completo",
    "FAILED": "Processamento falhou"
}

# Controle de processamento
running = True
ultimo_heartbeat = time.time()
INTERVALO_HEARTBEAT = int(os.environ.get("INTERVALO_HEARTBEAT", 3600))  # 1 hora por padrão

# ============================================
# DATABASE FUNCTIONS
# ============================================

def conectar_mysql():
    """Estabelece conexão com o banco de dados MySQL."""
    try:
        conexao = mysql.connector.connect(
            host=DATABASE_CONFIG["HOST"],
            user=DATABASE_CONFIG["USER"],
            password=DATABASE_CONFIG["PASSWORD"],
            database=DATABASE_CONFIG["DATABASE"]
        )
        if conexao.is_connected():
            logger.info("Conexão ao MySQL estabelecida com sucesso")
        return conexao
    except Error as erro:
        logger.error(f"Erro ao conectar ao MySQL: {erro}")
        return None

def listar_empresas():
    """Lista empresas do banco de dados MySQL"""
    logger.info("Listando empresas do banco de dados")
    conexao = conectar_mysql()
    if conexao and conexao.is_connected():
        cursor = conexao.cursor()
        cursor.execute(QUERY_LISTAR_EMPRESAS)
        resultados = cursor.fetchall()
        colunas = [desc[0] for desc in cursor.description]
        cursor.close()
        conexao.close()
        dados = [dict(zip(colunas, linha)) for linha in resultados]
        logger.info(f"{len(dados)} empresas encontradas")
        return dados
    logger.error("Falha ao conectar ao banco de dados")
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
            logger.error(f"Tipo inválido: {tipo}. Use 'ie' ou 'data'.")
            return None
    except Exception as e:
        logger.error(f"Erro ao processar o XML {xml_path}: {e}")
        return None

# ============================================
# DIRECTORY MANAGEMENT
# ============================================

def configurar_diretorios():
    """Cria a estrutura de diretórios necessária"""
    for nome, caminho in ESTRUTURA_DIRETORIOS.items():
        os.makedirs(caminho, exist_ok=True)
        logger.info(f"Diretório {nome} verificado: {caminho}")

def gerar_id_job():
    """Gera um ID único para o job de processamento"""
    return f"job_{uuid.uuid4().hex[:10]}_{int(time.time())}"

def atualizar_estado(job_dir, estado, dados_adicionais=None):
    """Atualiza o estado de um job de processamento"""
    state_file = os.path.join(job_dir, ".state")

    state_data = {
        "estado": estado,
        "descricao": ESTADOS.get(estado, "Estado desconhecido"),
        "timestamp": datetime.now().isoformat(),
    }

    if dados_adicionais:
        state_data.update(dados_adicionais)

    with open(state_file, 'w') as f:
        json.dump(state_data, f, indent=2)

    logger.info(f"Estado do job {os.path.basename(job_dir)} atualizado para: {estado}")

def ler_estado(job_dir):
    """Lê o estado atual de um job de processamento"""
    state_file = os.path.join(job_dir, ".state")

    if not os.path.exists(state_file):
        return None

    try:
        with open(state_file, 'r') as f:
            state_data = json.load(f)
        return state_data
    except Exception as e:
        logger.error(f"Erro ao ler estado do job {os.path.basename(job_dir)}: {e}")
        return None

def mover_para_falhas(job_dir, motivo):
    """Move um job falho para o diretório de falhas"""
    job_id = os.path.basename(job_dir)
    destino = os.path.join(ESTRUTURA_DIRETORIOS["failed"], job_id)

    # Garantir que não exista pasta com mesmo nome no destino
    if os.path.exists(destino):
        timestamp = int(time.time())
        destino = f"{destino}_{timestamp}"

    # Atualizar estado antes de mover
    atualizar_estado(job_dir, "FAILED", {"motivo": motivo})

    # Mover pasta
    shutil.move(job_dir, destino)
    logger.info(f"Job {job_id} movido para falhas. Motivo: {motivo}")

# ============================================
# RECOVERY FUNCTIONS
# ============================================

def verificar_processamentos_pendentes():
    """Verifica e recupera processamentos interrompidos na inicialização"""
    processing_dir = ESTRUTURA_DIRETORIOS["processing"]

    jobs = [d for d in os.listdir(processing_dir)
            if os.path.isdir(os.path.join(processing_dir, d))]

    if not jobs:
        logger.info("Nenhum processamento pendente encontrado")
        return

    logger.info(f"Encontrados {len(jobs)} processamentos pendentes. Tentando recuperar...")

    for job_id in jobs:
        job_dir = os.path.join(processing_dir, job_id)
        state_data = ler_estado(job_dir)

        if not state_data:
            logger.warning(f"Job {job_id} não possui arquivo de estado válido. Movendo para falhas...")
            mover_para_falhas(job_dir, "sem_estado_valido")
            continue

        estado = state_data.get("estado")
        logger.info(f"Recuperando job {job_id} no estado: {estado}")

        try:
            # Recuperar baseado no estado
            if estado == "INIT" or estado == "EXTRACTING":
                # Vamos recomeçar a extração
                extracted_dir = os.path.join(job_dir, "extracted")
                os.makedirs(extracted_dir, exist_ok=True)

                # Ver se há arquivo original para extrair
                arquivos_zip = [f for f in os.listdir(job_dir) if f.endswith('.zip')]
                if not arquivos_zip:
                    mover_para_falhas(job_dir, "arquivo_zip_original_ausente")
                    continue

                # Limpar qualquer conteúdo extraído anteriormente
                for item in os.listdir(extracted_dir):
                    item_path = os.path.join(extracted_dir, item)
                    if os.path.isfile(item_path):
                        os.remove(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)

                # Reextrair o ZIP
                processar_zip(os.path.join(job_dir, arquivos_zip[0]), job_dir)

            elif estado == "EXTRACTED" or estado == "ANALYZING":
                # Vamos recomeçar a análise dos XMLs
                extracted_dir = os.path.join(job_dir, "extracted")
                if not os.path.exists(extracted_dir):
                    mover_para_falhas(job_dir, "diretorio_extracted_ausente")
                    continue

                analisar_e_renomear(job_dir)

            elif estado == "RENAMING" or estado == "RENAMED":
                # Vamos finalizar a renomeação e mover para destino final
                mover_para_destino_final(job_dir)

            elif estado == "MOVING":
                # Apenas mover para destino final
                mover_para_destino_final(job_dir)

            elif estado == "COMPLETED":
                # Job já está completo, limpar
                finalizar_job(job_dir)

            elif estado == "FAILED":
                # Job já está marcado como falha
                mover_para_falhas(job_dir, state_data.get("motivo", "falha_anterior"))

            else:
                logger.warning(f"Estado desconhecido para job {job_id}: {estado}")
                mover_para_falhas(job_dir, "estado_desconhecido")

        except Exception as e:
            logger.error(f"Erro ao recuperar job {job_id}: {e}", exc_info=True)
            mover_para_falhas(job_dir, f"erro_recuperacao_{str(e).replace(' ', '_')[:50]}")

# ============================================
# PROCESSING FUNCTIONS
# ============================================

def criar_job_processamento(arquivo_zip):
    """Cria um novo job de processamento para um arquivo ZIP"""
    # Criar diretório do job
    job_id = gerar_id_job()
    job_dir = os.path.join(ESTRUTURA_DIRETORIOS["processing"], job_id)
    os.makedirs(job_dir, exist_ok=True)

    # Criar diretório de extração
    extracted_dir = os.path.join(job_dir, "extracted")
    os.makedirs(extracted_dir, exist_ok=True)

    # Copiar arquivo ZIP para o diretório de processamento
    zip_origem = os.path.join(ESTRUTURA_DIRETORIOS["incoming"], arquivo_zip)
    zip_destino = os.path.join(job_dir, arquivo_zip)
    shutil.copy2(zip_origem, zip_destino)

    # Inicializar estado
    atualizar_estado(job_dir, "INIT", {"arquivo_original": arquivo_zip})

    logger.info(f"Job de processamento {job_id} criado para o arquivo {arquivo_zip}")
    return job_dir

def finalizar_job(job_dir):
    """Finaliza um job bem-sucedido, removendo-o do sistema"""
    job_id = os.path.basename(job_dir)

    # Registrar conclusão no log
    logger.info(f"Finalizando job {job_id} - processamento concluído com sucesso")

    # Atualizar estado final antes de remover
    atualizar_estado(job_dir, "COMPLETED")

    # Remover o diretório do job
    try:
        shutil.rmtree(job_dir)
        logger.info(f"Job {job_id} removido após processamento bem-sucedido")
    except Exception as e:
        logger.error(f"Erro ao remover job {job_id}: {e}")

def processar_zip(zip_file_path, job_dir):
    """Processa um arquivo ZIP, extraindo seu conteúdo"""
    extracted_dir = os.path.join(job_dir, "extracted")
    arquivo_zip = os.path.basename(zip_file_path)

    try:
        # Atualizar estado
        atualizar_estado(job_dir, "EXTRACTING")

        # Verificar se é um ZIP válido e se começa com NFCE_XML
        if not arquivo_zip.endswith('.zip') or not arquivo_zip.startswith('NFCE_XML'):
            mover_para_falhas(job_dir, "formato_arquivo_invalido")
            return False

        # Extrair conteúdo
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            for item in zip_ref.namelist():
                # Extrair apenas arquivos XML
                if item.endswith('.xml'):
                    caminho_extraido = os.path.join(extracted_dir, item)
                    # Evitar sobrescrever arquivos com mesmo nome
                    if os.path.exists(caminho_extraido):
                        base_name, ext = os.path.splitext(caminho_extraido)
                        caminho_extraido = f"{base_name}_{int(time.time())}{ext}"

                    # Extrair o arquivo
                    with open(caminho_extraido, 'wb') as f:
                        f.write(zip_ref.read(item))

        # Verificar se extraiu algum arquivo
        arquivos_xml = [f for f in os.listdir(extracted_dir) if f.endswith('.xml')]
        if not arquivos_xml:
            mover_para_falhas(job_dir, "nenhum_xml_encontrado")
            return False

        # Atualizar estado
        atualizar_estado(job_dir, "EXTRACTED", {"qtd_xmls": len(arquivos_xml)})

        # Continuar processamento - analisar e renomear
        return analisar_e_renomear(job_dir)

    except zipfile.BadZipFile:
        logger.error(f"Arquivo ZIP inválido: {arquivo_zip}")
        mover_para_falhas(job_dir, "zip_invalido")
        return False
    except Exception as e:
        logger.error(f"Erro ao processar ZIP {arquivo_zip}: {e}", exc_info=True)
        mover_para_falhas(job_dir, f"erro_extracao_{str(e).replace(' ', '_')[:50]}")
        return False

def analisar_e_renomear(job_dir):
    """Analisa os XMLs extraídos e renomeia o diretório com base nos dados"""
    extracted_dir = os.path.join(job_dir, "extracted")

    try:
        # Atualizar estado
        atualizar_estado(job_dir, "ANALYZING")

        # Verificar se há XMLs para analisar
        arquivos_xml = [f for f in os.listdir(extracted_dir)
                        if f.endswith('.xml') and f.startswith('NFCE_')]

        if not arquivos_xml:
            mover_para_falhas(job_dir, "nenhum_xml_nfce_encontrado")
            return False

        # Extrair dados dos XMLs
        datas = []
        ies = set()

        for xml_file in arquivos_xml:
            xml_path = os.path.join(extracted_dir, xml_file)
            logger.debug(f"Extraindo dados do XML: {xml_file}")

            try:
                ie_empresa = extrair_dado_xml(xml_path, "ie")
                data_emissao = extrair_dado_xml(xml_path, "data")

                if data_emissao:
                    datas.append(data_emissao)
                if ie_empresa:
                    ies.add(ie_empresa)
            except Exception as e:
                logger.warning(f"Erro ao extrair dados do XML {xml_file}: {e}")
                # Continuar com os outros arquivos, não falhar o job inteiro

        # Determinar nome a ser usado
        if datas:
            data_ini = min(datas).strftime('%Y%m%d')
            data_fim = max(datas).strftime('%Y%m%d')
        else:
            data_ini = data_fim = '00000000'

        if len(ies) > 1:
            novo_nome = f"ERR_{data_ini}_{data_fim}_" + "_".join(ies)
        else:
            novo_nome = f"{data_ini}_{data_fim}_{next(iter(ies), 'SEM_IE')}"

        # Salvar informações no estado
        atualizar_estado(job_dir, "RENAMING", {
            "nome_diretorio": novo_nome,
            "data_ini": data_ini,
            "data_fim": data_fim,
            "ies": list(ies)
        })

        # Continuar processamento - mover para destino final
        return mover_para_destino_final(job_dir)

    except Exception as e:
        logger.error(f"Erro ao analisar XMLs no job {os.path.basename(job_dir)}: {e}", exc_info=True)
        mover_para_falhas(job_dir, f"erro_analise_{str(e).replace(' ', '_')[:50]}")
        return False

def encontrar_pasta_destino(ie):
    """Encontra a pasta de destino para uma determinada IE"""
    for folder in os.listdir(DIRETORIO_FINAL):
        folder_path = os.path.join(DIRETORIO_FINAL, folder)
        if os.path.isdir(folder_path) and folder.endswith(f"_{ie}"):
            return folder
    return None

def mover_para_destino_final(job_dir):
    """Move os arquivos processados para o destino final"""
    try:
        # Ler estado atual
        state_data = ler_estado(job_dir)
        if not state_data:
            mover_para_falhas(job_dir, "estado_ausente_ao_mover")
            return False

        # Verificar se temos o nome do diretório
        novo_nome = state_data.get("nome_diretorio")
        if not novo_nome:
            mover_para_falhas(job_dir, "nome_diretorio_ausente")
            return False

        # Atualizar estado
        atualizar_estado(job_dir, "MOVING", {"destino": DIRETORIO_FINAL})

        # Determinar se é um erro ou uma pasta normal
        if novo_nome.startswith("ERR_"):
            # Caso especial - diretório de erros
            erros_path = os.path.join(DIRETORIO_FINAL, "ERROS")
            if not os.path.exists(erros_path):
                os.makedirs(erros_path, exist_ok=True)
                logger.info(f"Subpasta 'ERROS' criada em {erros_path}")

            destino_final = os.path.join(erros_path, novo_nome)

            # Garantir nome único
            if os.path.exists(destino_final):
                base_name = novo_nome
                for i in range(1, 100):
                    novo_nome_com_sufixo = f"{base_name} ({i})"
                    destino_final = os.path.join(erros_path, novo_nome_com_sufixo)
                    if not os.path.exists(destino_final):
                        novo_nome = novo_nome_com_sufixo
                        break

            # Criar diretório final
            os.makedirs(destino_final, exist_ok=True)

            # Copiar arquivos XML
            extracted_dir = os.path.join(job_dir, "extracted")
            for arquivo in os.listdir(extracted_dir):
                if arquivo.endswith('.xml'):
                    shutil.copy2(
                        os.path.join(extracted_dir, arquivo),
                        os.path.join(destino_final, arquivo)
                    )

            logger.info(f"Arquivos copiados para pasta de erros: {destino_final}")

        else:
            # Pasta normal - procurar pela IE
            try:
                *_, ie = novo_nome.rsplit('_', 1)
            except ValueError:
                mover_para_falhas(job_dir, f"formato_invalido_nome_{novo_nome}")
                return False

            # Encontrar pasta correspondente no destino
            matching_folder = encontrar_pasta_destino(ie)

            if not matching_folder:
                # Não encontrou pasta de destino - mover para erros
                logger.error(f"Não foi encontrada subpasta para IE {ie}")

                erros_path = os.path.join(DIRETORIO_FINAL, "ERROS")
                os.makedirs(erros_path, exist_ok=True)

                destino_final = os.path.join(erros_path, novo_nome)
                if os.path.exists(destino_final):
                    base_name = novo_nome
                    for i in range(1, 100):
                        novo_nome_com_sufixo = f"{base_name} ({i})"
                        destino_final = os.path.join(erros_path, novo_nome_com_sufixo)
                        if not os.path.exists(destino_final):
                            novo_nome = novo_nome_com_sufixo
                            break
            else:
                # Encontrou pasta de destino
                destino_final = os.path.join(DIRETORIO_FINAL, matching_folder, novo_nome)

                # Garantir nome único
                if os.path.exists(destino_final):
                    # Se já existe, substituir
                    logger.info(f"Destino {destino_final} já existe, substituindo...")
                    shutil.rmtree(destino_final)

            # Criar diretório final
            os.makedirs(destino_final, exist_ok=True)

            # Copiar arquivos XML
            extracted_dir = os.path.join(job_dir, "extracted")
            for arquivo in os.listdir(extracted_dir):
                if arquivo.endswith('.xml'):
                    shutil.copy2(
                        os.path.join(extracted_dir, arquivo),
                        os.path.join(destino_final, arquivo)
                    )

            logger.info(f"Arquivos copiados para destino final: {destino_final}")

        # Finalizar job - agora vai remover o job em vez de movê-lo para completed
        finalizar_job(job_dir)
        return True

    except Exception as e:
        logger.error(f"Erro ao mover para destino final: {e}", exc_info=True)
        mover_para_falhas(job_dir, f"erro_mover_{str(e).replace(' ', '_')[:50]}")
        return False

def criar_pastas_empresas_destino(diretorio_destino):
    """Cria estrutura de pastas por empresa no destino final"""
    logger.info(f"Criando pastas de empresas no destino: {diretorio_destino}")
    empresas = listar_empresas()

    if not os.path.exists(diretorio_destino):
        os.makedirs(diretorio_destino, exist_ok=True)

    # Criar pasta para erros se não existir
    erros_path = os.path.join(diretorio_destino, "ERROS")
    os.makedirs(erros_path, exist_ok=True)

    # Criar pasta para cada empresa
    for empresa in empresas:
        nome_empresa, ie_empresa = empresa.get("apelido"), empresa.get("inscricao_estadual")
        pasta_empresa = os.path.join(diretorio_destino, f"{nome_empresa}_{ie_empresa}")
        if not os.path.exists(pasta_empresa):
            os.makedirs(pasta_empresa, exist_ok=True)
            logger.info(f"Criada pasta para empresa {nome_empresa}")

    logger.info(f"Estrutura de diretórios criada em {diretorio_destino}")

# ============================================
# WORKFLOW FUNCTIONS
# ============================================

def processar_arquivo_zip_existente(arquivo_zip):
    """Processa um arquivo ZIP existente na pasta incoming"""
    caminho_zip = os.path.join(ESTRUTURA_DIRETORIOS["incoming"], arquivo_zip)

    # Verificar se o arquivo existe e tem tamanho > 0
    if not os.path.exists(caminho_zip) or os.path.getsize(caminho_zip) == 0:
        logger.warning(f"Arquivo {arquivo_zip} não existe ou está vazio")
        return False

    logger.info(f"Processando arquivo ZIP existente: {arquivo_zip}")

    # Criar job de processamento
    job_dir = criar_job_processamento(arquivo_zip)

    # Processar o ZIP
    resultado = processar_zip(os.path.join(job_dir, arquivo_zip), job_dir)

    # Se processado com sucesso, excluir o arquivo original da pasta incoming
    if resultado:
        try:
            os.remove(caminho_zip)
            logger.info(f"Arquivo original {arquivo_zip} removido da pasta incoming após processamento")
        except Exception as e:
            logger.error(f"Erro ao remover arquivo original {arquivo_zip}: {e}")

    return resultado

def processar_arquivos_existentes():
    """Processa todos os arquivos existentes na pasta incoming"""
    logger.info("Processando arquivos existentes na pasta incoming")

    arquivos_zip = [arquivo for arquivo in os.listdir(ESTRUTURA_DIRETORIOS["incoming"])
                   if arquivo.endswith('.zip')]

    if not arquivos_zip:
        logger.info("Nenhum arquivo ZIP encontrado na pasta incoming")
        return

    logger.info(f"Encontrados {len(arquivos_zip)} arquivos ZIP para processar")

    for arquivo_zip in arquivos_zip:
        try:
            processar_arquivo_zip_existente(arquivo_zip)
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {arquivo_zip}: {e}", exc_info=True)
            # Continuar com o próximo arquivo

def heartbeat():
    """Registra que o serviço está funcionando"""
    global ultimo_heartbeat
    agora = time.time()
    if agora - ultimo_heartbeat > INTERVALO_HEARTBEAT:
        logger.info("Serviço de monitoramento funcionando - nenhuma atividade recente")
        ultimo_heartbeat = agora

# ============================================
# MONITORING
# ============================================

class ArquivoHandler(FileSystemEventHandler):
    """Manipulador de eventos para novos arquivos"""
    def on_created(self, event):
        global ultimo_heartbeat
        ultimo_heartbeat = time.time()

        # Verificar se é um arquivo e não uma pasta
        if not event.is_directory and event.src_path.endswith('.zip'):
            arquivo = os.path.basename(event.src_path)

            # Verificar se o arquivo está na pasta incoming
            if os.path.dirname(event.src_path) != ESTRUTURA_DIRETORIOS["incoming"]:
                return

            logger.info(f"Novo arquivo detectado: {arquivo}")

            # Dar um pequeno tempo para garantir que o arquivo está completamente copiado
            time.sleep(2)

            # Processar o novo arquivo
            try:
                processar_arquivo_zip_existente(arquivo)
            except Exception as e:
                logger.error(f"Erro ao processar o novo arquivo {arquivo}: {e}", exc_info=True)

def configurar_tratamento_sinais():
    """Configura handlers para sinais do sistema operacional"""
    def signal_handler(sig, frame):
        global running
        logger.info(f"Sinal recebido: {sig}. Encerrando...")
        running = False

    # Capturar SIGINT (Ctrl+C) e SIGTERM
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def iniciar_monitoramento():
    """Inicia o monitoramento da pasta incoming"""
    logger.info(f"Iniciando monitoramento da pasta: {ESTRUTURA_DIRETORIOS['incoming']}")

    # Criar o observador
    observer = Observer()
    observer.schedule(
        ArquivoHandler(),
        path=ESTRUTURA_DIRETORIOS["incoming"],
        recursive=False
    )
    observer.start()

    try:
        while running:
            # Verificação de saúde periódica
            heartbeat()
            time.sleep(1)
    except Exception as e:
        logger.error(f"Erro durante o monitoramento: {e}", exc_info=True)
    finally:
        observer.stop()
        observer.join()
        logger.info("Monitoramento finalizado")

def main():
    """Função principal"""
    execucao_id = f"GERENCIADOR-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    logger.info("=" * 80)
    logger.info(f"INICIANDO SERVIÇO DE MONITORAMENTO E GERENCIAMENTO DE ARQUIVOS NFC-e ({execucao_id})")
    logger.info("=" * 80)

    # Configuração para captura de sinais
    configurar_tratamento_sinais()

    # Configurar estrutura de diretórios
    configurar_diretorios()

    # Criar estrutura de pastas de destino
    criar_pastas_empresas_destino(DIRETORIO_FINAL)

    # Verificar se há processamentos pendentes e tentar recuperá-los
    verificar_processamentos_pendentes()

    # Processar arquivos existentes na pasta incoming
    processar_arquivos_existentes()

    # Iniciar o monitoramento contínuo
    iniciar_monitoramento()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Serviço de gerenciamento interrompido pelo usuário")
    except Exception as e:
        logger.critical(f"Erro fatal no serviço de gerenciamento: {str(e)}", exc_info=True)
        sys.exit(1)