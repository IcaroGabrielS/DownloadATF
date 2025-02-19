import os, zipfile, json, shutil, logging
import xml.etree.ElementTree as ET
from datetime import datetime
from util import listar_empresas, carregar_datas_solicitacoes

NAMESPACE = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

# Configuração do logging
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
    diretorios = constantes["UTIL"]["DIR"]

def gerar_nome_unico(caminho):
    contador = 1
    nome_base, extensao = os.path.splitext(caminho)
    while os.path.exists(caminho):
        caminho = f"{nome_base} ({contador}){extensao}"
        contador += 1
    return caminho

def parse_xml(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        ie_empresa = root.find('.//nfe:emit/nfe:IE', NAMESPACE).text
        return ie_empresa
    except Exception as e:
        logger.error(f"Erro ao processar o arquivo XML {file_path}: {e}")
        return None

def descompactar_arquivos(pasta):
    arquivos_zip = [arquivo for arquivo in os.listdir(pasta) if arquivo.endswith('.zip')]
    for i, arquivo in enumerate(arquivos_zip, start=1):
        caminho_completo = os.path.join(pasta, arquivo)
        if arquivo.startswith('NFCE_XML'):
            subpasta = os.path.join(pasta, str(i))
            os.makedirs(subpasta, exist_ok=True)
            with zipfile.ZipFile(caminho_completo, 'r') as zip_ref:
                for item in zip_ref.namelist():
                    caminho_extraido = os.path.join(subpasta, item)
                    if os.path.exists(caminho_extraido):
                        caminho_extraido = gerar_nome_unico(caminho_extraido)
                    with open(caminho_extraido, 'wb') as f:
                        f.write(zip_ref.read(item))
            os.remove(caminho_completo)
            logger.info(f"Descompactado e removido arquivo: {arquivo}")
        else:
            os.remove(caminho_completo)
            logger.info(f"Removido arquivo não conforme: {arquivo}")

    for root, dirs, files in os.walk(pasta):
        for arquivo in files:
            caminho_completo = os.path.join(root, arquivo)
            if not arquivo.endswith('.xml'):
                os.remove(caminho_completo)
                logger.info(f"Removido arquivo não XML: {arquivo}")

def renomear_diretorios_por_xml(diretorio_principal):
    data_ini, data_fim = carregar_datas_solicitacoes()
    for subdir in os.listdir(diretorio_principal):
        subdir_path = os.path.join(diretorio_principal, subdir)
        if os.path.isdir(subdir_path):
            xml_files = [f for f in os.listdir(subdir_path) if f.upper().endswith('.XML') and f.startswith('NFCE_')]
            if xml_files:
                primeiro_xml = os.path.join(subdir_path, xml_files[0])
                try:
                    ie_empresa = parse_xml(primeiro_xml)
                    if ie_empresa:
                        novo_nome = f"{data_ini}_{data_fim}_{ie_empresa}"
                        novo_subdir_path = os.path.join(diretorio_principal, novo_nome)
                        os.rename(subdir_path, novo_subdir_path)
                        logger.info(f"Renomeado diretório {subdir} para {novo_nome}")
                except Exception as e:
                    logger.error(f"Erro ao processar {primeiro_xml}: {e}")

def mover_subpastas_para_destino(diretorio_principal, destino):
    if not os.path.exists(destino):
        os.makedirs(destino)
    for subdir in os.listdir(diretorio_principal):
        subdir_path = os.path.join(diretorio_principal, subdir)
        if os.path.isdir(subdir_path):
            try:
                *_, ie = subdir.rsplit('_', 1)
            except ValueError:
                logger.error(f"Formato inválido: {subdir}")
                continue
            # Localiza subpasta no destino que termina com a mesma IE
            matching_folder = None
            for folder in os.listdir(destino):
                folder_path = os.path.join(destino, folder)
                if os.path.isdir(folder_path) and folder.endswith(f"_{ie}"):
                    matching_folder = folder
                    break
            if matching_folder:
                destino_final = os.path.join(destino, matching_folder, subdir)
                shutil.move(subdir_path, destino_final)
                logger.info(f"Movido: {subdir} -> {destino_final}")
            else:
                logger.error(f"Não foi encontrada subpasta para {subdir}")

def criar_pastas_empresas_no_destino(diretorio_destino):
    empresas = listar_empresas()
    if not os.path.exists(diretorio_destino):
        os.makedirs(diretorio_destino)
    for empresa in empresas:
        nome_empresa, ie_empresa = empresa.get("apelido"), empresa.get("inscricao_estadual")
        pasta_empresa = os.path.join(diretorio_destino, f"{nome_empresa}_{ie_empresa}")
        if not os.path.exists(pasta_empresa):
            os.makedirs(pasta_empresa)
            logger.info(f"Criada pasta para empresa {nome_empresa}")

def executar_processo_manage_files_nfce(diretorio_principal = diretorios["DIRETORIO_DONWLOADS"], diretorio_destino = diretorios["DIRETORIO_FINAL"]):
    logger.info("Iniciando processo de gerenciamento de arquivos NFC-e...")
    criar_pastas_empresas_no_destino(diretorio_destino)
    descompactar_arquivos(diretorio_principal)
    renomear_diretorios_por_xml(diretorio_principal)
    mover_subpastas_para_destino(diretorio_principal, diretorio_destino)
    logger.info("Processo de gerenciamento de arquivos NFC-e concluído.")

if __name__ == "__main__":
    executar_processo_manage_files_nfce()