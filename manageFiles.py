import os, re, zipfile, json, shutil, logging
import xml.etree.ElementTree as ET
from dateutil import parser
from util import listar_empresas


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

with open("json_files/constantes.json", "r", encoding="utf-8") as arquivo_constantes:
    constantes = json.load(arquivo_constantes)
    diretorios = constantes["UTIL"]["DIR"]

def gerar_nome_unico_se_existir(caminho):
    contador = 1
    nome_base, extensao = os.path.splitext(caminho)
    while os.path.exists(caminho):
        caminho = f"{nome_base} ({contador}){extensao}"
        contador += 1
    return caminho

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
            return parser.isoparse(dh_emi.text) if dh_emi is not None else None
        else:
            logger.error(f"Tipo inválido: {tipo}. Use 'ie' ou 'data'.")
            return None
    except Exception as e:
        logger.error(f"Erro ao processar o XML {xml_path}: {e}")
        return None

def descompactar_arquivos_zip(pasta):
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
                        caminho_extraido = gerar_nome_unico_se_existir(caminho_extraido)
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

def renomear_pastas_por_ie(diretorio_principal):
    for subdir in os.listdir(diretorio_principal):
        subdir_path = os.path.join(diretorio_principal, subdir)
        if os.path.isdir(subdir_path):
            xml_files = [f for f in os.listdir(subdir_path) if f.upper().endswith('.XML') and f.startswith('NFCE_')]
            if xml_files:
                datas = []
                ies = set()
                for xml_file in xml_files:
                    xml_path = os.path.join(subdir_path, xml_file)
                    data_emissao = extrair_dado_xml(xml_path, "ie")
                    if data_emissao: datas.append(data_emissao)
                    ie_empresa = extrair_dado_xml(xml_path, "data")
                    if ie_empresa: ies.add(ie_empresa)
                if datas: 
                    data_ini = min(datas).strftime('%Y%m%d')
                    data_fim = max(datas).strftime('%Y%m%d')
                else: data_ini = data_fim = '00000000'
                if len(ies) > 1: novo_nome = f"ERR_{data_ini}_{data_fim}_" + "_".join(ies)
                else: novo_nome = f"{data_ini}_{data_fim}_{next(iter(ies), 'SEM_IE')}"
                novo_subdir_path = os.path.join(diretorio_principal, novo_nome)
                for i in range(1, 6):
                    if not os.path.exists(novo_subdir_path): break
                    novo_nome_com_sufixo = f"{novo_nome} ({i})"
                    novo_subdir_path = os.path.join(diretorio_principal, novo_nome_com_sufixo)
                if os.path.exists(novo_subdir_path):
                    logger.error(f"Não foi possível renomear a pasta {subdir}. Limite de tentativas atingido.")
                else:
                    os.rename(subdir_path, novo_subdir_path)
                    logger.info(f"Renomeado diretório {subdir} para {os.path.basename(novo_subdir_path)}")


def mover_pastas_para_destino_final(diretorio_principal, destino):
    erros_path = os.path.join(destino, "ERROS")
    if not os.path.exists(erros_path):
        os.makedirs(erros_path)
        logger.info(f"Subpasta 'ERROS' criada em {erros_path}")

    for subdir in os.listdir(diretorio_principal):
        subdir_path = os.path.join(diretorio_principal, subdir)
        if os.path.isdir(subdir_path):
            if not re.match(r"^[0-9_]+$", subdir):
                destino_final = os.path.join(erros_path, subdir)
                shutil.move(subdir_path, destino_final)
                logger.info(f"Movido para ERROS (caracteres inválidos): {subdir} -> {destino_final}")
                continue
            if subdir.count("_") != 2:
                destino_final = os.path.join(erros_path, subdir)
                shutil.move(subdir_path, destino_final)
                logger.info(f"Movido para ERROS (formato inválido): {subdir} -> {destino_final}")
                continue
            try: *_, ie = subdir.rsplit('_', 1)
            except ValueError:
                logger.error(f"Formato inválido: {subdir}")
                continue
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
            else: logger.error(f"Não foi encontrada subpasta para {subdir}")

def criar_pastas_empresas_destino(diretorio_destino):
    empresas = listar_empresas()
    if not os.path.exists(diretorio_destino):
        os.makedirs(diretorio_destino)
    for empresa in empresas:
        nome_empresa, ie_empresa = empresa.get("apelido"), empresa.get("inscricao_estadual")
        pasta_empresa = os.path.join(diretorio_destino, f"{nome_empresa}_{ie_empresa}")
        if not os.path.exists(pasta_empresa):
            os.makedirs(pasta_empresa)
            logger.info(f"Criada pasta para empresa {nome_empresa}")

def executar_processo_gerenciar_arquivos_nfce(diretorio_principal = diretorios["DIRETORIO_DONWLOADS"], diretorio_destino = diretorios["DIRETORIO_FINAL"]):
    logger.info("Iniciando processo de gerenciamento de arquivos NFC-e...")
    criar_pastas_empresas_destino(diretorio_destino)
    descompactar_arquivos_zip(diretorio_principal)
    renomear_pastas_por_ie(diretorio_principal)
    mover_pastas_para_destino_final(diretorio_principal, diretorio_destino)
    logger.info("Processo de gerenciamento de arquivos NFC-e concluído.")

if __name__ == "__main__":
    executar_processo_gerenciar_arquivos_nfce()