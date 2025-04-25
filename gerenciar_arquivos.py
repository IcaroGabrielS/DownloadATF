import os, re, zipfile, shutil, logging, mysql.connector, sys
import xml.etree.ElementTree as ET
from mysql.connector import Error
from dateutil import parser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Função para obter o diretório de execução
def obter_diretorio_execucao():
    """Retorna o diretório onde o programa está sendo executado (funciona para script ou executável)"""
    if getattr(sys, 'frozen', False):
        # Executando como executável
        return os.path.dirname(sys.executable)
    else:
        # Executando como script
        return os.path.dirname(os.path.abspath(__file__))

# Constantes incorporadas diretamente no código
DATABASE_CONFIG = {
    "HOST": "10.0.100.37",
    "USER": "externo",
    "PASSWORD": "externo",
    "DATABASE": "transmissoes"
}

QUERY_LISTAR_EMPRESAS = "SELECT apelido, inscricao_estadual FROM transmissoes.empresas WHERE status_empresa = 'A' AND inscricao_estadual IS NOT NULL AND inscricao_estadual != 0 AND uf = 'PB'"

# Definindo diretórios com base no diretório de execução
DIRETORIO_EXECUCAO = obter_diretorio_execucao()
DIRETORIOS = {
    "DIRETORIO_DONWLOADS": os.path.join(DIRETORIO_EXECUCAO, "NFCE_XML_TEMP"),
    "DIRETORIO_FINAL": "Z:\\Fiscal\\18. Agendador de Downloads\\NFCE"
}

def gerar_nome_unico_se_existir(caminho):
    contador = 1
    nome_base, extensao = os.path.splitext(caminho)
    while os.path.exists(caminho):
        caminho = f"{nome_base} ({contador}){extensao}"
        contador += 1
    return caminho

def conectar_banco():
    logging.info("Util-conectar_banco - Tentando conectar ao banco de dados")
    try:
        conexao = mysql.connector.connect(
            host=DATABASE_CONFIG["HOST"],
            user=DATABASE_CONFIG["USER"],
            password=DATABASE_CONFIG["PASSWORD"],
            database=DATABASE_CONFIG["DATABASE"]
        )
        if conexao.is_connected():
            logging.info("Util-conectar_banco - Conexão estabelecida com sucesso")
        return conexao
    except Error as erro:
        logging.error(f"Util-conectar_banco - Erro ao conectar ao MySQL: {erro}")
        return None

def listar_empresas():
    logging.info("Util-listar_empresas - Listando empresas do banco de dados")
    conexao = conectar_banco()
    if conexao and conexao.is_connected():
        cursor = conexao.cursor()
        logging.info("Util-listar_empresas - Executando query para listar empresas")
        cursor.execute(QUERY_LISTAR_EMPRESAS)
        resultados = cursor.fetchall()
        colunas = [desc[0] for desc in cursor.description]
        cursor.close()
        conexao.close()
        dados = [dict(zip(colunas, linha)) for linha in resultados]
        logging.info(f"Util-listar_empresas - {len(dados)} empresas encontradas")
        return dados
    logging.error("Util-listar_empresas - Falha ao conectar ao banco de dados")
    return []

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
    logger.info(f"ManageFiles - Extraindo dado do XML: {xml_path}, Tipo: {tipo}")
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
            logger.error(f"ManageFiles - Tipo inválido: {tipo}. Use 'ie' ou 'data'.")
            return None
    except Exception as e:
        logger.error(f"ManageFiles - Erro ao processar o XML {xml_path}: {e}")
        return None

def descompactar_arquivos_zip(pasta):
    logger.info(f"ManageFiles - Descompactando arquivos ZIP na pasta: {pasta}")
    arquivos_zip = [arquivo for arquivo in os.listdir(pasta) if arquivo.endswith('.zip')]
    for i, arquivo in enumerate(arquivos_zip, start=1):
        caminho_completo = os.path.join(pasta, arquivo)
        logger.info(f"ManageFiles - Processando arquivo ZIP: {arquivo}")
        if arquivo.startswith('NFCE_XML'):
            subpasta = os.path.join(pasta, str(i))
            os.makedirs(subpasta, exist_ok=True)
            logger.info(f"ManageFiles - Criando subpasta: {subpasta}")
            with zipfile.ZipFile(caminho_completo, 'r') as zip_ref:
                for item in zip_ref.namelist():
                    caminho_extraido = os.path.join(subpasta, item)
                    if os.path.exists(caminho_extraido):
                        caminho_extraido = gerar_nome_unico_se_existir(caminho_extraido)
                    with open(caminho_extraido, 'wb') as f:
                        f.write(zip_ref.read(item))
            os.remove(caminho_completo)
            logger.info(f"ManageFiles - Descompactado e removido arquivo: {arquivo}")
        else:
            os.remove(caminho_completo)
            logger.info(f"ManageFiles - Removido arquivo não conforme: {arquivo}")

    for root, dirs, files in os.walk(pasta):
        for arquivo in files:
            caminho_completo = os.path.join(root, arquivo)
            if not arquivo.endswith('.xml'):
                os.remove(caminho_completo)
                logger.info(f"ManageFiles - Removido arquivo não XML: {arquivo}")

def renomear_pastas_por_ie(diretorio_principal):
    logger.info(f"ManageFiles - Renomeando pastas por IE no diretório: {diretorio_principal}")
    for subdir in os.listdir(diretorio_principal):
        subdir_path = os.path.join(diretorio_principal, subdir)
        if os.path.isdir(subdir_path):
            logger.info(f"ManageFiles - Processando subdiretório: {subdir}")
            xml_files = [f for f in os.listdir(subdir_path) if f.upper().endswith('.XML') and f.startswith('NFCE_')]
            if xml_files:
                datas = []
                ies = set()
                for xml_file in xml_files:
                    xml_path = os.path.join(subdir_path, xml_file)
                    logger.info(f"ManageFiles - Extraindo dados do XML: {xml_file}")
                    ie_empresa = extrair_dado_xml(xml_path, "ie")
                    data_emissao = extrair_dado_xml(xml_path, "data")
                    if data_emissao: datas.append(data_emissao)
                    if ie_empresa: ies.add(ie_empresa)
                if datas: 
                    data_ini = min(datas).strftime('%Y%m%d')
                    data_fim = max(datas).strftime('%Y%m%d')
                else: data_ini = data_fim = '00000000'
                if len(ies) > 1: novo_nome = f"ERR_{data_ini}_{data_fim}_" + "_".join(ies)
                else: novo_nome = f"{data_ini}_{data_fim}_{next(iter(ies), 'SEM_IE')}"
                novo_subdir_path = os.path.join(diretorio_principal, novo_nome)
                logger.info(f"ManageFiles - Tentando renomear {subdir_path} para {novo_subdir_path}")
                for i in range(1, 6):
                    if not os.path.exists(novo_subdir_path): break
                    novo_nome_com_sufixo = f"{novo_nome} ({i})"
                    novo_subdir_path = os.path.join(diretorio_principal, novo_nome_com_sufixo)
                if os.path.exists(novo_subdir_path):
                    logger.error(f"ManageFiles - Não foi possível renomear a pasta {subdir}. Limite de tentativas atingido.")
                else:
                    os.rename(subdir_path, novo_subdir_path)
                    logger.info(f"ManageFiles - Renomeado diretório {subdir} para {os.path.basename(novo_subdir_path)}")

def mover_pastas_para_destino_final(diretorio_principal, destino):
    logger.info(f"ManageFiles - Movendo pastas para o destino final: {destino}")
    erros_path = os.path.join(destino, "ERROS")
    if not os.path.exists(erros_path):
        os.makedirs(erros_path)
        logger.info(f"ManageFiles - Subpasta 'ERROS' criada em {erros_path}")

    for subdir in os.listdir(diretorio_principal):
        subdir_path = os.path.join(diretorio_principal, subdir)
        if os.path.isdir(subdir_path):
            logger.info(f"ManageFiles - Processando subdiretório: {subdir}")
            if not re.match(r"^[0-9_]+$", subdir):
                destino_final = os.path.join(erros_path, subdir)
                if os.path.exists(destino_final):
                    shutil.rmtree(destino_final)  # Remove existing folder
                    logger.info(f"ManageFiles - Substituindo pasta existente: {destino_final}")
                shutil.move(subdir_path, destino_final)
                logger.info(f"ManageFiles - Movido para ERROS (caracteres inválidos): {subdir} -> {destino_final}")
                continue
            if subdir.count("_") != 2:
                destino_final = os.path.join(erros_path, subdir)
                if os.path.exists(destino_final):
                    shutil.rmtree(destino_final)  # Remove existing folder
                    logger.info(f"ManageFiles - Substituindo pasta existente: {destino_final}")
                shutil.move(subdir_path, destino_final)
                logger.info(f"ManageFiles - Movido para ERROS (formato inválido): {subdir} -> {destino_final}")
                continue
            try: *_, ie = subdir.rsplit('_', 1)
            except ValueError:
                logger.error(f"ManageFiles - Formato inválido: {subdir}")
                continue
            matching_folder = None
            for folder in os.listdir(destino):
                folder_path = os.path.join(destino, folder)
                if os.path.isdir(folder_path) and folder.endswith(f"_{ie}"):
                    matching_folder = folder
                    break
            if matching_folder:
                destino_final = os.path.join(destino, matching_folder, subdir)
                if os.path.exists(destino_final):
                    shutil.rmtree(destino_final)  # Remove existing folder
                    logger.info(f"ManageFiles - Substituindo pasta existente: {destino_final}")
                shutil.move(subdir_path, destino_final)
                logger.info(f"ManageFiles - Movido: {subdir} -> {destino_final}")
            else: logger.error(f"ManageFiles - Não foi encontrada subpasta para {subdir}")

def criar_pastas_empresas_destino(diretorio_destino):
    logger.info(f"ManageFiles - Criando pastas de empresas no destino: {diretorio_destino}")
    empresas = listar_empresas()
    if not os.path.exists(diretorio_destino):
        os.makedirs(diretorio_destino)
    for empresa in empresas:
        nome_empresa, ie_empresa = empresa.get("apelido"), empresa.get("inscricao_estadual")
        pasta_empresa = os.path.join(diretorio_destino, f"{nome_empresa}_{ie_empresa}")
        if not os.path.exists(pasta_empresa):
            os.makedirs(pasta_empresa)
            logger.info(f"ManageFiles - Criada pasta para empresa {nome_empresa}")

def executar_processo_gerenciar_arquivos_nfce(diretorio_principal=DIRETORIOS["DIRETORIO_DONWLOADS"], diretorio_destino=DIRETORIOS["DIRETORIO_FINAL"]):
    logger.info("ManageFiles - Iniciando processo de gerenciamento de arquivos NFC-e...")
    if not os.path.exists(diretorio_principal):
        os.makedirs(diretorio_principal)
        logger.info(f"ManageFiles - Diretório de downloads criado: {diretorio_principal}")
    
    criar_pastas_empresas_destino(diretorio_destino)
    descompactar_arquivos_zip(diretorio_principal)
    renomear_pastas_por_ie(diretorio_principal)
    mover_pastas_para_destino_final(diretorio_principal, diretorio_destino)
    logger.info("ManageFiles - Processo de gerenciamento de arquivos NFC-e concluído.")

if __name__ == "__main__":
    executar_processo_gerenciar_arquivos_nfce()