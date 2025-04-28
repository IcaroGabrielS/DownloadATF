import time, logging, os
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
from utils import (
    conectar_postgres,
    iniciar_navegador_selenoid,
    autenticar_sefaz,
    acessar_pagina,
    verificar_downloads_em_progresso,
    clicar_elemento
)

load_dotenv()

# Configurar diretórios de log
os.makedirs("logs", exist_ok=True)

# Configurar logging
from logging.handlers import RotatingFileHandler
MAX_LOG_SIZE = 220 * 1024 * 1024  # 220 MB
logging.getLogger().setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_file = "logs/baixar_arquivos.log"
file_handler = RotatingFileHandler(log_file, maxBytes=MAX_LOG_SIZE, backupCount=5, encoding='utf-8')
file_handler.setFormatter(log_formatter)
logging.getLogger().addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logging.getLogger().addHandler(console_handler)

# Configurar diretório de downloads
DIRETORIO_DOWNLOADS = "/home/desenvolvimento/DownloadATF/NFCE_XML_TEMP/incoming"
# Garantir que o diretório existe com as permissões corretas
os.makedirs(DIRETORIO_DOWNLOADS, exist_ok=True)
os.chmod(DIRETORIO_DOWNLOADS, 0o777)  # Dar todas as permissões para garantir acesso

# ============================================
# FUNÇÕES DE BANCO DE DADOS E SOLICITAÇÕES
# ============================================

def obter_solicitacoes_com_link():
    """Busca solicitações que têm link mas ainda não foram baixadas"""
    conexao = conectar_postgres()
    if not conexao:
        return []
    
    try:
        cursor = conexao.cursor()
        # Buscar solicitações com link disponível mas não baixadas
        cursor.execute("""
            SELECT id, inscricao_estadual, link
            FROM solicitacoes 
            WHERE link IS NOT NULL AND link != '' AND baixado = FALSE AND tipo = 'NFCE'
            ORDER BY data_solicitacao
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
        
        logging.info(f"Encontradas {len(solicitacoes)} solicitações com link pendentes de download")
        return solicitacoes
    
    except Exception as erro:
        logging.error(f"Erro ao buscar solicitações: {erro}")
        if conexao:
            conexao.close()
        return []

def marcar_como_baixado(id_solicitacao):
    """Marca a solicitação como baixada no banco de dados"""
    conexao = conectar_postgres()
    if not conexao:
        logging.error(f"Falha ao conectar ao PostgreSQL para marcar solicitação {id_solicitacao} como baixada")
        return False
    
    try:
        cursor = conexao.cursor()
        cursor.execute("""
            UPDATE solicitacoes 
            SET baixado = TRUE, atualizado_em = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (id_solicitacao,))
        
        conexao.commit()
        cursor.close()
        conexao.close()
        
        logging.info(f"Solicitação {id_solicitacao} marcada como baixada")
        return True
    
    except Exception as erro:
        logging.error(f"Erro ao marcar solicitação {id_solicitacao} como baixada: {erro}")
        if conexao:
            conexao.rollback()
            conexao.close()
        return False

# ============================================
# FUNÇÕES DE DOWNLOAD
# ============================================

def realizar_download(navegador, solicitacao):
    """Acessa o link e inicia o download do arquivo"""
    try:
        # Acessa o link para download
        acessar_pagina(navegador, solicitacao["link"])
        
        # Clica nos elementos de download
        espera_curta = int(os.environ.get("ESPERA_CURTA", 2))
        if clicar_elemento(navegador, os.environ.get("XPATH_IMAGEM_ANEXO"), espera_curta) and \
           clicar_elemento(navegador, os.environ.get("XPATH_LINK_DOWNLOAD"), espera_curta):
            logging.info(f"Download iniciado para solicitação {solicitacao['id']}")
            # Aguardar um pouco mais para o download iniciar
            time.sleep(10)  # Aumentado para dar mais tempo ao download
            return True
        else:
            logging.error(f"Falha ao clicar nos elementos para download da solicitação {solicitacao['id']}")
            return False
    except Exception as e:
        logging.error(f"Erro ao realizar download para solicitação {solicitacao['id']}: {str(e)}")
        return False

# ============================================
# FUNÇÃO PRINCIPAL
# ============================================

def executar_processo_downloads():
    """Função principal que executa o processo de download dos arquivos"""
    logging.info("Iniciando processo de download dos arquivos")
    
    logging.info(f"Diretório de downloads configurado: {DIRETORIO_DOWNLOADS}")
    
    # Verificar permissões do diretório
    try:
        test_file_path = os.path.join(DIRETORIO_DOWNLOADS, "test_write_permission.tmp")
        with open(test_file_path, 'w') as f:
            f.write("Teste de permissão de escrita")
        os.remove(test_file_path)
        logging.info(f"Permissão de escrita verificada no diretório {DIRETORIO_DOWNLOADS}")
    except Exception as e:
        logging.error(f"ERRO DE PERMISSÃO: Não é possível escrever no diretório {DIRETORIO_DOWNLOADS}: {str(e)}")
        return
    
    # Carrega as solicitações pendentes de download do banco de dados
    solicitacoes = obter_solicitacoes_com_link()
    
    if not solicitacoes:
        logging.info("Não há solicitações pendentes de download")
        return
    
    navegador = None
    try:
        # Inicia o navegador com Selenoid
        browser_type = os.environ.get("SELENOID_BROWSER", "chrome")
        navegador = iniciar_navegador_selenoid(DIRETORIO_DOWNLOADS, browser_type)
        
        if navegador and autenticar_sefaz(navegador):
            downloads_realizados = 0
            
            # Processa cada solicitação com link disponível
            for solicitacao in solicitacoes:
                if realizar_download(navegador, solicitacao):
                    # Se o download foi iniciado com sucesso, marca como baixado no banco
                    if marcar_como_baixado(solicitacao["id"]):
                        downloads_realizados += 1
                        logging.info(f"Solicitação {solicitacao['id']} marcada como baixada no banco")
                    else:
                        logging.error(f"Falha ao atualizar status no banco para solicitação {solicitacao['id']}")
                
                # Listar arquivos após cada download para verificar
                try:
                    logging.info(f"Arquivos no diretório de download após solicitação {solicitacao['id']}:")
                    for arquivo in os.listdir(DIRETORIO_DOWNLOADS):
                        logging.info(f"  - {arquivo}")
                except Exception as e:
                    logging.error(f"Erro ao listar arquivos: {str(e)}")
                
                # Espera entre os downloads para evitar sobrecarga
                time.sleep(15)  # Aumentado para dar mais tempo
            
            logging.info(f"Processo de downloads concluído. {downloads_realizados} arquivos baixados.")
        else:
            logging.error("Falha na autenticação ou inicialização do navegador")
            
    except Exception as e:
        logging.error(f"Erro durante a execução: {str(e)}")
    finally:
        # Aguarda downloads em andamento concluírem
        max_tentativas = int(os.environ.get("MAX_TENTATIVAS", 10))
        tentativa = 0
        
        while verificar_downloads_em_progresso(DIRETORIO_DOWNLOADS) and tentativa < max_tentativas:
            time.sleep(2)
            tentativa += 1
            
        # Verificar se há arquivos no diretório de downloads
        try:
            arquivos = os.listdir(DIRETORIO_DOWNLOADS)
            logging.info(f"Total de {len(arquivos)} arquivos no diretório de download após o processo")
        except Exception as e:
            logging.error(f"Erro ao listar arquivos finais: {str(e)}")
            
        if navegador:
            navegador.quit()
            
        logging.info("Processo de download finalizado")
    
if __name__ == "__main__":
    logging.info("=" * 80)
    logging.info("INICIANDO PROCESSO DE DOWNLOAD DE ARQUIVOS XML")
    logging.info("=" * 80)
    executar_processo_downloads()