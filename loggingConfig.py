import logging
import os
import sys
from logging.handlers import RotatingFileHandler

def get_execution_path():
    """Retorna o diretório onde o script foi executado"""
    return os.path.dirname(os.path.abspath(sys.argv[0]))

def configure_logging(auto_configure=True):
    """
    Configuração centralizada de logging que:
    - Cria pasta logs/ no diretório de execução
    - Usa nome do arquivo principal como nome do log
    - Evita duplicação de handlers
    - Suporta configuração de nível através da variável de ambiente LOG_LEVEL
    """
    # Obtém o caminho de execução e nome do arquivo principal
    execution_path = get_execution_path()
    log_dir = os.path.join(execution_path, 'logs')
    main_file = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    log_file = os.path.join(log_dir, f'{main_file}.log')

    # Cria diretório de logs se não existir
    os.makedirs(log_dir, exist_ok=True)

    # Obtém o logger raiz
    logger = logging.getLogger()

    # Define o nível de log com base na variável de ambiente ou usa INFO como padrão
    log_level_name = os.environ.get('LOG_LEVEL', 'INFO').upper()
    log_level = getattr(logging, log_level_name, logging.INFO)
    logger.setLevel(log_level)

    # Remove todos os handlers existentes para evitar duplicação
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    # Formato padrão dos logs
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Handler para arquivo (rotativo, 20MB, 5 backups)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=20*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Handler para console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Configura níveis de log para bibliotecas externas
    logging.getLogger('selenium').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('psycopg2').setLevel(logging.WARNING)

    # Reduz logs de módulos internos para diminuir verbosidade
    logging.getLogger('utils').setLevel(logging.WARNING)

    logger.info(f'Logging configurado. Arquivo: {log_file} | Nível: {log_level_name}')

def get_logger(name=None):
    """
    Retorna um logger configurado adequadamente.
    Se nenhum nome for fornecido, retorna o logger raiz.
    Se o logger não tiver handlers, configura o logging básico.
    """
    logger = logging.getLogger(name)

    # Se logging ainda não estiver configurado, configura automaticamente
    if not logger.handlers and not logging.getLogger().handlers:
        configure_logging()

    return logger

# Configura automaticamente ao importar o módulo
configure_logging()