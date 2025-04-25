import logging
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from utils import (
    conectar_mysql, 
    conectar_postgres, 
    garantir_diretorios
)

# Load environment variables
load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("solicitacoes.log"),
        logging.StreamHandler()
    ]
)

def criar_estrutura_banco():
    """Verifica e cria a estrutura do banco de dados se não existir."""
    conexao = conectar_postgres()
    if not conexao:
        return False
    
    cursor = conexao.cursor()
    
    try:
        # Verificar se tabela empresas existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'empresas'
            );
        """)
        
        if not cursor.fetchone()[0]:
            logging.info("Criando estrutura do banco de dados...")
            
            # Criar tabela empresas (apenas com os campos necessários)
            cursor.execute("""
                CREATE TABLE empresas (
                    inscricao_estadual VARCHAR(20) PRIMARY KEY,
                    apelido VARCHAR(100),
                    uf VARCHAR(2) DEFAULT 'PB',
                    status_empresa CHAR(1) DEFAULT 'A',
                    ultima_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Criar tabela solicitacoes com solicitado como INTEGER
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS solicitacoes (
                    id SERIAL PRIMARY KEY,
                    inscricao_estadual VARCHAR(20) NOT NULL,
                    data_solicitacao DATE NOT NULL,
                    tipo VARCHAR(10) NOT NULL,
                    data_ini VARCHAR(10) NOT NULL,
                    data_fim VARCHAR(10) NOT NULL,
                    horario TIMESTAMP,
                    link TEXT,
                    solicitado INTEGER DEFAULT 0,  -- Campo modificado para INTEGER com valor padrão 0
                    baixado BOOLEAN DEFAULT FALSE,
                    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    atualizado_em TIMESTAMP,
                    
                    CONSTRAINT uk_solicitacao_empresa_dia_tipo UNIQUE (inscricao_estadual, data_solicitacao, tipo),
                    CONSTRAINT fk_solicitacao_empresa FOREIGN KEY (inscricao_estadual) 
                        REFERENCES empresas (inscricao_estadual)
                );
            """)
            
            # Criar tabela arquivos_xml
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS arquivos_xml (
                    id SERIAL PRIMARY KEY,
                    solicitacao_id INTEGER NOT NULL,
                    nome_arquivo VARCHAR(255) NOT NULL,
                    conteudo TEXT NOT NULL,
                    hash_arquivo VARCHAR(64),
                    importado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    CONSTRAINT fk_arquivo_solicitacao FOREIGN KEY (solicitacao_id) 
                        REFERENCES solicitacoes (id)
                );
            """)
            
            # Criar índices
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_solicitacoes_inscricao_data ON solicitacoes(inscricao_estadual, data_solicitacao);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_solicitacoes_status ON solicitacoes(solicitado, baixado);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_arquivos_solicitacao ON arquivos_xml(solicitacao_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_arquivos_nome ON arquivos_xml(nome_arquivo);")
            
            conexao.commit()
            logging.info("Estrutura do banco criada com sucesso!")
        
        return True
        
    except Exception as erro:
        conexao.rollback()
        logging.error(f"Erro ao criar estrutura do banco: {erro}")
        return False
    finally:
        cursor.close()
        conexao.close()

def sincronizar_empresas():
    """Importa ou atualiza as empresas do MySQL para o PostgreSQL."""
    # Conectar ao MySQL e obter empresas
    conexao_mysql = conectar_mysql()
    if not conexao_mysql or not conexao_mysql.is_connected():
        logging.error("Falha ao conectar ao banco de dados MySQL")
        return 0
    
    try:
        cursor_mysql = conexao_mysql.cursor()
        cursor_mysql.execute(os.environ.get('QUERY_LISTAR_EMPRESAS'))
        empresas = cursor_mysql.fetchall()
        logging.info(f"Encontradas {len(empresas)} empresas no MySQL")
    except Exception as erro:
        logging.error(f"Erro ao buscar empresas no MySQL: {erro}")
        return 0
    finally:
        if conexao_mysql.is_connected():
            cursor_mysql.close()
            conexao_mysql.close()
    
    # Conectar ao PostgreSQL e sincronizar empresas
    conexao_pg = conectar_postgres()
    if not conexao_pg:
        return 0
    
    empresas_atualizadas = 0
    empresas_inseridas = 0
    
    try:
        cursor_pg = conexao_pg.cursor()
        
        for apelido, inscricao_estadual in empresas:
            # Verificar se a empresa já existe
            cursor_pg.execute(
                "SELECT 1 FROM empresas WHERE inscricao_estadual = %s",
                (inscricao_estadual,)
            )
            
            if cursor_pg.fetchone():
                # Atualiza empresa existente
                cursor_pg.execute(
                    """
                    UPDATE empresas 
                    SET apelido = %s, ultima_atualizacao = CURRENT_TIMESTAMP
                    WHERE inscricao_estadual = %s
                    """,
                    (apelido, inscricao_estadual)
                )
                empresas_atualizadas += 1
            else:
                # Insere nova empresa
                cursor_pg.execute(
                    """
                    INSERT INTO empresas 
                    (inscricao_estadual, apelido)
                    VALUES (%s, %s)
                    """,
                    (inscricao_estadual, apelido)
                )
                empresas_inseridas += 1
        
        conexao_pg.commit()
        logging.info(f"Sincronização concluída: {empresas_inseridas} empresas inseridas, {empresas_atualizadas} atualizadas")
    
    except Exception as erro:
        conexao_pg.rollback()
        logging.error(f"Erro durante sincronização de empresas: {erro}")
        return 0
    
    finally:
        cursor_pg.close()
        conexao_pg.close()
    
    return empresas_inseridas + empresas_atualizadas

def criar_solicitacoes_cinco_dias_atras():
    """
    Cria solicitações com data de 5 dias atrás para todas as empresas.
    Ex: Se hoje é 24/04/2025, cria solicitações com data 19/04/2025.
    """
    # Calcular data de 5 dias atrás
    hoje = datetime.now()
    cinco_dias_atras = hoje - timedelta(days=5)
    
    # Data a ser usada nas solicitações
    data_solicitacao = cinco_dias_atras.date()
    
    # Formatando as datas no formato DD/MM/YYYY para os campos data_ini e data_fim
    data_formatada = cinco_dias_atras.strftime('%d/%m/%Y')
    
    logging.info(f"Criando solicitações para a data: {data_formatada}")
    
    # Conectar ao PostgreSQL
    conexao_pg = conectar_postgres()
    if not conexao_pg:
        return 0
    
    solicitacoes_criadas = 0
    
    try:
        cursor_pg = conexao_pg.cursor()
        
        # Buscar todas as empresas ativas
        cursor_pg.execute(
            "SELECT inscricao_estadual FROM empresas WHERE status_empresa = 'A'"
        )
        empresas = cursor_pg.fetchall()
        
        for (inscricao_estadual,) in empresas:
            # Verificar se já existe solicitação para esta data
            cursor_pg.execute(
                """
                SELECT id FROM solicitacoes 
                WHERE inscricao_estadual = %s AND data_solicitacao = %s AND tipo = 'NFCE'
                """,
                (inscricao_estadual, data_solicitacao)
            )
            
            if cursor_pg.fetchone() is None:
                # Criar nova solicitação com solicitado = 0 (contador de tentativas)
                cursor_pg.execute(
                    """
                    INSERT INTO solicitacoes 
                    (inscricao_estadual, data_solicitacao, tipo, data_ini, data_fim, solicitado, baixado)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
                    """,
                    (
                        inscricao_estadual,
                        data_solicitacao,
                        "NFCE",
                        data_formatada,
                        data_formatada,
                        0,  # CORRIGIDO: Agora enviando inteiro (0) em vez de booleano (False)
                        False
                    )
                )
                
                solicitacoes_criadas += 1
        
        conexao_pg.commit()
        logging.info(f"Processo concluído: {solicitacoes_criadas} novas solicitações criadas para {data_formatada}")
    
    except Exception as erro:
        conexao_pg.rollback()
        logging.error(f"Erro durante criação de solicitações: {erro}")
        return 0
    
    finally:
        cursor_pg.close()
        conexao_pg.close()
    
    return solicitacoes_criadas

def executar_processo_completo():
    """Executa todo o processo de importação e criação de solicitações."""
    # 1. Verificar e criar estrutura do banco de dados
    if not criar_estrutura_banco():
        logging.error("Falha ao criar estrutura do banco de dados. Abortando processo.")
        return
    
    # 2. Sincronizar empresas do MySQL para o PostgreSQL
    empresas_sincronizadas = sincronizar_empresas()
    logging.info(f"Total de empresas sincronizadas: {empresas_sincronizadas}")
    
    # 3. Criar solicitações com data de 5 dias atrás
    solicitacoes_criadas = criar_solicitacoes_cinco_dias_atras()
    logging.info(f"Total de solicitações criadas: {solicitacoes_criadas}")
    
    logging.info("Processo finalizado com sucesso!")

if __name__ == "__main__":
    # Registrar início do processo
    logging.info("=" * 80)
    logging.info("INICIANDO PROCESSO DE CRIAÇÃO DE SOLICITAÇÕES")
    logging.info("=" * 80)
    
    # Executar processo completo
    executar_processo_completo()