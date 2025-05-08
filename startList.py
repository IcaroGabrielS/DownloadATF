import os
from loggingConfig import get_logger
from datetime import datetime, timedelta
from dotenv import load_dotenv
from utils import conectar_mysql, conectar_postgres

load_dotenv()
logger = get_logger(__name__)

def minha_funcao():
    logger.info("Mensagem informativa")
    logger.error("Ocorreu um erro")

def criar_estrutura_banco():
    conexao = conectar_postgres()
    if not conexao: return False
    cursor = conexao.cursor()
    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS nfce;")
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'nfce' AND table_name = 'empresas');")
        if not cursor.fetchone()[0]:
            logger.info("Criando estrutura do banco de dados...")
            cursor.execute("""
                CREATE TABLE nfce.empresas (
                    inscricao_estadual VARCHAR(20) PRIMARY KEY, apelido VARCHAR(100), uf VARCHAR(2) DEFAULT 'PB',
                    status_empresa CHAR(1) DEFAULT 'A', inicio DATE DEFAULT CURRENT_DATE, ultima_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP);""")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS nfce.solicitacoes (
                    id SERIAL PRIMARY KEY, inscricao_estadual VARCHAR(20) NOT NULL,
                    tipo VARCHAR(10) NOT NULL, data_ini VARCHAR(10) NOT NULL, data_fim VARCHAR(10) NOT NULL,
                    horario TIMESTAMP, link TEXT, solicitado INTEGER DEFAULT 0, baixado INTEGER DEFAULT 0,
                    finalizado BOOLEAN DEFAULT FALSE, anexo BOOLEAN DEFAULT NULL,
                    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP, atualizado_em TIMESTAMP,
                    mensagens INTEGER DEFAULT NULL,
                    CONSTRAINT fk_solicitacao_empresa FOREIGN KEY (inscricao_estadual) REFERENCES nfce.empresas (inscricao_estadual));""")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_solicitacoes_inscricao ON nfce.solicitacoes(inscricao_estadual);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_solicitacoes_status ON nfce.solicitacoes(solicitado, baixado);")
            conexao.commit()
            logger.info("Estrutura do banco criada com sucesso!")
        else:
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'nfce' AND table_name = 'empresas' AND column_name = 'inicio';")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE nfce.empresas ADD COLUMN inicio DATE DEFAULT CURRENT_DATE;")
                logger.info("Coluna 'inicio' adicionada à tabela empresas")
            cursor.execute("SELECT data_type FROM information_schema.columns WHERE table_schema = 'nfce' AND table_name = 'solicitacoes' AND column_name = 'baixado';")
            coluna = cursor.fetchone()
            if coluna and coluna[0] != 'integer':
                cursor.execute("ALTER TABLE nfce.solicitacoes ALTER COLUMN baixado TYPE INTEGER USING CASE WHEN baixado THEN 1 ELSE 0 END;")
                logger.info("Coluna 'baixado' alterada para INTEGER")
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'nfce' AND table_name = 'solicitacoes' AND column_name = 'finalizado';")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE nfce.solicitacoes ADD COLUMN finalizado BOOLEAN DEFAULT FALSE;")
                logger.info("Coluna 'finalizado' adicionada à tabela solicitacoes")
            # Verificar se a coluna 'anexo' existe
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'nfce' AND table_name = 'solicitacoes' AND column_name = 'anexo';")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE nfce.solicitacoes ADD COLUMN anexo BOOLEAN DEFAULT NULL;")
                logger.info("Coluna 'anexo' adicionada à tabela solicitacoes")
            # Verificar se a coluna 'mensagens' existe
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'nfce' AND table_name = 'solicitacoes' AND column_name = 'mensagens';")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE nfce.solicitacoes ADD COLUMN mensagens INTEGER DEFAULT NULL;")
                logger.info("Coluna 'mensagens' adicionada à tabela solicitacoes")
            cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'nfce' AND table_name = 'arquivos_xml');")
            if cursor.fetchone()[0]:
                cursor.execute("DROP TABLE nfce.arquivos_xml CASCADE;")
                logger.info("Tabela arquivos_xml removida")
            conexao.commit()
        return True
    except Exception as erro:
        conexao.rollback()
        logger.error(f"Erro ao criar/atualizar estrutura do banco: {erro}")
        return False
    finally:
        cursor.close()
        conexao.close()

def sincronizar_empresas():
    conexao_mysql = conectar_mysql()
    if not conexao_mysql or not conexao_mysql.is_connected():
        logger.error("Falha ao conectar ao banco de dados MySQL")
        return 0
    try:
        cursor_mysql = conexao_mysql.cursor()
        cursor_mysql.execute(os.environ.get('QUERY_LISTAR_EMPRESAS'))
        empresas = cursor_mysql.fetchall()
        logger.info(f"Encontradas {len(empresas)} empresas no MySQL")
    except Exception as erro:
        logger.error(f"Erro ao buscar empresas no MySQL: {erro}")
        return 0
    finally:
        if conexao_mysql.is_connected():
            cursor_mysql.close()
            conexao_mysql.close()
    conexao_pg = conectar_postgres()
    if not conexao_pg: return 0
    empresas_atualizadas = empresas_inseridas = 0
    try:
        cursor_pg = conexao_pg.cursor()
        for apelido, inscricao_estadual in empresas:
            cursor_pg.execute("SELECT 1 FROM nfce.empresas WHERE inscricao_estadual = %s", (inscricao_estadual,))
            if cursor_pg.fetchone():
                cursor_pg.execute("UPDATE nfce.empresas SET apelido = %s, ultima_atualizacao = CURRENT_TIMESTAMP WHERE inscricao_estadual = %s", (apelido, inscricao_estadual))
                empresas_atualizadas += 1
            else:
                cursor_pg.execute("INSERT INTO nfce.empresas (inscricao_estadual, apelido) VALUES (%s, %s)", (inscricao_estadual, apelido))
                empresas_inseridas += 1
        conexao_pg.commit()
        logger.info(f"Sincronização concluída: {empresas_inseridas} empresas inseridas, {empresas_atualizadas} atualizadas")
    except Exception as erro:
        conexao_pg.rollback()
        logger.error(f"Erro durante sincronização de empresas: {erro}")
        return 0
    finally:
        cursor_pg.close()
        conexao_pg.close()
    return empresas_inseridas + empresas_atualizadas

def criar_solicitacoes_cinco_dias_atras():
    hoje = datetime.now()
    cinco_dias_atras = hoje - timedelta(days=5)
    data_formatada = cinco_dias_atras.strftime('%d/%m/%Y')
    logger.info(f"Criando solicitações para a data: {data_formatada}")
    conexao_pg = conectar_postgres()
    if not conexao_pg: return 0
    solicitacoes_criadas = 0
    try:
        cursor_pg = conexao_pg.cursor()
        cursor_pg.execute("SELECT inscricao_estadual FROM nfce.empresas WHERE status_empresa = 'A'")
        empresas = cursor_pg.fetchall()
        for (inscricao_estadual,) in empresas:
            cursor_pg.execute("SELECT id FROM nfce.solicitacoes WHERE inscricao_estadual = %s AND data_ini = %s AND tipo = 'NFCE'", (inscricao_estadual, data_formatada))
            if cursor_pg.fetchone() is None:
                cursor_pg.execute("""
                    INSERT INTO nfce.solicitacoes (inscricao_estadual, tipo, data_ini, data_fim, solicitado, baixado, finalizado)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id""", (inscricao_estadual, "NFCE", data_formatada, data_formatada, 0, 0, False))
                solicitacoes_criadas += 1
        conexao_pg.commit()
        logger.info(f"Processo concluído: {solicitacoes_criadas} novas solicitações criadas para {data_formatada}")
    except Exception as erro:
        conexao_pg.rollback()
        logger.error(f"Erro durante criação de solicitações: {erro}")
        return 0
    finally:
        cursor_pg.close()
        conexao_pg.close()
    return solicitacoes_criadas

def executar_processo_completo():
    if not criar_estrutura_banco():
        logger.error("Falha ao criar estrutura do banco de dados. Abortando processo.")
        return
    empresas_sincronizadas = sincronizar_empresas()
    logger.info(f"Total de empresas sincronizadas: {empresas_sincronizadas}")
    solicitacoes_criadas = criar_solicitacoes_cinco_dias_atras()
    logger.info(f"Total de solicitações criadas: {solicitacoes_criadas}")
    logger.info("Processo finalizado com sucesso!")

if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("INICIANDO PROCESSO DE CRIAÇÃO DE SOLICITAÇÕES")
    logger.info("=" * 80)
    executar_processo_completo()