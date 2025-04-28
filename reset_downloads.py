#!/usr/bin/env python3
import os
import logging
from dotenv import load_dotenv
import psycopg2
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("reset_downloads.log")
    ]
)

def conectar_postgres():
    """Estabelece conexão com o banco de dados PostgreSQL."""
    try:
        conexao = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST"),
            user=os.environ.get("POSTGRES_USER"),
            password=os.environ.get("POSTGRES_PASSWORD"),
            dbname=os.environ.get("POSTGRES_DATABASE"),
            port=os.environ.get("POSTGRES_PORT")
        )
        logging.info("Conexão ao PostgreSQL estabelecida com sucesso")
        return conexao
    except psycopg2.Error as erro:
        logging.error(f"Erro ao conectar ao PostgreSQL: {erro}")
        return None

def resetar_status_baixado():
    """Reseta todos os valores da coluna 'baixado' para FALSE"""
    # Carregar variáveis de ambiente
    load_dotenv()
    
    # Conectar ao banco de dados
    conexao = conectar_postgres()
    if not conexao:
        logging.error("Não foi possível conectar ao banco de dados")
        return
    
    try:
        # Criar cursor
        cursor = conexao.cursor()
        
        # Executar a atualização
        cursor.execute("""
            UPDATE solicitacoes 
            SET baixado = FALSE,
                atualizado_em = CURRENT_TIMESTAMP
            WHERE baixado = TRUE
        """)
        
        # Obter o número de linhas afetadas
        linhas_afetadas = cursor.rowcount
        
        # Commit da transação
        conexao.commit()
        
        # Fechar cursor e conexão
        cursor.close()
        conexao.close()
        
        # Registrar resultado
        logging.info(f"Operação concluída com sucesso! {linhas_afetadas} registros foram resetados.")
        print(f"\n========================================")
        print(f"Status de download resetado com sucesso!")
        print(f"{linhas_afetadas} registros foram atualizados.")
        print(f"Data/hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"========================================\n")
        
    except Exception as erro:
        # Em caso de erro, fazer rollback
        if conexao:
            conexao.rollback()
        logging.error(f"Erro ao resetar status: {erro}")
        print(f"\nERRO: Não foi possível resetar os status de download: {erro}\n")
    finally:
        # Garantir que a conexão seja fechada
        if conexao and not conexao.closed:
            conexao.close()

if __name__ == "__main__":
    print("\nIniciando reset dos status de download...\n")
    resetar_status_baixado()
