import os
from loggingConfig import get_logger
from datetime import datetime, timedelta
from dotenv import load_dotenv
from utils import conectar_postgres
import sys

load_dotenv()
logger = get_logger(__name__)

def criar_estrutura_banco():
    """Verifica se a estrutura do banco de dados existe"""
    conexao = conectar_postgres()
    if not conexao: return False
    cursor = conexao.cursor()
    try:
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'nfce' AND table_name = 'empresas');")
        if not cursor.fetchone()[0]:
            logger.error("Estrutura do banco de dados não existe! Execute o programa principal primeiro.")
            return False
        return True
    except Exception as erro:
        logger.error(f"Erro ao verificar estrutura do banco: {erro}")
        return False
    finally:
        cursor.close()
        conexao.close()

def validar_data(data_texto):
    """Valida uma data no formato DD/MM/AAAA"""
    try:
        return datetime.strptime(data_texto, "%d/%m/%Y")
    except ValueError:
        return None

def verificar_empresa(inscricao_estadual):
    """Verifica se a empresa existe e está ativa"""
    conexao = conectar_postgres()
    if not conexao: return False
    cursor = conexao.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM nfce.empresas WHERE status_empresa = 'A' AND inscricao_estadual = %s", (inscricao_estadual,))
        return cursor.fetchone()[0] > 0
    except Exception as erro:
        logger.error(f"Erro ao verificar empresa: {erro}")
        return False
    finally:
        cursor.close()
        conexao.close()

def criar_solicitacoes_periodo(data_periodo_inicio, data_periodo_fim, inscricao_estadual=None):
    """
    Cria solicitações para o período especificado, para uma empresa específica ou todas

    Args:
        data_periodo_inicio (datetime): Data inicial do período
        data_periodo_fim (datetime): Data final do período
        inscricao_estadual (str, opcional): Inscrição estadual da empresa específica. Se None, processa todas.

    Returns:
        int: Número de solicitações criadas
    """
    logger.info(f"Iniciando criação de solicitações para o período de {data_periodo_inicio.strftime('%d/%m/%Y')} a {data_periodo_fim.strftime('%d/%m/%Y')}")

    # Conectar ao banco de dados
    conexao_pg = conectar_postgres()
    if not conexao_pg:
        logger.error("Falha ao conectar ao banco de dados")
        return 0

    solicitacoes_criadas = 0
    ids_criados = []
    try:
        cursor_pg = conexao_pg.cursor()

        # Buscar empresas (específica ou todas)
        if inscricao_estadual:
            cursor_pg.execute("SELECT inscricao_estadual FROM nfce.empresas WHERE status_empresa = 'A' AND inscricao_estadual = %s", (inscricao_estadual,))
            empresas = cursor_pg.fetchall()
            if not empresas:
                logger.error(f"Empresa com inscrição estadual {inscricao_estadual} não encontrada ou não está ativa")
                return 0
        else:
            cursor_pg.execute("SELECT inscricao_estadual FROM nfce.empresas WHERE status_empresa = 'A'")
            empresas = cursor_pg.fetchall()

        total_empresas = len(empresas)
        logger.info(f"Processando {total_empresas} empresas")

        # Calcular o número de dias do período
        dias = (data_periodo_fim - data_periodo_inicio).days + 1
        dia_atual = data_periodo_inicio

        # Para cada dia do período
        for i in range(dias):
            data_formatada = dia_atual.strftime('%d/%m/%Y')
            logger.info(f"Processando dia {data_formatada} ({i+1}/{dias})")

            # Para cada empresa
            for idx, (inscricao_estadual,) in enumerate(empresas):
                # Verificar se já existe solicitação para esta empresa neste dia
                cursor_pg.execute(
                    "SELECT id FROM nfce.solicitacoes WHERE inscricao_estadual = %s AND data_ini = %s AND data_fim = %s AND tipo = 'NFCE'",
                    (inscricao_estadual, data_formatada, data_formatada)
                )

                if cursor_pg.fetchone() is None:
                    # Inserir nova solicitação e obter o ID inserido
                    cursor_pg.execute("""
                        INSERT INTO nfce.solicitacoes (inscricao_estadual, tipo, data_ini, data_fim, solicitado, baixado, finalizado)
                        VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                        (inscricao_estadual, "NFCE", data_formatada, data_formatada, 0, 0, False)
                    )
                    id_solicitacao = cursor_pg.fetchone()[0]
                    solicitacoes_criadas += 1
                    ids_criados.append(id_solicitacao)
                    print(f"Solicitação criada: ID {id_solicitacao} - IE: {inscricao_estadual} - Data: {data_formatada}")

                # Fazer commit a cada 100 solicitações
                if solicitacoes_criadas % 100 == 0 and solicitacoes_criadas > 0:
                    conexao_pg.commit()
                    logger.info(f"Commit intermediário: {solicitacoes_criadas} solicitações criadas até o momento")

            # Avançar para o próximo dia
            dia_atual += timedelta(days=1)
            conexao_pg.commit()  # Commit ao final de cada dia

        conexao_pg.commit()
        logger.info(f"Processo finalizado: {solicitacoes_criadas} novas solicitações criadas")

    except Exception as erro:
        conexao_pg.rollback()
        logger.error(f"Erro durante criação de solicitações: {erro}")
        return 0
    finally:
        cursor_pg.close()
        conexao_pg.close()

    return solicitacoes_criadas

def apagar_solicitacao_por_id(id_solicitacao):
    """Apaga uma solicitação específica pelo seu ID"""
    conexao = conectar_postgres()
    if not conexao: return False
    cursor = conexao.cursor()
    try:
        # Verificar se a solicitação existe
        cursor.execute("SELECT id FROM nfce.solicitacoes WHERE id = %s", (id_solicitacao,))
        if not cursor.fetchone():
            logger.error(f"Solicitação com ID {id_solicitacao} não encontrada!")
            return False

        # Apagar a solicitação
        cursor.execute("DELETE FROM nfce.solicitacoes WHERE id = %s", (id_solicitacao,))
        conexao.commit()
        logger.info(f"Solicitação com ID {id_solicitacao} apagada com sucesso")
        return True
    except Exception as erro:
        conexao.rollback()
        logger.error(f"Erro ao apagar solicitação: {erro}")
        return False
    finally:
        cursor.close()
        conexao.close()

def apagar_solicitacoes_por_periodo(data_periodo_inicio, data_periodo_fim, inscricao_estadual=None):
    """
    Apaga solicitações para o período especificado, para uma empresa específica ou todas

    Args:
        data_periodo_inicio (datetime): Data inicial do período
        data_periodo_fim (datetime): Data final do período
        inscricao_estadual (str, opcional): Inscrição estadual da empresa específica. Se None, apaga de todas.

    Returns:
        int: Número de solicitações apagadas
    """
    logger.info(f"Iniciando exclusão de solicitações para o período de {data_periodo_inicio.strftime('%d/%m/%Y')} a {data_periodo_fim.strftime('%d/%m/%Y')}")

    data_inicio_str = data_periodo_inicio.strftime('%d/%m/%Y')
    data_fim_str = data_periodo_fim.strftime('%d/%m/%Y')

    # Conectar ao banco de dados
    conexao = conectar_postgres()
    if not conexao: return 0
    cursor = conexao.cursor()

    try:
        # Preparar a consulta SQL
        if inscricao_estadual:
            # Apagar solicitações de uma empresa específica
            cursor.execute(
                "SELECT COUNT(*) FROM nfce.solicitacoes WHERE inscricao_estadual = %s AND data_ini >= %s AND data_ini <= %s AND tipo = 'NFCE'",
                (inscricao_estadual, data_inicio_str, data_fim_str)
            )
            total = cursor.fetchone()[0]

            if total == 0:
                logger.info(f"Nenhuma solicitação encontrada para o período e empresa especificados")
                return 0

            cursor.execute(
                "DELETE FROM nfce.solicitacoes WHERE inscricao_estadual = %s AND data_ini >= %s AND data_ini <= %s AND tipo = 'NFCE'",
                (inscricao_estadual, data_inicio_str, data_fim_str)
            )
        else:
            # Apagar solicitações de todas as empresas
            cursor.execute(
                "SELECT COUNT(*) FROM nfce.solicitacoes WHERE data_ini >= %s AND data_ini <= %s AND tipo = 'NFCE'",
                (data_inicio_str, data_fim_str)
            )
            total = cursor.fetchone()[0]

            if total == 0:
                logger.info(f"Nenhuma solicitação encontrada para o período especificado")
                return 0

            cursor.execute(
                "DELETE FROM nfce.solicitacoes WHERE data_ini >= %s AND data_ini <= %s AND tipo = 'NFCE'",
                (data_inicio_str, data_fim_str)
            )

        conexao.commit()
        logger.info(f"{total} solicitações apagadas com sucesso")
        return total
    except Exception as erro:
        conexao.rollback()
        logger.error(f"Erro ao apagar solicitações: {erro}")
        return 0
    finally:
        cursor.close()
        conexao.close()

def apagar_solicitacoes_por_empresa(inscricao_estadual):
    """
    Apaga todas as solicitações de uma empresa específica

    Args:
        inscricao_estadual (str): Inscrição estadual da empresa

    Returns:
        int: Número de solicitações apagadas
    """
    logger.info(f"Iniciando exclusão de solicitações para a empresa {inscricao_estadual}")

    # Conectar ao banco de dados
    conexao = conectar_postgres()
    if not conexao: return 0
    cursor = conexao.cursor()

    try:
        # Verificar quantas solicitações existem
        cursor.execute(
            "SELECT COUNT(*) FROM nfce.solicitacoes WHERE inscricao_estadual = %s AND tipo = 'NFCE'",
            (inscricao_estadual,)
        )
        total = cursor.fetchone()[0]

        if total == 0:
            logger.info(f"Nenhuma solicitação encontrada para a empresa {inscricao_estadual}")
            return 0

        # Apagar as solicitações
        cursor.execute(
            "DELETE FROM nfce.solicitacoes WHERE inscricao_estadual = %s AND tipo = 'NFCE'",
            (inscricao_estadual,)
        )

        conexao.commit()
        logger.info(f"{total} solicitações apagadas com sucesso")
        return total
    except Exception as erro:
        conexao.rollback()
        logger.error(f"Erro ao apagar solicitações: {erro}")
        return 0
    finally:
        cursor.close()
        conexao.close()

def apagar_solicitacoes_nao_processadas():
    """
    Apaga todas as solicitações que não foram processadas (solicitado = 0)

    Returns:
        int: Número de solicitações apagadas
    """
    logger.info("Iniciando exclusão de solicitações não processadas")

    # Conectar ao banco de dados
    conexao = conectar_postgres()
    if not conexao: return 0
    cursor = conexao.cursor()

    try:
        # Verificar quantas solicitações existem
        cursor.execute("SELECT COUNT(*) FROM nfce.solicitacoes WHERE solicitado = 0 AND tipo = 'NFCE'")
        total = cursor.fetchone()[0]

        if total == 0:
            logger.info("Nenhuma solicitação não processada encontrada")
            return 0

        # Apagar as solicitações
        cursor.execute("DELETE FROM nfce.solicitacoes WHERE solicitado = 0 AND tipo = 'NFCE'")

        conexao.commit()
        logger.info(f"{total} solicitações não processadas apagadas com sucesso")
        return total
    except Exception as erro:
        conexao.rollback()
        logger.error(f"Erro ao apagar solicitações: {erro}")
        return 0
    finally:
        cursor.close()
        conexao.close()

def menu_principal():
    """Menu principal do sistema"""
    while True:
        print("\n=== SISTEMA DE GESTÃO DE SOLICITAÇÕES ===\n")
        print("1 - Criar solicitações")
        print("2 - Apagar solicitações")
        print("0 - Sair")

        opcao = input("\nEscolha uma opção: ")

        if opcao == "0":
            print("Saindo do programa.")
            sys.exit(0)
        elif opcao == "1":
            menu_criar_solicitacoes()
        elif opcao == "2":
            menu_apagar_solicitacoes()
        else:
            print("Opção inválida!")

def menu_criar_solicitacoes():
    """Interface interativa para criação de solicitações"""
    print("\n=== CRIAÇÃO DE SOLICITAÇÕES ===\n")

    hoje = datetime(2025, 5, 8)  # Data fixa para o exemplo

    # Opções para seleção do período
    print("1 - Selecionar período específico")
    print("2 - Criar solicitações para todo o mês corrente")
    print("3 - Criar solicitações para o mês anterior")
    print("4 - Criar solicitações para todo o ano corrente")
    print("0 - Voltar ao menu principal")

    opcao = input("\nEscolha uma opção: ")

    # Processar opção de período
    data_periodo_inicio = None
    data_periodo_fim = None

    if opcao == "0":
        return
    elif opcao == "1":
        # Período específico
        while True:
            data_inicio_texto = input("Data inicial (DD/MM/AAAA): ")
            data_periodo_inicio = validar_data(data_inicio_texto)
            if not data_periodo_inicio:
                print("Data inválida! Use o formato DD/MM/AAAA.")
                continue

            # Verificar se a data é no passado
            if data_periodo_inicio > hoje:
                print("As solicitações só podem ser criadas para datas passadas!")
                continue
            break

        while True:
            data_fim_texto = input("Data final (DD/MM/AAAA): ")
            data_periodo_fim = validar_data(data_fim_texto)
            if not data_periodo_fim:
                print("Data inválida! Use o formato DD/MM/AAAA.")
                continue

            # Verificar se a data final é posterior à inicial
            if data_periodo_fim < data_periodo_inicio:
                print("A data final deve ser igual ou posterior à data inicial!")
                continue

            # Verificar se a data é no passado
            if data_periodo_fim > hoje:
                print("As solicitações só podem ser criadas para datas passadas!")
                continue
            break

    elif opcao == "2":
        # Mês corrente (até hoje)
        data_periodo_inicio = datetime(hoje.year, hoje.month, 1)
        data_periodo_fim = hoje
        print(f"Período selecionado: {data_periodo_inicio.strftime('%d/%m/%Y')} a {data_periodo_fim.strftime('%d/%m/%Y')}")

    elif opcao == "3":
        # Mês anterior
        if hoje.month == 1:
            # Janeiro do ano atual, mês anterior é dezembro do ano passado
            data_periodo_inicio = datetime(hoje.year - 1, 12, 1)
            data_periodo_fim = datetime(hoje.year, 1, 1) - timedelta(days=1)
        else:
            # Qualquer outro mês
            data_periodo_inicio = datetime(hoje.year, hoje.month - 1, 1)
            data_periodo_fim = datetime(hoje.year, hoje.month, 1) - timedelta(days=1)
        print(f"Período selecionado: {data_periodo_inicio.strftime('%d/%m/%Y')} a {data_periodo_fim.strftime('%d/%m/%Y')}")

    elif opcao == "4":
        # Ano corrente (até hoje)
        data_periodo_inicio = datetime(hoje.year, 1, 1)
        data_periodo_fim = hoje
        print(f"Período selecionado: {data_periodo_inicio.strftime('%d/%m/%Y')} a {data_periodo_fim.strftime('%d/%m/%Y')}")

    else:
        print("Opção inválida!")
        return

    # Opções para seleção de empresa
    print("\n--- Seleção de Empresa ---")
    print("1 - Processar todas as empresas ativas")
    print("2 - Inserir inscrição estadual manualmente")
    print("0 - Voltar ao menu anterior")

    opcao_empresa = input("\nEscolha uma opção: ")
    inscricao_estadual = None

    if opcao_empresa == "0":
        return menu_criar_solicitacoes()
    elif opcao_empresa == "1":
        # Todas as empresas
        print("Todas as empresas ativas serão processadas.")
    elif opcao_empresa == "2":
        # Entrada manual
        inscricao_estadual = input("Digite a inscrição estadual: ")
        if not verificar_empresa(inscricao_estadual):
            print(f"Erro: Empresa com inscrição estadual {inscricao_estadual} não encontrada ou não está ativa.")
            return
    else:
        print("Opção inválida!")
        return

    # Confirmar operação
    print("\n--- Resumo da Operação ---")
    print(f"Período: {data_periodo_inicio.strftime('%d/%m/%Y')} a {data_periodo_fim.strftime('%d/%m/%Y')}")
    if inscricao_estadual:
        print(f"Empresa: Inscrição Estadual {inscricao_estadual}")
    else:
        print("Empresa: Todas as empresas ativas")

    confirmacao = input("\nConfirma a criação das solicitações? (S/N): ")
    if confirmacao.upper() != "S":
        print("Operação cancelada pelo usuário.")
        return

    # Executar criação de solicitações
    logger.info("=" * 80)
    logger.info("INICIANDO PROCESSO DE CRIAÇÃO DE SOLICITAÇÕES")
    logger.info("=" * 80)

    if not criar_estrutura_banco():
        logger.error("Falha ao verificar estrutura do banco de dados. Abortando processo.")
        print("Erro: Estrutura do banco de dados inválida. Verifique o log.")
        return

    solicitacoes_criadas = criar_solicitacoes_periodo(data_periodo_inicio, data_periodo_fim, inscricao_estadual)
    logger.info(f"Total de solicitações criadas: {solicitacoes_criadas}")

    if solicitacoes_criadas > 0:
        print(f"\nProcesso finalizado com sucesso! Foram criadas {solicitacoes_criadas} solicitações.")
    else:
        print("\nNenhuma solicitação foi criada. Verifique o log para mais detalhes.")

    input("\nPressione Enter para continuar...")

def menu_apagar_solicitacoes():
    """Interface interativa para apagar solicitações"""
    print("\n=== EXCLUSÃO DE SOLICITAÇÕES ===\n")

    print("1 - Apagar solicitação por ID")
    print("2 - Apagar solicitações por período")
    print("3 - Apagar solicitações por empresa")
    print("4 - Apagar solicitações por período e empresa")
    print("5 - Apagar todas as solicitações não processadas")
    print("0 - Voltar ao menu principal")

    opcao = input("\nEscolha uma opção: ")

    hoje = datetime(2025, 5, 8)  # Data fixa para o exemplo

    if opcao == "0":
        return
    elif opcao == "1":
        # Apagar por ID
        try:
            id_solicitacao = int(input("Digite o ID da solicitação que deseja apagar: "))

            confirmacao = input(f"Confirma a exclusão da solicitação ID {id_solicitacao}? (S/N): ")
            if confirmacao.upper() != "S":
                print("Operação cancelada pelo usuário.")
                return

            resultado = apagar_solicitacao_por_id(id_solicitacao)

            if resultado:
                print(f"Solicitação ID {id_solicitacao} apagada com sucesso.")
            else:
                print(f"Erro ao apagar solicitação ID {id_solicitacao}. Verifique o log.")
        except ValueError:
            print("ID inválido! Digite um número inteiro.")

    elif opcao == "2":
        # Apagar por período
        while True:
            data_inicio_texto = input("Data inicial (DD/MM/AAAA): ")
            data_periodo_inicio = validar_data(data_inicio_texto)
            if not data_periodo_inicio:
                print("Data inválida! Use o formato DD/MM/AAAA.")
                continue
            break

        while True:
            data_fim_texto = input("Data final (DD/MM/AAAA): ")
            data_periodo_fim = validar_data(data_fim_texto)
            if not data_periodo_fim:
                print("Data inválida! Use o formato DD/MM/AAAA.")
                continue

            # Verificar se a data final é posterior à inicial
            if data_periodo_fim < data_periodo_inicio:
                print("A data final deve ser igual ou posterior à data inicial!")
                continue
            break

        print(f"\nPeríodo selecionado: {data_periodo_inicio.strftime('%d/%m/%Y')} a {data_periodo_fim.strftime('%d/%m/%Y')}")
        confirmacao = input(f"Confirma a exclusão de TODAS as solicitações neste período? (S/N): ")
        if confirmacao.upper() != "S":
            print("Operação cancelada pelo usuário.")
            return

        total = apagar_solicitacoes_por_periodo(data_periodo_inicio, data_periodo_fim)

        if total > 0:
            print(f"{total} solicitações apagadas com sucesso.")
        else:
            print("Nenhuma solicitação foi apagada. Verifique o log.")

    elif opcao == "3":
        # Apagar por empresa
        inscricao_estadual = input("Digite a inscrição estadual: ")
        if not verificar_empresa(inscricao_estadual):
            print(f"Aviso: Empresa com inscrição estadual {inscricao_estadual} não encontrada ou não está ativa.")
            confirmacao = input("Deseja continuar mesmo assim? (S/N): ")
            if confirmacao.upper() != "S":
                print("Operação cancelada pelo usuário.")
                return

        confirmacao = input(f"Confirma a exclusão de TODAS as solicitações da empresa {inscricao_estadual}? (S/N): ")
        if confirmacao.upper() != "S":
            print("Operação cancelada pelo usuário.")
            return

        total = apagar_solicitacoes_por_empresa(inscricao_estadual)

        if total > 0:
            print(f"{total} solicitações apagadas com sucesso.")
        else:
            print("Nenhuma solicitação foi apagada. Verifique o log.")

    elif opcao == "4":
        # Apagar por período e empresa
        while True:
            data_inicio_texto = input("Data inicial (DD/MM/AAAA): ")
            data_periodo_inicio = validar_data(data_inicio_texto)
            if not data_periodo_inicio:
                print("Data inválida! Use o formato DD/MM/AAAA.")
                continue
            break

        while True:
            data_fim_texto = input("Data final (DD/MM/AAAA): ")
            data_periodo_fim = validar_data(data_fim_texto)
            if not data_periodo_fim:
                print("Data inválida! Use o formato DD/MM/AAAA.")
                continue

            # Verificar se a data final é posterior à inicial
            if data_periodo_fim < data_periodo_inicio:
                print("A data final deve ser igual ou posterior à data inicial!")
                continue
            break

        inscricao_estadual = input("Digite a inscrição estadual: ")
        if not verificar_empresa(inscricao_estadual):
            print(f"Aviso: Empresa com inscrição estadual {inscricao_estadual} não encontrada ou não está ativa.")
            confirmacao = input("Deseja continuar mesmo assim? (S/N): ")
            if confirmacao.upper() != "S":
                print("Operação cancelada pelo usuário.")
                return

        print(f"\nPeríodo: {data_periodo_inicio.strftime('%d/%m/%Y')} a {data_periodo_fim.strftime('%d/%m/%Y')}")
        print(f"Empresa: {inscricao_estadual}")
        confirmacao = input(f"Confirma a exclusão de todas as solicitações neste período para esta empresa? (S/N): ")
        if confirmacao.upper() != "S":
            print("Operação cancelada pelo usuário.")
            return

        total = apagar_solicitacoes_por_periodo(data_periodo_inicio, data_periodo_fim, inscricao_estadual)

        if total > 0:
            print(f"{total} solicitações apagadas com sucesso.")
        else:
            print("Nenhuma solicitação foi apagada. Verifique o log.")

    elif opcao == "5":
        # Apagar não processadas
        confirmacao = input("Confirma a exclusão de TODAS as solicitações não processadas? (S/N): ")
        if confirmacao.upper() != "S":
            print("Operação cancelada pelo usuário.")
            return

        total = apagar_solicitacoes_nao_processadas()

        if total > 0:
            print(f"{total} solicitações não processadas apagadas com sucesso.")
        else:
            print("Nenhuma solicitação foi apagada. Verifique o log.")

    else:
        print("Opção inválida!")
        return

    input("\nPressione Enter para continuar...")

if __name__ == "__main__":
    menu_principal()