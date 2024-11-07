# Importacoes
import psycopg2
import logging
from os import getenv
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta



# Configuracao do logging
logging.basicConfig(
    filename='rpa_log.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Funcao para conectar ao banco de dados do primeiro
def conectar_banco(uri):
    try:
        conexao = psycopg2.connect(uri)
        cursor = conexao.cursor()
        logging.info("Conexao com o banco de dados estabelecida com sucesso.")
        return conexao, cursor
    except Exception as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        return None, None

# Funcao para sincronizar a tabela endereco
def sync_endereco(cursor_db1, cursor_db2, connection_db1, connection_db2):
    try:
        # Captura os dados da tabela do banco 1
        cursor_db1.execute("SELECT * FROM endereco;")
        endereco_records_db1 = cursor_db1.fetchall()
        logging.info("Dados de endereco obtidos do Banco 1 para sincronizacao.")

        # Captura os dados da tabela do banco 2
        cursor_db2.execute("SELECT * FROM endereco;")
        endereco_records_db2 = cursor_db2.fetchall()
        logging.info("Dados de endereco obtidos do Banco 2 para sincronizacao.")

        ids_db1 = [x[0] for x in endereco_records_db1]

        for endereco in endereco_records_db2:
            (id, numero, rua, bairro, cidade, estado, cep) = endereco
            
            if id not in ids_db1:
                cursor_db1.execute("""
                    INSERT INTO endereco 
                    (id, numero, rua, bairro, cidade, estado, cep)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """, (id, numero, rua, bairro, cidade, estado, cep))
                logging.info(f"Novo registro de endereco com ID {id} inserido no Banco 1.")

        for endereco in endereco_records_db1:
            (id, numero, rua, bairro, cidade, estado, cep) = endereco
            cursor_db2.execute("""
                UPDATE endereco SET
                numero = %s, rua = %s, bairro = %s, cidade = %s, estado = %s, cep = %s
                WHERE id = %s;
            """, (numero, rua, bairro, cidade, estado, cep, id))
            logging.info(f"Registro de endereco com ID {id} atualizado no Banco 2.")

        connection_db1.commit()
        connection_db2.commit()
        logging.info("Sincronizacao de endereco finalizada.")

    except Exception as e:
        connection_db1.rollback()
        connection_db2.rollback()
        # print(e.with_traceback(e))
        logging.error(f"Erro ao sincronizar tabela endereco: {e}")

# Funcao para sincronizar a tabela escola
def sync_escola(cursor_db1, cursor_db2, connection_db1, connection_db2):
    try:
        # Captura os dados da tabela do banco 1
        cursor_db1.execute("SELECT * FROM escola;")
        escola_records_db1 = cursor_db1.fetchall()
        logging.info("Dados de escola obtidos do Banco 1 para sincronização.")

        # Captura os dados da tabela do banco 2
        cursor_db2.execute("SELECT * FROM escola;")
        escola_records_db2 = cursor_db2.fetchall()
        logging.info("Dados de escola obtidos do Banco 2 para sincronização.")

        # Extrai os IDs da tabela 'escola' do Banco 1
        ids_db1 = [record[0] for record in escola_records_db1]

        # Insere registros que estão no Banco 2 e faltam no Banco 1
        for escola in escola_records_db2:
            (id, nome, email, telefone, id_endereco) = escola
            if id not in ids_db1:
                cursor_db1.execute("""
                    INSERT INTO escola 
                    (id, nome, email, telefone, id_endereco)
                    VALUES (%s, %s, %s, %s, %s);
                """, (id, nome, email, telefone, id_endereco))
                logging.info(f"Novo registro de escola com ID {id} inserido no Banco 1.")

        # Atualiza registros do Banco 1 para o Banco 2
        for escola in escola_records_db1:
            (id, nome, email, telefone, id_endereco) = escola
            cursor_db2.execute("""
                UPDATE escola SET
                nome = %s, email = %s, telefone = %s, id_endereco = %s
                WHERE id = %s;
            """, (nome, email, telefone, id_endereco, id))
            logging.info(f"Registro de escola com ID {id} atualizado no Banco 2.")

        # Confirma as alterações
        connection_db1.commit()
        connection_db2.commit()
        logging.info("Sincronização de escola finalizada.")

    except Exception as e:
        # Em caso de erro, reverte as alterações em ambas as conexões
        connection_db1.rollback()
        connection_db2.rollback()
        logging.error(f"Erro ao sincronizar tabela escola: {e}")

def sync_professor(cursor_db1, cursor_db2, connection_db1, connection_db2):
    try:
        # Captura os dados da tabela do banco 1
        cursor_db1.execute("SELECT * FROM professor;")
        professor_records_db1 = cursor_db1.fetchall()
        logging.info("Dados de professor obtidos do Banco 1 para sincronização.")

        # Captura os dados da tabela do banco 2
        cursor_db2.execute("SELECT * FROM professor;")
        professor_records_db2 = cursor_db2.fetchall()
        logging.info("Dados de professor obtidos do Banco 2 para sincronização.")

        # Extrai os IDs da tabela 'professor' do Banco 1
        ids_db1 = [record[0] for record in professor_records_db1]

        # Insere registros que estão no Banco 2 e faltam no Banco 1
        for professor in professor_records_db2:
            (id, nome, sobrenome, email, senha) = professor
            if id not in ids_db1:
                cursor_db1.execute("""
                    INSERT INTO professor 
                    (id, nome, sobrenome, email, senha)
                    VALUES (%s, %s, %s, %s, %s);
                """, (id, nome, sobrenome, email, senha))
                logging.info(f"Novo registro de professor com ID {id} inserido no Banco 1.")

        # Atualiza registros do Banco 1 para o Banco 2
        for professor in professor_records_db1:
            (id, nome, sobrenome, email, senha) = professor
            cursor_db2.execute("""
                UPDATE professor SET
                nome = %s, sobrenome = %s, email = %s, senha = %s
                WHERE id = %s;
            """, (nome, sobrenome, email, senha, id))
            logging.info(f"Registro de professor com ID {id} atualizado no Banco 2.")

        # Confirma as alterações
        connection_db1.commit()
        connection_db2.commit()
        logging.info("Sincronização de professor finalizada.")

    except Exception as e:
        # Em caso de erro, reverte as alterações em ambas as conexões
        connection_db1.rollback()
        connection_db2.rollback()
        logging.error(f"Erro ao sincronizar tabela professor: {e}")


def sync_turma(cursor_db1, cursor_db2, connection_db1, connection_db2):
    try:
        # Captura os dados da tabela do banco 1
        cursor_db1.execute("SELECT * FROM turma;")
        turma_records_db1 = cursor_db1.fetchall()
        logging.info("Dados de turma obtidos do Banco 1 para sincronização.")

        # Captura os dados da tabela do banco 2
        cursor_db2.execute("SELECT * FROM turma;")
        turma_records_db2 = cursor_db2.fetchall()
        logging.info("Dados de turma obtidos do Banco 2 para sincronização.")

        # Extrai os IDs da tabela 'turma' do Banco 1
        ids_db1 = [record[0] for record in turma_records_db1]

        # Insere registros que estão no Banco 2 e faltam no Banco 1
        for turma in turma_records_db2:
            (id, nomenclatura, serie, ano, id_escola, id_professor) = turma
            if id not in ids_db1:
                cursor_db1.execute("""
                    INSERT INTO turma 
                    (id, nomenclatura, serie, ano, id_escola, id_professor)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (id, nomenclatura, serie, ano, id_escola, id_professor))
                logging.info(f"Novo registro de turma com ID {id} inserido no Banco 1.")

        # Atualiza registros do Banco 1 para o Banco 2
        for turma in turma_records_db1:
            (id, nomenclatura, serie, ano, id_escola, id_professor) = turma
            cursor_db2.execute("""
                UPDATE turma SET
                nomenclatura = %s, serie = %s, ano = %s, id_escola = %s, id_professor = %s
                WHERE id = %s;
            """, (nomenclatura, serie, ano, id_escola, id_professor, id))
            logging.info(f"Registro de turma com ID {id} atualizado no Banco 2.")

        # Confirma as alterações
        connection_db1.commit()
        connection_db2.commit()
        logging.info("Sincronização de turma finalizada.")

    except Exception as e:
        # Em caso de erro, reverte as alterações em ambas as conexões
        connection_db1.rollback()
        connection_db2.rollback()
        logging.error(f"Erro ao sincronizar tabela turma: {e}")

def sync_aluno(cursor_db1, cursor_db2, connection_db1, connection_db2):
    try:
        # Captura os dados da tabela do banco 1
        cursor_db1.execute("SELECT * FROM aluno;")
        aluno_records_db1 = cursor_db1.fetchall()
        logging.info("Dados de aluno obtidos do Banco 1 para sincronização.")

        # Captura os dados da tabela do banco 2
        cursor_db2.execute("SELECT * FROM aluno;")
        aluno_records_db2 = cursor_db2.fetchall()
        logging.info("Dados de aluno obtidos do Banco 2 para sincronização.")

        # Extrai os IDs da tabela 'aluno' do Banco 1
        ids_db1 = [record[0] for record in aluno_records_db1]

        # Insere registros que estão no Banco 2 e faltam no Banco 1
        for aluno in aluno_records_db2:
            (id_user, nome, sobrenome, email, senha, xp, id_turma) = aluno
            if id_user not in ids_db1:
                cursor_db1.execute("""
                    INSERT INTO aluno 
                    (id, nome, sobrenome, email, senha, xp, id_turma)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """, (id_user, nome, sobrenome, email, senha, xp, id_turma))
                logging.info(f"Novo registro de aluno com ID {id} inserido no Banco 1.")

        # Atualiza registros do Banco 1 para o Banco 2
        for aluno in aluno_records_db1:
            print(aluno)
            (id_user, nome, sobrenome, xp, email, senha, id_turma) = aluno
            cursor_db2.execute("""
                UPDATE aluno SET
                nome = %s, sobrenome = %s, email = %s, senha = %s, xp = %s, id_turma = %s
                WHERE id = %s;
            """, (nome, sobrenome, email, senha, xp, id_turma, id_user))
            logging.info(f"Registro de aluno com ID {id} atualizado no Banco 2.")

        # Confirma as alterações
        connection_db1.commit()
        connection_db2.commit()
        logging.info("Sincronização de aluno finalizada.")

    except Exception as e:
        # Em caso de erro, reverte as alterações em ambas as conexões
        connection_db1.rollback()
        connection_db2.rollback()
        logging.error(f"Erro ao sincronizar tabela aluno: {e}")

def sync_responsavel(cursor_db1, cursor_db2, connection_db1, connection_db2):
    try:
        # Captura os dados da tabela do banco 1
        cursor_db1.execute("SELECT * FROM responsavel;")
        responsavel_records_db1 = cursor_db1.fetchall()
        logging.info("Dados de responsavel obtidos do Banco 1 para sincronização.")

        # Captura os dados da tabela do banco 2
        cursor_db2.execute("SELECT * FROM responsavel;")
        responsavel_records_db2 = cursor_db2.fetchall()
        logging.info("Dados de responsavel obtidos do Banco 2 para sincronização.")

        # Extrai os IDs da tabela 'responsavel' do Banco 1
        ids_db1 = [record[0] for record in responsavel_records_db1]

        # Insere registros que estão no Banco 2 e faltam no Banco 1
        for responsavel in responsavel_records_db2:
            (id, nome, sobrenome, email, senha, id_aluno) = responsavel
            if id not in ids_db1:
                cursor_db1.execute("""
                    INSERT INTO responsavel 
                    (id, nome, sobrenome, email, senha, id_aluno)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (id, nome, sobrenome, email, senha, id_aluno))
                logging.info(f"Novo registro de responsavel com ID {id} inserido no Banco 1.")

        # Atualiza registros do Banco 1 para o Banco 2
        for responsavel in responsavel_records_db1:
            (id, nome, sobrenome, email, senha, id_aluno) = responsavel
            cursor_db2.execute("""
                UPDATE responsavel SET
                nome = %s, sobrenome = %s, email = %s, senha = %s, id_aluno = %s
                WHERE id = %s;
            """, (nome, sobrenome, email, senha, id_aluno, id))
            logging.info(f"Registro de responsavel com ID {id} atualizado no Banco 2.")

        # Confirma as alterações
        connection_db1.commit()
        connection_db2.commit()
        logging.info("Sincronização de responsavel finalizada.")

    except Exception as e:
        # Em caso de erro, reverte as alterações em ambas as conexões
        connection_db1.rollback()
        connection_db2.rollback()
        logging.error(f"Erro ao sincronizar tabela responsavel: {e}")


# Funcao principal
def main():
    load_dotenv()

    connection_db1, cursor_db1 = conectar_banco(getenv("URI_BANCO_1"))
    connection_db2, cursor_db2 = conectar_banco(getenv("URI_BANCO_2"))

    if cursor_db1 and cursor_db2:
        try:
            sync_endereco(cursor_db1, cursor_db2, connection_db1, connection_db2)
            sync_escola(cursor_db1, cursor_db2, connection_db1, connection_db2)
            sync_professor(cursor_db1, cursor_db2, connection_db1, connection_db2)
            sync_turma(cursor_db1, cursor_db2, connection_db1, connection_db2)
            sync_aluno(cursor_db1, cursor_db2, connection_db1, connection_db2)
            sync_responsavel(cursor_db1, cursor_db2, connection_db1, connection_db2)


            logging.info("Sincronizacao completa com sucesso.")
        except Exception as error:
            logging.error(f"Erro ao realizar a sincronizacao: {error}")
        finally:
            cursor_db1.close()
            connection_db1.close()
            cursor_db2.close()
            connection_db2.close()
            logging.info("Conexoes com os bancos de dados fechadas.")
    else:
        logging.error("Falha na conexao com os bancos de dados.")

if __name__ == "__main__":
    main()