import oracledb
import os

def conectar_db(oficial=False):
    if not oficial:
        user = os.getenv('ORACLE_USER')
        password = os.getenv('ORACLE_PASSWORD')
        host = os.getenv('ORACLE_HOST')
        port = os.getenv('ORACLE_PORT')
        db_name = os.getenv('ORACLE_DB_NAME')

        connection = oracledb.connect(
            user=user,
            password=password,
            dsn=f"{host}:{port}/{db_name}"
        )
        cursor = connection.cursor()
        return cursor
    if oficial:
        user = os.getenv('USER_ORACLE_OFICIAL')
        password = os.getenv('SENHA_ORACLE_OFICIAL')
        host = os.getenv('ORACLE_HOST')
        port = os.getenv('ORACLE_PORT')
        db_name = os.getenv('ORACLE_DB_NAME')

        connection = oracledb.connect(
            user=user,
            password=password,
            dsn=f"{host}:{port}/{db_name}"
        )
        cursor = connection.cursor()
        return cursor
