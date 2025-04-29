import logging
import sys
import graypy
from fastapi import FastAPI
import threading

class Server:
    """Classe que eu instancio os objetos do server para centralizar"""

    def __init__(self) -> None:
        self.versao = '3.2.1'
        self.app = FastAPI(version=self.versao, title='API-Testes', docs_url='/api-testes/docs',
                           openapi_url='/api-testes/openapi.json')
        self.lock_json = threading.Lock()


class Log:
    """Classe que eu instancio as variaveis de Logs para serem usadas no codigo, para centralizar"""

    def __init__(self):
        self.default_graylog_fields = {'app': 'api-testes'}
        self.logger = logging.getLogger('api-testes')
        self.setup_logging()

    def setup_logging(self):
        """Configura o logging"""
        self.logger.setLevel(logging.INFO)
        stdout_handler = logging.StreamHandler(stream=sys.stdout)
        gray_log_handler = graypy.GELFUDPHandler('logs.zitausch.com', 12201, debugging_fields=False)
        gray_log_handler.setLevel(logging.INFO)
        gray_log_handler.setFormatter(logging.Formatter("%(message)s"))
        self.logger.addHandler(stdout_handler)
        self.logger.addHandler(gray_log_handler)


server = Server()
log = Log()