import json
import os
import sys
from uuid import uuid4
import boto3
from flask import Flask, request, make_response, Blueprint
from flask_cors import CORS
import main_invoice_response
import main_response_quotation
from cotefacilib.utils.constants import (sucesso, exception, sucesso_fila, sucesso_vencimento, sucesso_excluir,
                                         sucesso_enviado_fila, sucesso_em_analise)
import logging
import graypy

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

logger = logging.getLogger()
logger.setLevel(logging.INFO)
stdout_handler = logging.StreamHandler(stream=sys.stdout)
grayLog_handler = graypy.GELFUDPHandler('logs.zitausch.com', 12201, debugging_fields=False)
grayLog_handler.setLevel(logging.INFO)
grayLog_handler.setFormatter(logging.Formatter("%(message)s"))

logging.basicConfig(format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
                    handlers=[stdout_handler, grayLog_handler])
default_graylog_fields = {'app': 'api-testes'}


def abrirArquivo(nome):
    with open(nome, 'r') as file:
        return json.load(file)


config_gerais_ex = abrirArquivo('arquivos_config/config_geral.example.json')
config_geral_retorno = abrirArquivo('arquivos_config/config_geral_retorno.example.json')


def validar_json(json_inval):
    try:
        id_cotacao = json_inval["id_cotacao"]
        config_geral = json_inval["config_geral"]
        multipla_resposta = config_geral["multipla_resposta"]["multipla"]
        qtd_problema_de_minimo = json_inval["config_produto"]['qtd_problema_de_minimo']
        qtd_problema_de_embalagem = json_inval["config_produto"]['qtd_problema_de_embalagem']
        oportunidades = json_inval["config_produto"]['oportunidades']
        oportunidades_fixada = json_inval["config_produto"]['oportunidades_fixada']
        produtos_sem_st = json_inval["config_produto"]['produtos_sem_st']
        so_com_st = json_inval["config_produto"]['so_com_st']
        sem_estoque = json_inval["config_produto"]['sem_estoque']
        resposta_parcial_em_porcentagem = json_inval["config_geral"]['resposta_parcial_em_porcentagem']
        minimo_de_faturamento = json_inval["config_geral"]['minimo_de_faturamento']
        oportunidade = json_inval['tipo_teste']['looping']
        motivo_por_resposta = config_geral['motivo_por_resposta']['quantos']
        aguardando_resposta = config_geral['motivo_por_resposta']['aguardando_resposta']
        if all([id_cotacao,
                qtd_problema_de_minimo or qtd_problema_de_minimo == 0,
                qtd_problema_de_embalagem or qtd_problema_de_embalagem == 0,
                oportunidades or oportunidades == 0,
                oportunidades_fixada or oportunidades_fixada == 0,
                produtos_sem_st or produtos_sem_st == 0,
                so_com_st or so_com_st == 0,
                multipla_resposta in [True, False],
                sem_estoque in [True, False],
                resposta_parcial_em_porcentagem in [True, False],
                not minimo_de_faturamento or minimo_de_faturamento > 0,
                oportunidade in [True, False],
                motivo_por_resposta >= 0,
                aguardando_resposta >= 0]):
            return True
    except ValueError:
        return False


def responder():
    return make_response(sucesso_fila, 200)


@app.route('/api-testes', methods=['GET', ])
def status_ok():
    logger.info("api-testes rodando", extra={**default_graylog_fields, 'SourceMethodName': '/'})
    return make_response({'result': 'api-testes rodando'}, 200)


cotacao = Blueprint('cotacao', __name__, url_prefix='/api-testes/cotacao')


@cotacao.route('/example', methods=['GET', ])
def cotacao_example():
    logger.info("Examplo da cotação consultado", extra={**default_graylog_fields,
                                                        'SourceMethodName': '/cotacao/example'})
    return make_response(config_gerais_ex, 200)


@cotacao.route('/fila_responder', methods=['POST', ])
def gravar_fila():
    logger.info('Iniciando processo [/fila_responder]', extra={**default_graylog_fields,
                                                               'SourceMethodName': '/cotacao/fila_responder'})
    try:
        body = request.get_data()
        mensagem = json.loads(body)
        sqs = boto3.session.Session().resource(
            'sqs',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION'),
            endpoint_url=f'https://sqs.{os.getenv('AWS_DEFAULT_REGION')}.amazonaws.com')
        queue_name = os.getenv('QUEUE_BCKT_NAME_RESPONDER_COTACAO')
        queue_geral = sqs.get_queue_by_name(QueueName=queue_name)
        mensagem_sqs = json.dumps(mensagem)
        queue_geral.send_message(
            MessageBody=mensagem_sqs,
            MessageDeduplicationId=str(uuid4()),
            MessageGroupId=str(uuid4()))
        logger.info('Enviado para a fila com sucesso', extra={**default_graylog_fields,
                                                              'SourceMethodName': '/cotacao/fila_responder',
                                                              'fila': queue_name})
        return make_response(sucesso_enviado_fila, 200)
    except Exception:
        logger.exception('Erro no endpoint /cotacao/fila_responder')
        return make_response(exception, 400)


@cotacao.route('/responder', methods=['POST', ])
def responder_cotacao():
    try:
        body = request.get_data()
        config_gerais = json.loads(body)
        logger.info('Iniciando processo [/responder]',
                    extra={**default_graylog_fields,
                           'SourceMethodName': '/cotacao/responder',
                           'idcotacao': config_gerais["id_cotacao"]})
        if validar_json(config_gerais):
            logger.info('Passou na validação de payload, iniciando processamento de dados e montagem de DTO',
                        extra={**default_graylog_fields, 'SourceMethodName': '/cotacao/responder',
                               'idcotacao': config_gerais["id_cotacao"]})
            main_response_quotation.processar(config_gerais)
            return make_response(sucesso, 200)
        else:
            logger.error('Payload não passou na validação de payload', extra={**default_graylog_fields,
                                                                              'SourceMethodName': '/cotacao/responder'})
            return make_response(exception, 400)
    except Exception:
        logger.exception('Erro não esperado no endpoint [/responder]:',
                         extra={**default_graylog_fields,
                                'SourceMethodName': '/cotacao/responder'})
        return make_response(exception, 400)


@cotacao.route('/alterar_vencimento', methods=['POST', ])
def alterar_vencimento():
    logger.info('Iniciando processo [/alterar_vencimento]',
                extra={**default_graylog_fields, 'SourceMethodName': '/cotacao/alterar_vencimento'})
    try:
        body = request.get_data()
        body = json.loads(body)
        main_response_quotation.alterar_vencimento(body)
        logger.info('Vencimento alterado com sucesso, finalizando processo',
                    extra={**default_graylog_fields, 'SourceMethodName': '/cotacao/alterar_vencimento'})
        return make_response(sucesso_vencimento, 200)
    except Exception:
        logger.exception('Erro não esperado no endpoint [/alterar_vencimento]:',
                         extra={**default_graylog_fields,
                                'SourceMethodName': '/cotacao/alterar_vencimento'})
        return make_response(exception, 400)


@cotacao.route('/mudar_para_em_analise', methods=['POST', ])
def mudar_para_em_analise():
    logger.info('Iniciando processo [/mudar_para_em_analise]',
                extra={**default_graylog_fields, 'SourceMethodName': '/cotacao/mudar_para_em_analise'})
    try:
        body = request.get_data()
        body = json.loads(body)
        logger.info(f'Body [/mudar_para_em_analise]: {body}',
                    extra={**default_graylog_fields, 'SourceMethodName': '/retorno/mudar_para_em_analise'})
        main_response_quotation.mudar_para_em_analise(body)
        logger.info('Mudança de estado de cotacao de finalizada para em analise realizada com sucesso, finalizando '
                    'processo',
                    extra={**default_graylog_fields, 'SourceMethodName': '/cotacao/mudar_para_em_analise'})
        return make_response(sucesso_em_analise, 200)
    except Exception:
        logger.exception('Erro não esperado no endpoint [/mudar_para_em_analise]:',
                         extra={**default_graylog_fields,
                                'SourceMethodName': '/cotacao/mudar_para_em_analise'})
        return make_response(exception, 400)


@cotacao.route('/excluir_registros', methods=['POST', ])
def excluir_registros():
    logger.info('Iniciando processo [/excluir_registros]',
                extra={**default_graylog_fields, 'SourceMethodName': '/cotacao/excluir_registros'})
    try:
        logger.info('excluindo registros', extra={'app': 'api-testes', 'operacao': 'excluir_registros'})
        body = request.get_data()
        body = json.loads(body)
        logger.info(f'Body [/excluir_registros]: {body}',
                    extra={**default_graylog_fields, 'SourceMethodName': '/retorno/excluir_registros'})
        main_response_quotation.excluir_registros(body)
        logger.info('Exclusao de registros de resposta da cotacao realizado com sucesso, finalizando processo',
                    extra={**default_graylog_fields, 'SourceMethodName': '/cotacao/excluir_registros'})
        return make_response(sucesso_excluir, 200)
    except Exception:
        logger.exception('Erro não esperado no endpoint [/excluir_registros]:',
                         extra={**default_graylog_fields,
                                'SourceMethodName': '/cotacao/excluir_registros'})
        return make_response(exception, 400)


retorno = Blueprint('retorno', __name__, url_prefix='/api-testes/retorno')


@retorno.route('/example', methods=['GET', ])
def retorno_example():
    logger.info("Exemplos da retorno_faturamento consultado", extra={**default_graylog_fields,
                                                                     'SourceMethodName': '/retorno/example'})
    return make_response(config_geral_retorno, 200)


@retorno.route('/pedido', methods=['POST', ])
def retorno_pedido():
    logger.info('Iniciando processo [/pedido]',
                extra={**default_graylog_fields, 'SourceMethodName': '/retorno/pedido'})
    try:
        body = request.get_data()
        body = json.loads(body)
        logger.info(f'Body [/pedido]: {body}',
                    extra={**default_graylog_fields, 'SourceMethodName': '/retorno/pedido'})
        main_invoice_response.processar_retorno_pedido(body)
        logger.info("Retorno[s] gerado[s] com sucesso, finalizando processo",
                    extra={**default_graylog_fields,
                           'SourceMethodName': '/retorno/pedido'})
        return make_response(sucesso, 200)
    except Exception:
        logger.exception('Erro não esperado no endpoint [/pedido]:',
                         extra={**default_graylog_fields,
                                'SourceMethodName': '/retorno/pedido'})
        return make_response(exception, 400)


@retorno.route('/cotacao', methods=['POST', ])
def retorno_cotacao():
    logger.info('Iniciando processo [/cotacao]',
                extra={**default_graylog_fields, 'SourceMethodName': '/retorno/cotacao'})
    try:
        body = request.get_data()
        body = json.loads(body)
        logger.info(f'Body [/cotacao]: {body}',
                    extra={**default_graylog_fields, 'SourceMethodName': '/retorno/cotacao'})
        main_invoice_response.processar_retorno_cotacao(body)
        logger.info("Retorno[s] gerado[s] com sucesso, finalizando processo",
                    extra={**default_graylog_fields,
                           'SourceMethodName': '/retorno/cotacao'})
        return make_response(sucesso, 200)
    except Exception:
        logger.exception('Erro não esperado no endpoint [/cotacao]:',
                         extra={**default_graylog_fields,
                                'SourceMethodName': '/retorno/cotacao'})
        return make_response(exception, 400)


@retorno.route('/deletar', methods=['POST', ])
def deletar_retorno():
    logger.info('Iniciando processo [/deletar]',
                extra={**default_graylog_fields, 'SourceMethodName': '/retorno/deletar'})
    try:
        body = request.get_data()
        body = json.loads(body)
        logger.info(f'Body [/deletar]: {body}',
                    extra={**default_graylog_fields, 'SourceMethodName': '/retorno/deletar'})
        main_invoice_response.deletar_retorno(body)
        logger.info("Delete de retorno[s] realizado com sucesso, finalizando processo",
                    extra={**default_graylog_fields,
                           'SourceMethodName': '/retorno/deletar'})
        return make_response(sucesso, 200)
    except Exception:
        logger.exception('Erro não esperado no endpoint [/deletar]:',
                         extra={**default_graylog_fields,
                                'SourceMethodName': '/retorno/deletar'})
        return make_response(exception, 400)


if __name__ == '__main__':
    app.register_blueprint(cotacao)
    app.register_blueprint(retorno)
    app.run(host='0.0.0.0', port=8000)
