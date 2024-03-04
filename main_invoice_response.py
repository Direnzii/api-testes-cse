import http.client
import json
import os
import random
import sys
from math import ceil
import graypy
from database.connect_db import conectar_ao_banco
from database.queries import retorno_faturamento_info, retorno_faturamento_itens, get_pedido_from_cotacao
from cotefacilib.utils import xml_retorno
import requests
import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)
stdout_handler = logging.StreamHandler(stream=sys.stdout)
grayLog_handler = graypy.GELFUDPHandler('logs.zitausch.com', 12201, debugging_fields=False)
grayLog_handler.setLevel(logging.INFO)
grayLog_handler.setFormatter(logging.Formatter("%(message)s"))
logging.basicConfig(format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
                    handlers=[stdout_handler, grayLog_handler])
default_graylog_fields = {'app': 'api-testes'}


def getUrlAuth(rota):
    uri = os.getenv('AUTH_URL')
    port = os.getenv('AUTH_PORT_DEMO')
    return uri + ':' + port + rota


def enviar_xml(retornos):
    for payload in retornos:
        WS_LOGAN = os.getenv('WS_LOGAN')
        WS_LOGAN_PORT = os.getenv('WS_LOGAN_PORT')
        conn = http.client.HTTPConnection(f'{WS_LOGAN}:{WS_LOGAN_PORT}')
        headers = {
            'Content-Type': "text/xml",
            'User-Agent': "insomnia/8.5.1",
            'SOAPAction': ""  # POST""
        }
        conn.request("POST", "/CTFLLogan-webapp/webService", payload, headers)


def montar_cabecalho(cursor, idpedido, fat=True):
    cursor.execute(retorno_faturamento_info(idpedido))
    result = cursor.fetchall()[0]
    cnpj_cliente = result[0]
    cnpj_forn = result[1]
    idcotacao = result[2]
    if not fat:
        fat = 0
    else:
        fat = 1
    cabecalho = xml_retorno.cabecalho(cli=cnpj_cliente, forn=cnpj_forn, cot=idcotacao, fat=fat, ped=idpedido)
    return cabecalho


def arredondar_para_cima(result):
    """Esta função permite pegar a menor porção arredondada para cima do total de itens de um pedido, e dropar esses
    itens, a fim de obter um retorno de faturamento parcial por ‘item’"""
    qtd_itens = len(result)
    meia_qtd_itens = qtd_itens / 2
    saida_arredondada_para_cima = ceil(meia_qtd_itens)
    return saida_arredondada_para_cima


def get_itens(cursor, idpedido, fat_item=False, fat_qtd=False, notFat=False):
    itens = ''
    if fat_item:
        cursor.execute(retorno_faturamento_itens(idpedido))
        result = cursor.fetchall()
        random.shuffle(result)
        for i in range(0, arredondar_para_cima(result)):
            result.pop(i)
    elif fat_qtd:
        cursor.execute(retorno_faturamento_itens(idpedido, menos_qtd=True))
        result = cursor.fetchall()
    elif notFat:
        cursor.execute(retorno_faturamento_itens(idpedido, notFat=True))
        result = cursor.fetchall()
    else:
        cursor.execute(retorno_faturamento_itens(idpedido))
        result = cursor.fetchall()
    for linha in result:
        itens += linha[0] + '\n'
    return itens


def aleatorizar_retornos(tupla_pedidos, cursor):
    """Essa funcao permite gerar retorno de forma aleatoria, numa lista existem todas as possibilidades
    possiveis para os retornos, e por meio de um sorteio os retornos de faturamento são gerados"""
    possibilidades = [
        'faturado', 'nao_faturado', 'parcial_item', 'parcial_quantidade', 'sem_retorno'
    ]
    motivo = '!5Tm#Qp@9*d$a+LZ'
    saida_list = []
    for pedido in tupla_pedidos:
        sorteio = random.randint(0, 4)
        sorteio = possibilidades[sorteio]
        idpedido = pedido[0]
        if sorteio == 'sem_retorno':
            pass
        else:
            if sorteio == 'nao_faturado':
                saida = montar_saida_retorno(idpedido=idpedido,
                                             cursor=cursor,
                                             motivo=motivo)  # por padrao é não faturado
            elif sorteio == 'parcial_quantidade':
                saida = montar_saida_retorno(faturado_parcialmente_quantidade=True,
                                             idpedido=idpedido,
                                             cursor=cursor,
                                             motivo=motivo)
            elif sorteio == 'parcial_item':
                saida = montar_saida_retorno(faturado_parcialmente_item=True,
                                             idpedido=idpedido,
                                             cursor=cursor,
                                             motivo=motivo)
            else:
                saida = montar_saida_retorno(faturado=True,
                                             idpedido=idpedido,
                                             cursor=cursor,
                                             motivo=motivo)
            saida_list.append(saida)
    enviar_xml(saida_list)


def montar_saida_retorno(cursor,
                         idpedido,
                         motivo,
                         faturado=False,
                         faturado_parcialmente_quantidade=False,
                         faturado_parcialmente_item=False):
    saida = []
    if not faturado:
        cabecalho = montar_cabecalho(cursor, idpedido, fat=False)
        itens = get_itens(cursor, idpedido, notFat=True)
        logger.info(f': Pedido: {idpedido} >> retorno: não faturado', extra={**default_graylog_fields})
    elif faturado_parcialmente_quantidade:
        cabecalho = montar_cabecalho(cursor, idpedido)
        itens = get_itens(cursor, idpedido, fat_qtd=True)
        logger.info(f': Pedido: {idpedido} >> retorno: faturado_parcialmente_quantidade',
                    extra={**default_graylog_fields})
    elif faturado_parcialmente_item:
        cabecalho = montar_cabecalho(cursor, idpedido)
        itens = get_itens(cursor, idpedido, fat_item=True)
        logger.info(f': Pedido: {idpedido} >> retorno: faturado_parcialmente_item',
                    extra={**default_graylog_fields})
    else:
        cabecalho = montar_cabecalho(cursor, idpedido)
        itens = get_itens(cursor, idpedido)
        logger.info(f': Pedido: {idpedido} >> retorno: faturado',
                    extra={**default_graylog_fields})
    rodape = xml_retorno.rodape(motivo)
    saida += [cabecalho + itens + rodape]
    return saida


def processar_retorno_pedido(payload):
    idpedido = payload['idpedido']
    faturado = payload['faturado']
    faturado_parcialmente_item = payload['faturado_parcialmente_item']
    faturado_parcialmente_quantidade = payload['faturado_parcialmente_quantidade']
    motivo = payload['motivo']
    cursor = conectar_ao_banco()
    saida = montar_saida_retorno(faturado=faturado,
                                 faturado_parcialmente_item=faturado_parcialmente_item,
                                 faturado_parcialmente_quantidade=faturado_parcialmente_quantidade,
                                 motivo=motivo,
                                 cursor=cursor,
                                 idpedido=idpedido)
    cursor.close()
    enviar_xml(saida)


def processar_retorno_cotacao(payload):
    cotacao = payload['idcotacao']
    faturado = payload['faturado']
    faturado_parcialmente_item = payload['faturado_parcialmente_item']
    faturado_parcialmente_quantidade = payload['faturado_parcialmente_quantidade']
    aleatorio = payload['random']
    motivo = payload['motivo']
    cursor = conectar_ao_banco()
    cursor.execute(get_pedido_from_cotacao(cotacao))
    result = cursor.fetchall()
    if aleatorio:
        aleatorizar_retornos(result, cursor)
        cursor.close()
    else:
        for pedido in result:
            idpedido = pedido[0]
            saida = montar_saida_retorno(faturado=faturado,
                                         faturado_parcialmente_item=faturado_parcialmente_item,
                                         faturado_parcialmente_quantidade=faturado_parcialmente_quantidade,
                                         motivo=motivo,
                                         cursor=cursor,
                                         idpedido=idpedido)
            enviar_xml(saida)
        cursor.close()


def deletar_retorno(payload):
    idcotacao = payload['idcotacao']
    cursor = conectar_ao_banco()
    cursor.execute(get_pedido_from_cotacao(idcotacao, soComRetorno=True))
    result = cursor.fetchall()
    data = {
        "username": os.getenv('AUTH_USER'),
        "password": os.getenv('AUTH_SENHA')
    }
    token = requests.post(getUrlAuth('/login'), json=data)
    token = json.loads(token.text)
    headers = {
        'Content-Type': "application/json",
        'Authorization': f"Bearer {token['jwttoken']}"
    }
    for pedido in result:
        pedido_json = {"idPedido": pedido[0]}
        requests.post(getUrlAuth('/admin/apagarRetornoFaturamento'), headers=headers, json=pedido_json)
        logger.info(f': Pedido: {pedido[0]} >> retorno deletado',
                    extra={**default_graylog_fields})
