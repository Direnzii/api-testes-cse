import json
import os
import random
from math import ceil
from database.conectar import conectar_db
from database import queries
from processos.retorno_de_faturamento import xml_retorno
import requests
from server.instance import log

logger = log.logger
default_graylog_fields = log.default_graylog_fields


def processar_retorno_pedido(payload):
    idpedido = payload.idpedido
    faturado = payload.faturado
    faturado_parcialmente_item = payload.faturado_parcialmente_item
    faturado_parcialmente_quantidade = payload.faturado_parcialmente_quantidade
    motivo = payload.motivo
    oficial = payload.oficial
    with conectar_db(oficial) as cursor:
        saida = montar_saida_retorno(faturado=faturado,
                                     faturado_parcialmente_item=faturado_parcialmente_item,
                                     faturado_parcialmente_quantidade=faturado_parcialmente_quantidade,
                                     motivo=motivo,
                                     cursor=cursor,
                                     idpedido=idpedido)
    return enviar_xml(saida, oficial=oficial)


def processar_retorno_cotacao(payload):
    cotacao = payload.idcotacao
    faturado = payload.faturado
    faturado_parcialmente_item = payload.faturado_parcialmente_item
    faturado_parcialmente_quantidade = payload.faturado_parcialmente_quantidade
    aleatorio = payload.random
    motivo = payload.motivo
    oficial = payload.oficial
    with conectar_db(oficial) as cursor:
        cursor.execute(queries.get_pedido_from_cotacao(cotacao))
        result = cursor.fetchall()
        if aleatorio:
            response = aleatorizar_retornos(result, cursor)
            return response
        else:
            for pedido in result:
                idpedido = pedido[0]
                saida = montar_saida_retorno(faturado=faturado,
                                             faturado_parcialmente_item=faturado_parcialmente_item,
                                             faturado_parcialmente_quantidade=faturado_parcialmente_quantidade,
                                             motivo=motivo,
                                             cursor=cursor,
                                             idpedido=idpedido)
            return enviar_xml(saida, oficial=oficial)


def montar_saida_retorno(cursor,
                         idpedido,
                         motivo,
                         faturado=False,
                         faturado_parcialmente_quantidade=False,
                         faturado_parcialmente_item=False):
    saida = []
    if faturado_parcialmente_quantidade:
        cabecalho = montar_cabecalho(cursor, idpedido)
        itens = get_itens(cursor, idpedido, fat_qtd=True)
        logger.info(f'Pedido: {idpedido} >> retorno: faturado_parcialmente_quantidade',
                    extra={**default_graylog_fields})
    elif faturado_parcialmente_item:
        cabecalho = montar_cabecalho(cursor, idpedido)
        itens = get_itens(cursor, idpedido, fat_item=True)
        logger.info(f'Pedido: {idpedido} >> retorno: faturado_parcialmente_item',
                    extra={**default_graylog_fields})
    elif not faturado:
        cabecalho = montar_cabecalho(cursor, idpedido, fat=False)
        itens = get_itens(cursor, idpedido, not_fat=True)
        logger.info(f'Pedido: {idpedido} >> retorno: não faturado', extra={**default_graylog_fields})
    else:
        cabecalho = montar_cabecalho(cursor, idpedido)
        itens = get_itens(cursor, idpedido)
        logger.info(f'Pedido: {idpedido} >> retorno: faturado',
                    extra={**default_graylog_fields})
    rodape = xml_retorno.rodape(motivo)
    encoding = '<?xml version="1.0" encoding="UTF-8"?>\n'
    saida += [encoding + cabecalho + itens + rodape]
    return saida


def get_url_auth(rota):
    uri = os.getenv('AUTH_URL')
    port = os.getenv('AUTH_PORT_DEMO')
    return uri + ':' + port + rota


def enviar_xml(retornos, oficial=False):
    for payload in retornos:
        if not oficial:
            ws_logan = os.getenv('WS_LOGAN_URL')
            url = f'http://{ws_logan}:8084/CTFLLogan-webapp/webService'
        else:
            ws_logan = os.getenv('WS_LOGAN_URL_PROD')
            url = f'https://{ws_logan}/CTFLLogan-webapp/webService'
        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            "SOAPAction": "processarRetornoFaturamentoESBToCTFL"
        }
        try:
            print(payload)
            return requests.request("POST", url, headers=headers, data=payload.encode("utf-8"))
        except Exception as E:
            raise E


def arredondar_para_cima(result):
    """Esta função permite pegar a menor porção arredondada para cima do total de itens de um pedido, e dropar esses
    itens, a fim de obter um retorno de faturamento parcial por ‘item’"""
    qtd_itens = len(result)
    meia_qtd_itens = qtd_itens / 2
    saida_arredondada_para_cima = ceil(meia_qtd_itens)
    return saida_arredondada_para_cima


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
        if sorteio != 'sem_retorno':
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
    return enviar_xml(saida_list)


def montar_cabecalho(cursor, idpedido, fat=True):
    cursor.execute(queries.retorno_faturamento_info(idpedido))
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


def get_itens(cursor, idpedido, fat_item=False, fat_qtd=False, not_fat=False):
    itens = ''
    if fat_item:
        cursor.execute(queries.retorno_faturamento_itens(idpedido))
        result = cursor.fetchall()
        random.shuffle(result)
        for i in range(0, arredondar_para_cima(result)):
            if i: # coloquei isso aqui pq caso existisse apenas 1 item no pedido, simplesmente ele removia o unico
               result.pop(i)
    elif fat_qtd:
        cursor.execute(queries.retorno_faturamento_itens(idpedido, menos_qtd=True))
        result = cursor.fetchall()
    elif not_fat:
        cursor.execute(queries.retorno_faturamento_itens(idpedido, not_fat=True))
        result = cursor.fetchall()
    else:
        cursor.execute(queries.retorno_faturamento_itens(idpedido))
        result = cursor.fetchall()
    for linha in result:
        itens += linha[0] + '\n'
    return itens


def deletar_retorno(idcotacao, oficial):
    response_saida = []
    with conectar_db(oficial) as cursor:
        try:
            cursor.execute(queries.get_pedido_from_cotacao(idcotacao, so_com_retorno=True, pedido_looping=False))
            result = cursor.fetchall()
            data = {
                "username": os.getenv('AUTH_USER'),
                "password": os.getenv('AUTH_SENHA')
            }
            token = requests.post(get_url_auth('/login'), json=data)
            token = json.loads(token.text)
            headers = {
                'Content-Type': "application/json",
                'Authorization': f"Bearer {token['jwttoken']}"
            }
            for pedido in result:
                pedido_json = {"idPedido": pedido[0]}
                response = requests.post(get_url_auth('/admin/apagarRetornoFaturamento'), headers=headers, json=pedido_json)
                logger.info(f': Pedido: {pedido[0]} >> retorno deletado',
                            extra={**default_graylog_fields})
                response_saida.append(response)
            return response_saida
        except Exception:
            raise
