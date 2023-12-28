import http.client
import random
from math import ceil

from database.connect_db import conectar_ao_banco
from database.queries import retorno_faturamento_info, retorno_faturamento_itens
from cotefacilib.utils import xml_retorno


def enviar_xml(cabecalho, itens, rodape):
    conn = http.client.HTTPConnection("52.201.113.49:8084")
    payload = (cabecalho + itens + rodape)
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


def get_itens(cursor, idpedido, fat_item=False):
    cursor.execute(retorno_faturamento_itens(idpedido))
    result = cursor.fetchall()
    itens = ''
    if fat_item:
        random.shuffle(result)
        a = len(result)
        b = a / 2
        c = ceil(b)
        for i in range(0, c):
            result.pop(i)

    for linha in result:
        itens += linha[0] + '\n'
    return itens


def processar_retorno(payload):
    idpedido = payload['idpedido']
    faturado = payload['faturado']
    faturado_parcialmente_item = payload['faturado_parcialmente_item']
    faturado_parcialmente_quantidade = payload['faturado_parcialmente_quantidade']
    motivo = payload['motivo']
    cursor = conectar_ao_banco()
    if not faturado:
        cabecalho = montar_cabecalho(cursor, idpedido, fat=False)
        itens = get_itens(cursor, idpedido)
    elif faturado_parcialmente_quantidade:
        cabecalho = montar_cabecalho(cursor, idpedido)
        itens = get_itens(cursor, idpedido, fat_qtd=True)
    elif faturado_parcialmente_item:
        cabecalho = montar_cabecalho(cursor, idpedido)
        itens = get_itens(cursor, idpedido, fat_item=True)
    else:
        cabecalho = montar_cabecalho(cursor, idpedido)
        itens = get_itens(cursor, idpedido)
    rodape = xml_retorno.rodape(motivo)
    enviar_xml(cabecalho, itens, rodape)
    cursor.close()


def main():
    processar_retorno('')


if __name__ == '__main__':
    main()
