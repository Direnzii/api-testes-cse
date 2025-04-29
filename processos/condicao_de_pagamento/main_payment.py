import os
import random

import requests
import json
from database import queries
from database.conectar import conectar_db


def retornar_forn_e_lista_condicoes_itens(texto):
    list_saida = []
    cnpj_forn = ''
    string_list = texto.split('\r\n')
    for linha in string_list:
        list_linha = linha.split(';')
        if int(list_linha[0]) == 1:
            cnpj_forn = list_linha[1]
        elif int(list_linha[0]) == 2:
            cnpj = list_linha[1]
            codigo = list_linha[2]
            prazo = list_linha[3]
            descricao = list_linha[5]
            payload = {
                'cnpjCliente': cnpj,
                'codigo': codigo,
                'dias': prazo,
                "descricao": descricao,
            }
            list_saida.append(payload)
    return cnpj_forn, list_saida


def retornar_payload_condicao_oficial(itens):
    with conectar_db(True) as cursor:
        try:
            cursor.execute(queries.retornar_id_representante_ftp_pelo_fornecedor(cnpj_fornecedor=itens[0]))
            idrepresentante = cursor.fetchall()
            if not idrepresentante:
                return False
            payload = {
                "cnpjFornecedor": itens[0],
                "codigoRepresentante": idrepresentante,
                "cnpjCliente": itens[1][0]['cnpjCliente'],
                "inativaCondicao": False,
                "itens": itens[1]
            }
            return payload
        except:
            raise


def enviar_condicao_para_api(payload):
    response = requests.post(url=os.getenv('API_CONDICAO_PAGAMENTO_URL_OFICIAL'), json=payload)
    return response


def enviar_condicao_para_api_demo(payload):
    return requests.post(url=os.getenv('API_CONDICAO_PAGAMENTO_URL_DEMO'), json=payload)


def processar_condicao_e_enviar_ftp_oficial(texto):
    payload = retornar_payload_condicao_oficial(itens=retornar_forn_e_lista_condicoes_itens(texto=texto))
    if payload:
        return enviar_condicao_para_api(payload)
    return False


def trazer_id_representante(cnpj_fornecedor, nome_representante, cnpj_cliente):
    with conectar_db() as cursor:
        cursor.execute(queries.retornar_id_representante(cnpj_fornecedor=cnpj_fornecedor,
                                                         nome_representante=nome_representante, cnpjs_str=cnpj_cliente))
        id_repre = cursor.fetchall()
    if not id_repre and len(id_repre) > 1:
        raise ValueError(f"Error: ao encontrar algum ID")
    return id_repre[0][0]


def transforma_em_json_valido_api_condicao(cnpj_fornecedor, cnpj_cliente, id_representante, quantidade):
    payload_cond_pagamento = {
        "cnpjFornecedor": cnpj_fornecedor,
        "codigoRepresentante": id_representante,
        "cnpjCliente": cnpj_cliente,
        "inativaCondicao": False
    }
    itens = aleatoriza_itens_condicao(quantidade=quantidade, cnpjCliente=cnpj_cliente)
    payload_cond_pagamento["itens"] = itens
    return payload_cond_pagamento


def aleatoriza_itens_condicao(quantidade, cnpjCliente):
    faixa_total = list(range(1,90))
    itens = [
        {
            "cnpjCliente": cnpjCliente,
            "codigo": str(random.randint(1, 999)),
            "dias": '|'.join(map(str, random.sample(faixa_total, random.randint(1, 5))))
        }
        for _ in range(quantidade)
    ]

    return itens

async def gera_condicao_pagamento(payload):
    cnpj_fornecedor = payload.cnpj_fornecedor
    cnpj_cliente = payload.cnpj_cliente
    nome_representante = payload.nome_representante
    quantidade = payload.quantidade_de_condicoes

    id_representante = trazer_id_representante(cnpj_fornecedor=cnpj_fornecedor,
                                               cnpj_cliente=cnpj_cliente, nome_representante=nome_representante)
    request = transforma_em_json_valido_api_condicao(cnpj_fornecedor=cnpj_fornecedor, cnpj_cliente=cnpj_cliente,
                                                     id_representante=id_representante, quantidade=quantidade)
    return enviar_condicao_para_api_demo(request)


async def desativar_condicao_pagamento(payload):
    nome_representante = payload.nome_representante
    cnpj_cliente = payload.cnpj_cliente
    cnpj_fornecedor = payload.cnpj_fornecedor
    id_representante = trazer_id_representante(cnpj_fornecedor=cnpj_fornecedor,
                                               cnpj_cliente=cnpj_cliente, nome_representante=nome_representante)
    with conectar_db() as cursor:
        cursor.execute(queries.desativar_condicao_pagamento(cnpj_fornecedor=cnpj_fornecedor,
                                                            id_representante=id_representante, cnpjs_str=cnpj_cliente))
        cursor.connection.commit()
