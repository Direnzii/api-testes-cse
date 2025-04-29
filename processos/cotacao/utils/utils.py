import json
import datetime
import os
import random
from math import ceil
import pytz
import requests
from database import queries
from database.conectar import conectar_db
from processos.cotacao.dto import dto
from server.instance import log

LIST_MOTIVO = [
    "Sucesso",
    "Sucesso",
    "Sucesso",
    "Sucesso",
    "Vencido",
    "Produtos não encontrados na base do fornecedor.",
    "Usuario e/ou senha invalido.",
    "Entre em contato com o suporte da Cotefacil atraves do chat ou por um de nossos telefones.",
    "Codigo de cliente nao encontrado no fornecedor.",
    "Nenhum produto encontrado no fornecedor.",
    "Cliente nao possui limite de credito suficiente.",
    "Expedição não encontrada. Entre em contato com o suporte ",
    "da Cotefacil através do chat ou por um de nossos telefones.",
    "Condição de pagamento inválida ou não disponível para o cliente.",
    "Cliente com documentacao vencida, verificar no portal."
]

logger = log.logger
default_graylog_fields = log.default_graylog_fields


def randomizar_lista_porcentagem(porcentagem_minima, porcentagem_maxima, lista):
    if len(lista) != 1:
        quantidade_minima = int(len(lista) * (porcentagem_minima / 100))
        quantidade_maxima = int(len(lista) * (porcentagem_maxima / 100))
        quantidade_randomizada = random.randint(quantidade_minima, quantidade_maxima)
        lista_randomizada = random.sample(lista, quantidade_randomizada)
        return lista_randomizada
    return lista


def progress_bar(iterable, prefix='', suffix='', decimals=1, length=100, fill='█', print_end="\r"):
    total = len(iterable)
    if total != 0:
        def print_progress_bar(iteration):
            percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
            filled_lengh = int(length * iteration // total)
            bar = fill * filled_lengh + '-' * (length - filled_lengh)
            print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=print_end)

        print_progress_bar(0)
        for i, item in enumerate(iterable):
            yield item
            print_progress_bar(i + 1)
        print()


def tranformar_em_sem_resposta(dto_prop, produtos):
    dto_prop['filiais'][0]['atende'] = 'N'
    dto_prop['filiais'][0]['motivo'] = ''
    dto_prop['codigoCondicaoPagamento'] = 'SEM RESPOSTA'
    while True:
        if len(produtos) != len(dto_prop['itens']):
            if len(produtos) > len(dto_prop['itens']):
                dto_prop['itens'].append(dto_prop['itens'][0])
            else:
                dto_prop['itens'].pop(0)
        else:
            break
    for indx, item in enumerate(dto_prop['itens']):
        item['codigoBarras'] = produtos[indx]
        item['qtde'] = 0
        item['atende'] = random.randint(1, 2)
    return dto_prop


def get_lista_sem_resposta(itens):
    list_sem_respostas = []
    for _ in range(0, ceil(len(itens) / 2)):
        list_sem_respostas.append(itens[0]['codigoBarras'])
        itens.pop(0)
    logger.info(f'Lista de produtos removidos: {sorted(list_sem_respostas)}',
                extra={**default_graylog_fields, 'operacao': "getListaSemResposta"})
    return list_sem_respostas


def gerar_dto_looping(result_ordenado, minimo_de_faturamento, multipla, conf, cursor_p, tipo_pagamento = 0,
                      randomizar=False):
    saida_list = []
    loop = 1
    for linha_result in result_ordenado:

        loop = float("{:.2f}".format(loop))
        saida_dto = dto.full_dto(linha_result=linha_result,
                                 minimo=minimo_de_faturamento,
                                 multipla=multipla,
                                 tipoPagamento= randomizar_tipoPagamento(
                                     tipoPagamento=tipo_pagamento,
                                     randomizar=randomizar),
                                 list_dict_itens=dto.produtos_dto_v2(linha_result, conf, cursor_p, looping=loop),
                                 cursor=cursor_p)
        saida_list.append(saida_dto)
        loop += 0.10
    return saida_list


def consultar_respostas(cursor, cote):
    cursor.execute(queries.get_todos_os_dados_para_resposta_com_base_na_cotacao(cote))
    return cursor.fetchall()


def aleatorizar_resposta_itens(dto_prop):
    if len(dto_prop['itens']) > 1:
        for _ in range(0, random.randint(1, int(len(dto_prop['itens']) / random.uniform(1.01, 2)))):
            dto_prop['itens'].pop(-1)  # função criara para aleatorizar respostas de multipla, removendo itens
        return dto_prop
    else:
        return dto_prop


def aleatorizar_resposta_itens_v2(dto_prop):
    """função criara para aleatorizar respostas de multipla, removendo itens"""
    itens = dto_prop.get('itens', [])
    if len(itens) <= 1:
        return dto_prop
    max_remocoes = int(len(itens) / random.uniform(1.01, 2))
    num_remocoes = random.randint(1, max_remocoes)
    for _ in range(num_remocoes):
        itens.pop()
    return dto_prop


def trazer_uma_unica_filial(list_dto_mc):
    result_list = []
    for resposta in list_dto_mc:
        for dto_respostas in resposta['respostas']:
            if dto_respostas['filiais'][0]['cnpj'] == '45776051000148':
                result_list.append(dto_respostas)  # rotina criada para trazer apenas uma filial no DTO
    return result_list


def checar_vencimento(cursor, cote):
    brasilia_timezone = pytz.timezone('America/Sao_Paulo')
    data_hora_atual = datetime.datetime.now(brasilia_timezone)
    formato = "%Y-%m-%d %H:%M:%S"
    time_date = data_hora_atual.strftime(formato).split(' ')
    ano_mes_dia = time_date[0].split('-')
    hora_min = time_date[1].split(':')
    cursor.execute(queries.get_vencimento(cote))
    vencimento = cursor.fetchall()
    vencimento_db = vencimento[0][0]
    ano_sys = ano_mes_dia[0]
    mes_sys = ano_mes_dia[1]
    dia_sys = ano_mes_dia[2]
    hora_sys = hora_min[0]
    min_sys = hora_min[1]
    ano_db = vencimento_db.strftime('%Y')
    mes_db = vencimento_db.strftime('%m')
    dia_db = vencimento_db.strftime('%d')
    hora_db = vencimento_db.strftime('%H')
    min_db = vencimento_db.strftime('%M')
    if ano_sys == ano_db and mes_sys == mes_db and dia_sys <= dia_db:
        if hora_sys < hora_db:
            return True
        if hora_sys == hora_db:
            return min_sys < min_db
    return False


def checar_vencimento_v2(cursor, cotacao):
    agora = datetime.datetime.now()
    cursor.execute(queries.get_vencimento(cotacao))
    vencimento = cursor.fetchone()[0]
    if agora.date() < vencimento.date():
        return True
    if agora.date() == vencimento.date():
        return agora.time() < vencimento.time()
    return False


def get_motivos(motivo_motivo, motivo_fixo=None):
    list_return = []
    for _ in range(0, motivo_motivo):
        if motivo_fixo:
            list_return.append('motivo_fixo')
        else:
            index = random.randint(0, len(LIST_MOTIVO) - 1)
            list_return.append(LIST_MOTIVO[index])
    return list_return


def get_url_env_api_resposta(multipla, oficial):
    if oficial:
        if multipla:
            url = os.getenv('API_RESPOSTA_URL_MULTIPLA_PROD')
        else:
            url = os.getenv('API_RESPOSTA_URL_PROD')
    else:
        if multipla:
            url = os.getenv('API_RESPOSTA_URL_MULTIPLA_DEMO')
        else:
            url = os.getenv('API_RESPOSTA_URL_DEMO')
    return url


def enviar_respostas(list_dtos, oficial):
    contador = 1
    for dto_from_list in list_dtos:
        try:
            cnpj_fornecedor = dto_from_list['respostas'][0]['cnpjFornecedor']
            idrepresentante = dto_from_list['respostas'][0]['codigoRepresentante']
            cnpj = dto_from_list['respostas'][0]['filiais'][0]["cnpj"]
            motivo = dto_from_list['respostas'][0]['filiais'][0]["motivo"]
            multipla_mensagem = ' > multipla resposta'
            multipla = True
        except Exception:  # tudo que nao for multipla cairá aqui
            cnpj_fornecedor = dto_from_list['cnpjFornecedor']
            idrepresentante = dto_from_list['codigoRepresentante']
            cnpj = dto_from_list['filiais'][0]["cnpj"]
            motivo = dto_from_list['filiais'][0]["motivo"]
            multipla_mensagem = ''
            multipla = False
        logger.info(f'Enviando DTO > {str(contador)}\n'
                    f'> cliente: {cnpj}{multipla_mensagem}\n'
                    f'> fornecedor: {cnpj_fornecedor}\n'
                    f'> idrepresentante: {idrepresentante}\n'
                    f'> motivo: {motivo}',
                    extra={**default_graylog_fields, 'operacao': "enviar_respostas"})
        contador += 1
        url = get_url_env_api_resposta(multipla=multipla, oficial=oficial)
        requests.post(url=url, json=dto_from_list)


def motivo_resposta(dto_prop):
    try:
        motivo = dto_prop['respostas'][0]['filiais'][0]["motivo"]
    except Exception:
        motivo = dto_prop['filiais'][0]["motivo"]
    return motivo


def alterar_vencimento(body):
    idcotacao = body.idcotacao
    quantos_dias = body.quantos_dias
    para_mais = body.para_mais
    de = body.motivoresposta_de
    para = body.motivoresposta_para
    oficial = body.oficial
    with conectar_db(oficial) as cursor:
        cursor.execute(queries.update_resposta_cliente(idcotacao, quantos_dias, para_mais))
        cursor.execute(queries.update_pedidomanualitemresposta(idcotacao, quantos_dias, para_mais))
        cursor.execute(queries.update_pedido(idcotacao, quantos_dias, para_mais))
        cursor.execute(queries.update_cotacao(idcotacao, quantos_dias, para_mais))
        cursor.execute(queries.update_vencido_para_respondido(idcotacao, de, para))
        cursor.connection.commit()


def excluir_registros(idcotacao, oficial):
    with conectar_db(oficial) as cursor:
        cursor.execute(queries.delete_negociacaodetalhecliente(idcotacao))
        cursor.execute(queries.delete_respostaclienteitem(idcotacao))
        cursor.execute(queries.delete_produtosemresposta(idcotacao))
        cursor.execute(queries.delete_respostacliente(idcotacao))
        cursor.connection.commit()


def mudar_para_em_analise(idcotacao, oficial=False, situacao=2):
    with conectar_db(oficial) as cursor:
        cursor.execute(queries.voltar_para_em_analise_1(idcotacao))
        cursor.execute(queries.voltar_para_em_analise_2(idcotacao))
        cursor.execute(queries.voltar_para_em_analise_3(idcotacao))
        cursor.execute(queries.deletar_pedidos_by_cotacao(idcotacao))
        cursor.execute(queries.update_situacao_cotacao(idcotacao, situacao))
        cursor.execute(queries.deletar_dados_looping(idcotacao))
        cursor.execute(queries.atualizar_data_montagem_pedido_manual(idcotacao))
        cursor.execute(queries.update_cotacao_inserir_no_pedido_iniciar_montagem_to_null(idcotacao))
        cursor.connection.commit()


def abrir_arquivo(nome):
    with open(nome, 'r') as file:
        return json.load(file)


def randomizar_tipoPagamento(randomizar: bool, tipoPagamento: int) -> int:
   return random.randint(0,3) if randomizar else tipoPagamento