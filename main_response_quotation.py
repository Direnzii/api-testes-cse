import io
import json
import datetime
import logging
import sys
from math import ceil
import graypy
import pytz
from database.connect_db import conectar_ao_banco
from cotefacilib.utils.send_to_s3 import strategy, strategy_oficial
from cotefacilib.utils.full_DTO import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)
stdout_handler = logging.StreamHandler(stream=sys.stdout)
grayLog_handler = graypy.GELFUDPHandler('logs.zitausch.com', 12201, debugging_fields=False)
grayLog_handler.setLevel(logging.INFO)
grayLog_handler.setFormatter(logging.Formatter("%(message)s"))
logging.basicConfig(format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
                    handlers=[stdout_handler, grayLog_handler])
default_graylog_fields = {'app': 'api-testes'}
default_graylog_processar = {'SourceMethodName': 'main_response_quotation.processar'}


def conectar_banco(oficial=False):
    return conectar_ao_banco(oficial)


def randomizar_lista_porcentagem(porcentagem_minima, porcentagem_maxima, lista):
    if len(lista) != 1:
        quantidade_minima = int(len(lista) * (porcentagem_minima / 100))
        quantidade_maxima = int(len(lista) * (porcentagem_maxima / 100))
        quantidade_randomizada = random.randint(quantidade_minima, quantidade_maxima)
        lista_randomizada = random.sample(lista, quantidade_randomizada)
        return lista_randomizada
    return lista


def produtos_dto(linha_result, problemas_prop, cursor_db, looping=0.0, sem_resposta=False):
    id_rep = linha_result[0]
    cnpj_cliente = linha_result[1]
    id_cotacao_processar = linha_result[3]
    cursor_db.execute(query_get_prod_conjunta(id_cotacao_processar, id_rep, cnpj_cliente))
    produtos = cursor_db.fetchall()
    if not produtos:
        cursor_db.execute(query_get_prod_simples(id_cotacao_processar, id_rep))
        produtos = cursor_db.fetchall()
    if sem_resposta:
        saida_list = []
        for produto in produtos:
            saida_json = itens_DTO(produto)
            saida_list.append(saida_json)
        return saida_list
    minimo = problemas_prop["config_produto"]["qtd_problema_de_minimo"]
    embalagem = problemas_prop["config_produto"]["qtd_problema_de_embalagem"]
    sem_st = problemas_prop["config_produto"]["produtos_sem_st"]
    com_st = problemas_prop["config_produto"]["so_com_st"]
    sem_estoque = problemas_prop["config_produto"]["sem_estoque"]
    resposta_parcial_em_porcentagem = problemas_prop["config_geral"]["resposta_parcial_em_porcentagem"]
    oportunidades = problemas_prop["config_produto"]["oportunidades"]
    oportunidades_fixada = problemas_prop["config_produto"]["oportunidades_fixada"]
    quantidade_de_resposta_produto = problemas_prop["config_geral"]["quantidade_de_resposta_produto"]
    atende = problemas_prop["config_geral"]["atende"]
    if problemas_prop["config_geral"]["versao_arquivo"]["versao"] == '4.2':
        cashBackQuantidade = problemas_prop["config_produto"]['cashback_4.2']['qtd_de_itens']
        cashBackDesconto = problemas_prop["config_produto"]['cashback_4.2']['porcentagem_cashback']
    else:
        cashBackQuantidade = problemas_prop["config_produto"]['cashback_4.3']['qtd_de_itens']
        cashBackDesconto = 0
    saida_list = []
    random.shuffle(produtos)
    if resposta_parcial_em_porcentagem:
        produtos = randomizar_lista_porcentagem(10, 75, produtos)
    for produto in produtos:
        if looping:
            produto = list(produto)
            produto[2] = looping  # preco hardcoded para o caso de testes com looping
        if not quantidade_de_resposta_produto:
            if minimo > 0:
                saida_json = itens_DTO(produto,
                                       qtd_problema_minimo=True,
                                       cashBack=cashBackDesconto,
                                       looping=looping)
                minimo -= 1
            elif embalagem > 0:
                saida_json = itens_DTO(produto,
                                       qtd_problema_embalagem=True,
                                       cashBack=cashBackDesconto,
                                       looping=looping)
                embalagem -= 1
            elif sem_st > 0:
                saida_json = itens_DTO(produto,
                                       qtd_sem_st=True,
                                       cashBack=cashBackDesconto,
                                       looping=looping)
                sem_st -= 1
            elif com_st > 0:
                saida_json = itens_DTO(produto,
                                       com_st=True,
                                       cashBack=cashBackDesconto,
                                       looping=looping)
                com_st -= 1
            elif sem_estoque:
                saida_json = itens_DTO(produto,
                                       sem_estoque=True,
                                       atende=atende,
                                       cashBack=cashBackDesconto,
                                       looping=looping)
            elif oportunidades > 0:
                saida_json = itens_DTO(produto,
                                       oportunidades=True,
                                       cashBack=cashBackDesconto,
                                       looping=looping)
                oportunidades -= 1
            elif oportunidades_fixada > 0:
                saida_json = itens_DTO(produto,
                                       oportunidades_fixada=True,
                                       cashBack=cashBackDesconto,
                                       looping=looping)
                oportunidades_fixada -= 1
            else:
                saida_json = itens_DTO(produto,
                                       cashBack=cashBackDesconto,
                                       looping=looping)
            saida_list.append(saida_json)
        else:
            quantidade_de_resposta_produto -= 1
        if cashBackQuantidade:
            cashBackQuantidade -= 1
    return saida_list


def montarDTOmotivo(motivo, linha_result, conf, cursor, multipla, vencido=False):
    versaoArquivo = conf["config_geral"]["versao_arquivo"]["versao"]
    minimo_de_faturamento = conf["config_geral"]["minimo_de_faturamento"]
    if motivo == 'Sucesso' or motivo == 'Vencido':  # se for sucesso o motivo
        if multipla:
            return getListMultipla(linha_result=linha_result,
                                   cursor_p=cursor,
                                   minimo_de_faturamento=minimo_de_faturamento,
                                   versaoArquivo=versaoArquivo,
                                   conf=conf,
                                   vencido=vencido)
        else:
            list_dict_itens = produtos_dto(linha_result=linha_result, problemas_prop=conf, cursor_db=cursor)
            motivo = None  # por que para sucesso o full DTO ja seta a string sucesso como padrão
    else:  # se nao for sucesso o motivo
        list_dict_itens = []  # lista vazia, pois com motivos nao tem itens
    return full_DTO(linha_result=linha_result,
                    versaoArquivo=versaoArquivo,
                    list_dict_itens=list_dict_itens,
                    minimo=minimo_de_faturamento,
                    list_motivo=motivo,
                    cursor=cursor,
                    vencido=vencido)


def progressBar(iterable, prefix='', suffix='', decimals=1, length=100, fill='█', printEnd="\r"):
    total = len(iterable)
    if total != 0:
        def printProgressBar(iteration):
            percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
            filledLength = int(length * iteration // total)
            bar = fill * filledLength + '-' * (length - filledLength)
            print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)

        printProgressBar(0)
        for i, item in enumerate(iterable):
            yield item
            printProgressBar(i + 1)
        print()
    else:
        pass


def tranformar_em_sem_resposta(dto, produtos):
    dto['filiais'][0]['atende'] = 'N'
    dto['filiais'][0]['motivo'] = ''
    dto['codigoCondicaoPagamento'] = 'SEM RESPOSTA'
    while True:
        if len(produtos) != len(dto['itens']):
            if len(produtos) > len(dto['itens']):
                dto['itens'].append(dto['itens'][0])
            else:
                dto['itens'].pop(0)
        else:
            break
    for indx, item in enumerate(dto['itens']):
        item['codigoBarras'] = produtos[indx]
        item['qtde'] = 0
        item['atende'] = random.randint(1, 2)
    return dto


def getListaSemResposta(itens):
    list_sem_respostas = []
    for _ in range(0, ceil(len(itens) / 2)):
        list_sem_respostas.append(itens[0]['codigoBarras'])
        itens.pop(0)
    logger.info(f'Lista de produtos removidos: {sorted(list_sem_respostas)}',
                extra={**default_graylog_fields, **default_graylog_processar, 'operacao': "getListaSemResposta"})
    return list_sem_respostas


def getListMultipla(linha_result,
                    conf,
                    minimo_de_faturamento,
                    versaoArquivo,
                    cursor_p,
                    list_sem_respostas=None,
                    vencido=False):
    sem_resposta = conf['config_geral']['multipla_resposta']['sem_resposta']
    saida_list = []
    for num in range(1, random.randint(3, 7)):  # quantidade de multiplas por representante (7)
        saida_dto = full_DTO(linha_result=linha_result,
                             versaoArquivo=versaoArquivo,
                             list_dict_itens=produtos_dto(linha_result, conf, cursor_p),
                             minimo=minimo_de_faturamento,
                             cursor=cursor_p,
                             multipla=num,
                             vencido=vencido)
        saida_list.append(saida_dto)
    if sem_resposta:
        for dto in saida_list:
            indx_2 = 0
            sairam = []
            naoSairam = []
            while True:
                try:
                    if dto['itens'][indx_2]['codigoBarras'] in list_sem_respostas:
                        sairam.append(dto['itens'][indx_2]['codigoBarras'])
                        dto['itens'].pop(indx_2)
                        indx_2 = 0
                    else:
                        if dto['itens'][indx_2]['codigoBarras'] not in naoSairam:
                            naoSairam.append(dto['itens'][indx_2]['codigoBarras'])
                        indx_2 += 1
                        pass
                except:
                    logger.info(f'Popados do DTO: {sorted(sairam)}',
                                extra={**default_graylog_fields, **default_graylog_processar,
                                       'operacao': "getListMultipla",
                                       'idcotacao': conf['id_cotacao']})
                    logger.info(f'Nao popados do DTO: {sorted(naoSairam)}',
                                extra={**default_graylog_fields, **default_graylog_processar,
                                       'operacao': "getListMultipla",
                                       'idcotacao': conf['id_cotacao']})
                    sairam.clear()
                    naoSairam.clear()
                    break
        tranformar_em_sem_resposta(saida_list[0], list_sem_respostas)
    saida_list = {"respostas": saida_list}
    return saida_list


def gerarDTOsLooping(result_ordenado, minimo_de_faturamento, multipla, conf, cursor_p):
    saida_list = []
    loop = 1
    for linha_result in result_ordenado:
        loop = float("{:.2f}".format(loop))
        saida_dto = full_DTO(linha_result=linha_result,
                             minimo=minimo_de_faturamento,
                             multipla=multipla,
                             list_dict_itens=produtos_dto(linha_result, conf, cursor_p, looping=loop),
                             cursor=cursor_p)
        saida_list.append(saida_dto)
        loop += 0.10
    return saida_list


def gerarDTO(id_cotacao_processar, conf, cursor_p, multipla=False):
    versaoArquivo = conf["config_geral"]["versao_arquivo"]["versao"]
    minimo_de_faturamento = conf["config_geral"]["minimo_de_faturamento"]
    motivo_quantidade = conf["config_geral"]["motivo_por_resposta"]["quantos"]
    motivo_fixo = conf["config_geral"]["motivo_por_resposta"]["motivo"]
    qtd_aguardando = conf["config_geral"]["motivo_por_resposta"]["aguardando_resposta"]
    sem_resposta = conf['config_geral']['multipla_resposta']['sem_resposta']
    looping = conf['tipo_teste']['looping']
    logger.info('Tentando consultar as informações no banco',
                extra={**default_graylog_fields, **default_graylog_processar, 'operacao': "gerarDTO",
                       'idcotacao': id_cotacao_processar})
    result = consultar_respostas(cursor_p, id_cotacao_processar)
    if not looping:
        random.shuffle(result)  # aleatorizar lista se não for para testar o looping
    if looping:
        logger.info('Resposta gerada para testar looping, preços não correspondem ao banco',
                    extra={**default_graylog_fields, **default_graylog_processar, 'operacao': "gerarDTO",
                           'idcotacao': id_cotacao_processar})
        saida_list = gerarDTOsLooping(result, minimo_de_faturamento, multipla, conf, cursor_p)
    else:
        saida_list = []
        logger.info('Consulta no banco respondida com sucesso\nInicio da montagem do DTO',
                    extra={**default_graylog_fields, **default_graylog_processar, 'operacao': "gerarDTO",
                           'idcotacao': id_cotacao_processar})
        logger.info(f'Quantidade de respostas validas > {len(result)}',
                    extra={**default_graylog_fields, **default_graylog_processar, 'operacao': "gerarDTO",
                           'idcotacao': id_cotacao_processar})
        if qtd_aguardando:
            for _ in range(0, qtd_aguardando):
                result.pop(0)  # excluir linha do resultado para nao responder
        if motivo_quantidade > len(result):
            motivo_quantidade = len(result)
        list_motivos = get_motivos(motivo_quantidade, motivo_fixo)
        if list_motivos:
            logger.info(f'Quantidade de motivos alterados > {len(list_motivos)}',
                        extra={**default_graylog_fields, **default_graylog_processar, 'operacao': "gerarDTO",
                               'idcotacao': id_cotacao_processar})
            for motivo in list_motivos:
                if motivo == "Vencido":
                    saida_list.append(montarDTOmotivo(motivo=motivo,
                                                      linha_result=result[0],
                                                      conf=conf,
                                                      cursor=cursor_p,
                                                      multipla=multipla,
                                                      vencido=True))
                else:
                    saida_list.append(
                        montarDTOmotivo(motivo=motivo,
                                        linha_result=result[0],
                                        conf=conf,
                                        cursor=cursor_p,
                                        multipla=multipla))
                result.pop(0)
        logger.info('Inicio do processamento de DTOs validos',
                    extra={**default_graylog_fields, **default_graylog_processar, 'operacao': "gerarDTO",
                           'idcotacao': id_cotacao_processar})
        list_sem_respostas = []
        if multipla and sem_resposta:
            list_sem_respostas.extend(getListaSemResposta(produtos_dto(result[0], conf, cursor_p)))
        for linha_result in progressBar(result, prefix='Início:', suffix='Fim', length=50):
            if multipla:
                saida_list.append(getListMultipla(linha_result=linha_result,
                                                  conf=conf,
                                                  minimo_de_faturamento=minimo_de_faturamento,
                                                  versaoArquivo=versaoArquivo,
                                                  cursor_p=cursor_p,
                                                  list_sem_respostas=list_sem_respostas))
            else:
                saida_dto = full_DTO(linha_result=linha_result,
                                     versaoArquivo=versaoArquivo,
                                     list_dict_itens=produtos_dto(linha_result, conf, cursor_p),
                                     minimo=minimo_de_faturamento,
                                     cursor=cursor_p)
                saida_list.append(saida_dto)
    return saida_list


def consultar_respostas(cursor, cote):
    cursor.execute(query_fullDTO(cote))
    return cursor.fetchall()


def aleatorizar_resposta_itens(dto):
    if len(dto['itens']) > 1:
        for _ in range(0, random.randint(1, int(len(dto['itens']) / random.uniform(1.01, 2)))):
            dto['itens'].pop(-1)  # função criara para aleatorizar respostas de multipla, removendo itens
        return dto
    else:
        return dto


def gravarDTO(dto):
    try:
        with io.open('arquivos_config/backup_ultima_cotacao_processada.json', 'r', encoding='utf-8'):
            lista_vazia = []
            with open('arquivos_config/backup_ultima_cotacao_processada.json', 'w', encoding='utf-8') as f:
                json.dump(lista_vazia, f, ensure_ascii=False)
        with io.open('arquivos_config/backup_ultima_cotacao_processada.json', 'r', encoding='utf-8') as file:
            lista_atual = json.load(file)
            lista_atual.append(dto)
            with open('arquivos_config/backup_ultima_cotacao_processada.json', 'w', encoding='utf-8') as f:
                json.dump(lista_atual, f, ensure_ascii=False)
    except:
        with open('arquivos_config/backup_ultima_cotacao_processada.json', 'w', encoding='utf-8') as f:
            f.write('[]')
            gravarDTO(dto)
        return


def trazer_uma_unica_filial(list_dto_mc):
    result_list = []
    for resposta in list_dto_mc:
        for dto in resposta['respostas']:
            if dto['filiais'][0]['cnpj'] == '45776051000148':
                result_list.append(dto)  # rotina criada para trazer apenas uma filial no DTO
    return result_list


def checarVencimento(cursor, cote):
    brasilia_timezone = pytz.timezone('America/Sao_Paulo')
    data_hora_atual = datetime.datetime.now(brasilia_timezone)
    formato = "%Y-%m-%d %H:%M:%S"
    timeDate = data_hora_atual.strftime(formato).split(' ')
    anoMesDia = timeDate[0].split('-')
    horaMin = timeDate[1].split(':')
    cursor.execute(getVencimento(cote))
    vencimento = cursor.fetchall()
    vencimentoDb = vencimento[0][0]
    anoSys = anoMesDia[0]
    mesSys = anoMesDia[1]
    diaSys = anoMesDia[2]
    horaSys = horaMin[0]
    minSys = horaMin[1]
    anoDb = vencimentoDb.strftime('%Y')
    mesDb = vencimentoDb.strftime('%m')
    diaDb = vencimentoDb.strftime('%d')
    horaDb = vencimentoDb.strftime('%H')
    minDb = vencimentoDb.strftime('%M')
    if anoSys == anoDb and mesSys == mesDb and diaSys <= diaDb:
        if horaSys < horaDb:
            return True
        if horaSys == horaDb:
            if minSys < minDb:
                return True
    return False


def get_motivos(motivo_motivo, motivo_fixo=None):
    list_return = []
    list_motivos = [
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
        "Expedição não encontrada. Entre em contato com o suporte "
        "da Cotefacil através do chat ou por um de nossos telefones.",
        "Condição de pagamento inválida ou não disponível para o cliente.",
        "Cliente com documentacao vencida, verificar no portal."
    ]
    for i in range(0, motivo_motivo):
        if motivo_fixo:
            list_return.append('motivo_fixo')
        else:
            index = random.randint(0, len(list_motivos) - 1)
            list_return.append(list_motivos[index])
    return list_return


def enviar_respostas(list_dtos, oficial):
    contador = 1
    for dto in list_dtos:
        try:
            cnpj_fornecedor = dto['respostas'][0]['cnpjFornecedor']
            idrepresentante = dto['respostas'][0]['codigoRepresentante']
            cnpj = dto['respostas'][0]['filiais'][0]["cnpj"]
            motivo = dto['respostas'][0]['filiais'][0]["motivo"]
            multipla_mensagem = ' > multipla resposta'
        except:  # tudo que nao for multipla cairá aqui
            cnpj_fornecedor = dto['cnpjFornecedor']
            idrepresentante = dto['codigoRepresentante']
            cnpj = dto['filiais'][0]["cnpj"]
            motivo = dto['filiais'][0]["motivo"]
            multipla_mensagem = ''
        logger.info(f'Enviando DTO > {str(contador)}\n'
                    f'> cliente: {cnpj}{multipla_mensagem}\n'
                    f'> fornecedor: {cnpj_fornecedor}\n'
                    f'> idrepresentante: {idrepresentante}\n'
                    f'> motivo: {motivo}',
                    extra={**default_graylog_fields, **default_graylog_processar, 'operacao': "enviar_respostas"})
        contador += 1
        if oficial:
            strategy_oficial.send(data=dto)
        else:
            strategy.send(data=dto)


def motivoResposta(dto):
    try:
        motivo = dto['respostas'][0]['filiais'][0]["motivo"]
    except Exception:
        motivo = dto['filiais'][0]["motivo"]
    return motivo


def alterar_vencimento(body):
    idcotacao = body['idcotacao']
    quantos_dias = body['quantos_dias']
    para_mais = body['para_mais']
    de = body['motivoresposta_de']
    para = body['motivoresposta_para']
    cursor = conectar_banco()
    cursor.execute(update_resposta_cliente(idcotacao, quantos_dias, para_mais))
    cursor.execute(update_pedidomanualitemresposta(idcotacao, quantos_dias, para_mais))
    cursor.execute(update_pedido(idcotacao, quantos_dias, para_mais))
    cursor.execute(update_cotacao(idcotacao, quantos_dias, para_mais))
    cursor.execute(update_vencido_para_respondido(idcotacao, de, para))
    cursor.connection.commit()
    cursor.close()


def excluir_registros(body):
    idcotacao = body['idcotacao']
    cursor = conectar_banco()
    cursor.execute(delete_negociacaodetalhecliente(idcotacao))
    cursor.execute(delete_respostaclienteitem(idcotacao))
    cursor.execute(delete_produtosemresposta(idcotacao))
    cursor.execute(delete_respostacliente(idcotacao))
    cursor.connection.commit()
    cursor.close()


def mudar_para_em_analise(body):
    idcotacao = body['idcotacao']
    cursor = conectar_banco()
    cursor.execute(voltar_para_em_analise_1(idcotacao))
    cursor.execute(voltar_para_em_analise_2(idcotacao))
    cursor.execute(voltar_para_em_analise_3(idcotacao))
    cursor.execute(voltar_para_em_analise_4(idcotacao))
    cursor.execute(voltar_para_em_analise_5(idcotacao))
    cursor.connection.commit()
    cursor.close()


def processar(config, oficial=False):
    logger.info('Processamento de DTO iniciado', extra={**default_graylog_fields, **default_graylog_processar,
                                                        'idcotacao': config["id_cotacao"]})
    logger.info('Checando vencimento da cotação', extra={**default_graylog_fields, **default_graylog_processar,
                                                         'idcotacao': config["id_cotacao"]})
    cursor_p = conectar_banco(oficial)
    try:
        id_cotacao_processar = config["id_cotacao"]
    except TypeError:  # ajuste para resolver em casos de captura de mensagem da fila (str)
        config = json.loads(config)
        id_cotacao_processar = config['id_cotacao']
    except Exception:
        logger.exception('Erro não esperado no methodo processar [main_response_quotation.processar]:',
                         extra={**default_graylog_fields,
                                **default_graylog_processar})
        return
    if not checarVencimento(cursor_p, id_cotacao_processar):
        logger.info('Cotação está vencida, altere o vencimento',
                    extra={**default_graylog_fields, **default_graylog_processar, 'idcotacao': id_cotacao_processar})
    else:
        logger.info('Dentro do vencimento, OK', extra={**default_graylog_fields, **default_graylog_processar,
                                                       'idcotacao': id_cotacao_processar})
        multipla = config["config_geral"]["multipla_resposta"]["multipla"]
        aleatorizar_resposta = config["config_geral"]["aleatorizar_quantidade_respondida"]
        if multipla:  # c/ multipla
            list_dto = gerarDTO(id_cotacao_processar, config, cursor_p, multipla=True)
        else:  # s/ multipla
            list_dto = gerarDTO(id_cotacao_processar, config, cursor_p)
        cursor_p.close()
        logger.info('Cursor de conexão com o banco fechado',
                    extra={**default_graylog_fields, **default_graylog_processar,
                           'idcotacao': id_cotacao_processar})
        gravarDTO(list_dto)
        sucessos = []
        demais_motivos = []
        for dto in list_dto:
            if aleatorizar_resposta:
                dto = aleatorizar_resposta_itens(dto)
            if motivoResposta(dto) == "Sucesso":
                sucessos.append(dto)
            else:
                demais_motivos.append(dto)
        enviar_respostas(demais_motivos, oficial=oficial)
        enviar_respostas(sucessos, oficial=oficial)
        logger.info('Processo finalizado com sucesso',
                    extra={**default_graylog_fields, **default_graylog_processar,
                           'idcotacao': id_cotacao_processar})


def abrirArquivo(nome):
    with open(nome, 'r') as file:
        return json.load(file)


def main():
    config_gerais = abrirArquivo('arquivos_config/config_geral.json')
    try:
        processar(config_gerais)
    except Exception:
        logger.exception('Erro no processamento da mensagem ou parada forçada',
                         extra={**default_graylog_fields, **default_graylog_processar, 'operacao': "main"})


if __name__ == '__main__':
    main()
