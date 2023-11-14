import io
import json
import datetime
from connect_db import conectar_ao_banco
from cotefacilib.utils.send_to_s3 import strategy, strategy_oficial
from cotefacilib.utils.full_DTO import *


def conectar_banco(oficial):
    return conectar_ao_banco(oficial)


def randomizar_lista_porcentagem(porcentagem_minima, porcentagem_maxima, lista):
    if len(lista) != 1:
        quantidade_minima = int(len(lista) * (porcentagem_minima / 100))
        quantidade_maxima = int(len(lista) * (porcentagem_maxima / 100))
        quantidade_randomizada = random.randint(quantidade_minima, quantidade_maxima)
        lista_randomizada = random.sample(lista, quantidade_randomizada)
        return lista_randomizada
    return lista


def produtos_dto(linha_result, problemas_prop, cursor_db):
    id_rep = linha_result[0]
    cnpj_cliente = linha_result[1]
    id_cotacao_processar = linha_result[3]
    cursor_db.execute(query_get_prod_conjunta(id_cotacao_processar, id_rep, cnpj_cliente))
    produtos = cursor_db.fetchall()
    if not produtos:
        cursor_db.execute(query_get_prod_simples(id_cotacao_processar, id_rep))
        produtos = cursor_db.fetchall()
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
        if not quantidade_de_resposta_produto:
            codbarras = produto[0]
            qtd_cotada = produto[1]
            valor_historico = produto[2]
            if minimo > 0:
                saida_json = itens_DTO(codbarras,
                                       qtd_cotada,
                                       valor_historico,
                                       qtd_problema_minimo=True,
                                       cashBack=cashBackDesconto)
                minimo -= 1
            elif embalagem > 0:
                saida_json = itens_DTO(codbarras,
                                       qtd_cotada,
                                       valor_historico,
                                       qtd_problema_embalagem=True,
                                       cashBack=cashBackDesconto)
                embalagem -= 1
            elif sem_st > 0:
                saida_json = itens_DTO(codbarras,
                                       qtd_cotada,
                                       valor_historico,
                                       qtd_sem_st=True,
                                       cashBack=cashBackDesconto)
                sem_st -= 1
            elif com_st > 0:
                saida_json = itens_DTO(codbarras, qtd_cotada, valor_historico, com_st=True, cashBack=cashBackDesconto)
                com_st -= 1
            elif sem_estoque:
                saida_json = itens_DTO(codbarras,
                                       qtd_cotada,
                                       valor_historico,
                                       sem_estoque=True,
                                       atende=atende,
                                       cashBack=cashBackDesconto)
            elif oportunidades > 0:
                saida_json = itens_DTO(codbarras,
                                       qtd_cotada,
                                       valor_historico,
                                       oportunidades=True,
                                       cashBack=cashBackDesconto)
                oportunidades -= 1
            elif oportunidades_fixada > 0:
                saida_json = itens_DTO(codbarras,
                                       qtd_cotada,
                                       valor_historico,
                                       oportunidades_fixada=True,
                                       cashBack=cashBackDesconto)
                oportunidades_fixada -= 1
            else:
                saida_json = itens_DTO(codbarras,
                                       qtd_cotada,
                                       valor_historico,
                                       cashBack=cashBackDesconto)
            saida_list.append(saida_json)
        else:
            quantidade_de_resposta_produto -= 1
        if cashBackQuantidade:
            cashBackQuantidade -= 1
    return saida_list


def montarDTOmotivo(motivo, linha_result, conf, cursor, multipla):
    versaoArquivo = conf["config_geral"]["versao_arquivo"]["versao"]
    minimo_de_faturamento = conf["config_geral"]["minimo_de_faturamento"]
    if motivo != 'Sucesso':
        list_dict_itens = []  # lista vazia, pois com motivos nao tem itens
    else:  # se for sucesso o motivo
        if multipla:
            return getListMultipla(linha_result=linha_result,
                                   cursor_p=cursor,
                                   minimo_de_faturamento=minimo_de_faturamento,
                                   versaoArquivo=versaoArquivo,
                                   conf=conf)
        else:
            list_dict_itens = produtos_dto(linha_result=linha_result, problemas_prop=conf, cursor_db=cursor)
            motivo = None  # por que para sucesso o full DTO ja seta a string sucesso como padrão
    return full_DTO(linha_result=linha_result,
                    versaoArquivo=versaoArquivo,
                    list_dict_itens=list_dict_itens,
                    minimo=minimo_de_faturamento,
                    list_motivo=motivo,
                    cursor=cursor)


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


def getListMultipla(linha_result,
                    conf,
                    minimo_de_faturamento,
                    versaoArquivo,
                    cursor_p):
    saida_list = []
    for num in range(1, random.randint(3, 7)):  # quantidade de multiplas por representante (10)
        saida_dto = full_DTO(linha_result=linha_result,
                             versaoArquivo=versaoArquivo,
                             list_dict_itens=produtos_dto(linha_result, conf, cursor_p),
                             minimo=minimo_de_faturamento,
                             cursor=cursor_p)
        saida_list.append(saida_dto)
    saida_list = {"respostas": saida_list}
    return saida_list


def gerarDTO(id_cotacao_processar, conf, cursor_p, multipla=False):
    versaoArquivo = conf["config_geral"]["versao_arquivo"]["versao"]
    minimo_de_faturamento = conf["config_geral"]["minimo_de_faturamento"]
    motivo_quantidade = conf["config_geral"]["motivo_por_resposta"]["quantos"]
    motivo_fixo = conf["config_geral"]["motivo_por_resposta"]["motivo"]
    qtd_aguardando = conf["config_geral"]["motivo_por_resposta"]["aguardando_resposta"]
    logs(tipo=1, mensagem=logs(tipo=2) + ': Tentando consultar as informações no banco')
    result = consultar_respostas(cursor_p, id_cotacao_processar)
    random.shuffle(result)  # aleatorizar lista
    saida_list = []
    logs(tipo=1, mensagem=logs(tipo=2) + ': Consulta no banco respondida com sucesso')
    logs(tipo=1, mensagem=logs(tipo=2) + ': Inicio da montagem do DTO')
    logs(tipo=1, mensagem=logs(tipo=2) + f': Quantidade de respostas validas > {len(result)}')
    if qtd_aguardando:
        for _ in range(0, qtd_aguardando):
            result.pop(0)  # excluir linha do resultado para nao responder
    if motivo_quantidade > len(result):
        motivo_quantidade = len(result)
    list_motivos = get_motivos(motivo_quantidade, motivo_fixo)
    if list_motivos:
        logs(tipo=1, mensagem=logs(tipo=2) + f': Quantidade de motivos alterados > {len(list_motivos)}')
        for motivo in list_motivos:
            saida_list.append(
                montarDTOmotivo(motivo=motivo, linha_result=result[0], conf=conf, cursor=cursor_p, multipla=multipla))
            result.pop(0)
    logs(tipo=1, mensagem=logs(tipo=2) + ': Inicio do processamento de DTOs validos')
    for linha_result in progressBar(result, prefix='Início:', suffix='Fim', length=50):
        if multipla:
            saida_list.append(getListMultipla(linha_result=linha_result,
                                              conf=conf,
                                              minimo_de_faturamento=minimo_de_faturamento,
                                              versaoArquivo=versaoArquivo,
                                              cursor_p=cursor_p))
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


def logs(tipo, mensagem=None):
    if tipo == 1 and mensagem:
        print(mensagem)
    if tipo == 2:
        data_hora_atual = datetime.datetime.now()
        formato = "%Y-%m-%d %H:%M:%S"
        data_hora_formatada = data_hora_atual.strftime(formato)
        return data_hora_formatada
    else:
        return None


def gravarDTO(DTO, limpar=False):
    try:
        if limpar:
            with io.open('DTO_processado.json', 'r', encoding='utf-8') as file:
                lista_vazia = []
                file.close()
                with open('DTO_processado.json', 'w', encoding='utf-8') as f:
                    json.dump(lista_vazia, f, ensure_ascii=False)
                    f.close()
            return True
        with io.open('DTO_processado.json', 'r', encoding='utf-8') as file:
            lista_atual = json.load(file)
            lista_atual.append(DTO)
            file.close()
            with open('DTO_processado.json', 'w', encoding='utf-8') as f:
                json.dump(lista_atual, f, ensure_ascii=False)
                f.close()
        return True
    except ValueError:
        return


def trazer_uma_unica_filial(list_dto_mc):
    result_list = []
    for resposta in list_dto_mc:
        for dto in resposta['respostas']:
            if dto['filiais'][0]['cnpj'] == '45776051000148':
                result_list.append(dto)  # rotina criada para trazer apenas uma filial no DTO
    return result_list


def checarVencimento(cursor, cote):
    timeDate = logs(tipo=2).split(' ')
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
        except Exception:  # tudo que nao for multipla cairá aqui
            cnpj_fornecedor = dto['cnpjFornecedor']
            idrepresentante = dto['codigoRepresentante']
            cnpj = dto['filiais'][0]["cnpj"]
            motivo = dto['filiais'][0]["motivo"]
            multipla_mensagem = ''
        logs(tipo=1, mensagem=logs(tipo=2)
                              + f': Enviando DTO > {str(contador)} '
                                f'> cliente: {cnpj}{multipla_mensagem} '
                                f'> fornecedor: {cnpj_fornecedor} '
                                f'> idrepresentante: {idrepresentante}'
                                f'> motivo: {motivo}')
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


def processar(config, oficial):
    logs(tipo=1, mensagem=logs(tipo=2) + ': Processamento de DTO iniciado >>>')
    logs(tipo=1, mensagem=logs(tipo=2) + ': Checando vencimento da cotação...')
    cursor_p = conectar_banco(oficial)
    id_cotacao_processar = config["id_cotacao"]
    if not checarVencimento(cursor_p, id_cotacao_processar):
        logs(tipo=1, mensagem=logs(tipo=2) + ': Cotação está vencida, altere o vencimento >>>')
    else:
        logs(tipo=1, mensagem=logs(tipo=2) + ': Dentro do vencimento, OK')
        multipla = config["config_geral"]["multipla_resposta"]["multipla"]
        aleatorizar_resposta = config["config_geral"]["aleatorizar_quantidade_respondida"]
        if multipla:  # c/ multipla
            list_dto = gerarDTO(id_cotacao_processar, config, cursor_p, multipla=True)
        else:  # s/ multipla
            list_dto = gerarDTO(id_cotacao_processar, config, cursor_p)
        cursor_p.close()
        logs(tipo=1, mensagem=logs(tipo=2) + ': Cursor de conexão com o banco fechado')
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
        logs(tipo=1, mensagem=logs(tipo=2) + ': Processo finalizado com sucesso')


def abrirArquivo(nome):
    with open(nome, 'r') as file:
        return json.load(file)


def main():
    config_gerais = abrirArquivo('config_geral.json')
    try:
        gravarDTO(DTO=None, limpar=True)
        processar(config_gerais, oficial=False)
    except Exception as Error:
        logs(tipo=1, mensagem='>>> Erro no processamento da mensagem ou parada forçada')
        print(Error)


if __name__ == '__main__':
    main()
