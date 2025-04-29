import time
from datetime import date, timedelta
from database.queries import query_get_prod_conjunta, query_get_prod_simples, get_cotacao_fake_query
import random
from processos.cotacao.utils import utils
from server.instance import log

logger = log.logger
default_graylog_fields = log.default_graylog_fields


def _remover_itens_sem_resposta(dto_list, codigos_sem_resposta):
    for dto in dto_list:
        itens = dto.get("itens", [])
        removidos = []
        dto["itens"] = [
            item for item in itens
            if item["codigoBarras"] not in codigos_sem_resposta or removidos.append(item["codigoBarras"])
        ]
        mantidos = [
            item["codigoBarras"]
            for item in itens
            if item["codigoBarras"] not in removidos
        ]


def get_list_multipla_v2(
        linha_result,
        payload,
        cursor,
        list_sem_respostas=None,
        vencido=False,
        tipo_pagamento=0,
        randomizar=False
):
    cfg = payload.config_geral
    versao = cfg.versao_arquivo
    minimo_faturamento = cfg.minimo_de_faturamento
    combo = cfg.multipla_resposta.combo

    sem_resposta = payload.config_geral.multipla_resposta.sem_resposta
    saida_list = []
    total_variacoes = random.randint(3, 7)
    posicao_combo = random.randint(1, total_variacoes - 1)
    for i in range(1, total_variacoes):
        is_combo = combo and i == posicao_combo
        itens = produtos_dto_v2(
            linha_result,
            payload,
            cursor,
            combo=is_combo if combo else False
        )
        dto = juntar_nivel_fornecedor_com_itens_dto_full_dt_v2(
            linha_result=linha_result,
            versao_arquivo=versao,
            list_dict_itens=itens,
            minimo=minimo_faturamento,
            cursor=cursor,
            multipla=i,
            vencido=vencido,
            tipoPagamento=tipo_pagamento,
            randomizar=randomizar
        )
        if is_combo:
            dto['combo'] = True

        saida_list.append(dto)
    if sem_resposta:
        _remover_itens_sem_resposta(saida_list, list_sem_respostas)
        utils.tranformar_em_sem_resposta(saida_list[0], list_sem_respostas)
    return {"respostas": saida_list}


def get_cotacao_fake(filial, cotacao_matriz, cursor):
    cursor.execute(get_cotacao_fake_query(cotacao_matriz, filial))
    result = cursor.fetchall()
    if not result:
        return
    return result[0][0]


def full_dto(linha_result,
             list_dict_itens,
             cursor,
             tipoPagamento=0,
             randomizar=False,
             versao_arquivo='4.2',
             multipla=0,
             minimo=False,
             list_motivo=None,
             vencido=False):
    id_rep = linha_result[0]
    cli_cnpj = linha_result[1]
    cnpj_forn = linha_result[2]
    id_cot_real = linha_result[3]
    id_cote = get_cotacao_fake(cli_cnpj, id_cot_real, cursor)
    cond_dias = linha_result[4]
    if not id_cote:
        id_cote = id_cot_real
    result_dto = {
        "cnpjFornecedor": cnpj_forn,
        "codigoCotacao": id_cote,
        "versaoArquivo": versao_arquivo,
        "itens": list_dict_itens,
        "filiais": func_filial_dto(cli_cnpj, list_motivo),
        "cotacaoVencida": False,
        "respostaPE": False,
        "todosItensBloqueados": False,
        "dataValidade": int(time.mktime(date.today().timetuple()) * 1000),
        "codigoRepresentante": id_rep
    }
    if vencido:
        result_dto['validade'] = (date.today() - timedelta(days=1)).strftime('%d%m%Y')
    else:
        result_dto['validade'] = (date.today() + timedelta(days=20)).strftime('%d%m%Y')
    if minimo:
        result_dto['minimoFaturamento'] = minimo
    else:
        result_dto['minimoFaturamento'] = random.randint(20, 50)
    if multipla and multipla >= 1:
        if multipla == 1:
            result_dto['codigoCondicaoPagamento'] = 'MINHA CONDIÇÃO'
            result_dto['prazoPagamento'] = cond_dias
            result_dto['prazoPagamentoDescricao'] = 'A prazo, Minha condição'
        else:
            result_dto['codigoCondicaoPagamento'] = f'{multipla}-{random.randint(1, 99)}-{random.randint(1, 999)}'
            result_dto['prazoPagamento'] = multipla + random.randint(1, 100)
            result_dto['prazoPagamentoDescricao'] = f"Oferta, {multipla}:{id_rep}"
    else:
        result_dto['codigoCondicaoPagamento'] = 'MINHA CONDIÇÃO'
        result_dto['prazoPagamento'] = cond_dias
        result_dto['prazoPagamentoDescricao'] = 'Teste, Áá@#$Rã, 123//**'
    if tipoPagamento or tipoPagamento == 0:
        result_dto['tipoPagamento'] = utils.randomizar_tipoPagamento(randomizar=randomizar, tipoPagamento=tipoPagamento)
    return result_dto


def _preencher_condicoes_pagamento(dto, multipla, cond_dias, id_rep, tipoPagamento, randomizar):
    if multipla and multipla >= 1:
        if multipla == 1:
            dto['codigoCondicaoPagamento'] = 'MINHA CONDIÇÃO'
            dto['prazoPagamento'] = cond_dias
            dto['prazoPagamentoDescricao'] = 'A prazo, Minha condição'
        else:
            dto['codigoCondicaoPagamento'] = f'{multipla}-{random.randint(1, 99)}-{random.randint(1, 999)}'
            dto['prazoPagamento'] = multipla + random.randint(1, 100)
            dto['prazoPagamentoDescricao'] = f"Oferta, {multipla}:{id_rep}"
    else:
        dto['codigoCondicaoPagamento'] = 'MINHA CONDIÇÃO'
        dto['prazoPagamento'] = cond_dias
        dto['prazoPagamentoDescricao'] = 'Teste, Áá@#$Rã, 123//**'
    if tipoPagamento or tipoPagamento == 0:
        dto['tipoPagamento'] = utils.randomizar_tipoPagamento(randomizar=randomizar, tipoPagamento=tipoPagamento)


def juntar_nivel_fornecedor_com_itens_dto_full_dt_v2(
        linha_result,
        list_dict_itens,
        cursor,
        versao_arquivo='4.2',
        multipla=0,
        minimo=False,
        list_motivo=None,
        vencido=False,
        tipoPagamento=0,
        randomizar=False
):
    id_rep = linha_result[0]
    cli_cnpj = linha_result[1]
    cnpj_forn = linha_result[2]
    id_cot_real = linha_result[3]
    cond_dias = linha_result[4]
    id_cote = get_cotacao_fake(cli_cnpj, id_cot_real, cursor) or id_cot_real
    validade_data = (
        (date.today() - timedelta(days=1)) if vencido
        else (date.today() + timedelta(days=20))
    )
    result_dto = {
        "cnpjFornecedor": cnpj_forn,
        "codigoCotacao": id_cote,
        "versaoArquivo": versao_arquivo,
        "itens": list_dict_itens,
        "filiais": func_filial_dto(cli_cnpj, list_motivo),
        "cotacaoVencida": False,
        "respostaPE": False,
        "todosItensBloqueados": False,
        "dataValidade": int(time.mktime(date.today().timetuple()) * 1000),
        "validade": validade_data.strftime('%d%m%Y'),
        "codigoRepresentante": id_rep,
        "minimoFaturamento": minimo if minimo else random.randint(20, 50),
    }
    _preencher_condicoes_pagamento(result_dto, multipla, cond_dias, id_rep, tipoPagamento, randomizar)
    return result_dto


def gerar_dto_v2(payload, cursor):
    cotacao = payload.idcotacao
    cfg = payload.config_geral
    tipo_teste = payload.tipo_teste
    versao = cfg.versao_arquivo
    minimo_faturamento = cfg.minimo_de_faturamento
    motivo_qtd = cfg.motivo_por_resposta.quantos
    motivo_fixo = cfg.motivo_por_resposta.motivo
    qtd_aguardando = cfg.motivo_por_resposta.aguardando_resposta
    multipla = cfg.multipla_resposta.multipla
    combo = cfg.multipla_resposta.combo
    sem_resposta = cfg.multipla_resposta.sem_resposta
    looping = tipo_teste.looping
    tipo_pagamento = payload.config_geral.opcoes_tipo_pagamento.tipoPagamento
    randomizar = payload.config_geral.opcoes_tipo_pagamento.random
    respostas = utils.consultar_respostas(cursor, cotacao)
    if not looping:  # aloatorizar lista se nao for looping
        random.shuffle(respostas)
    if looping:
        return utils.gerar_dto_looping(respostas, minimo_faturamento, multipla, payload, cursor, randomizar=randomizar,
                                       tipo_pagamento=tipo_pagamento)
    respostas = respostas[qtd_aguardando:]
    motivo_qtd = min(motivo_qtd, len(respostas))
    lista_motivos = utils.get_motivos(motivo_qtd, motivo_fixo)
    saida_list = []
    if lista_motivos:
        logger.info(f'Quantidade de motivos alterados > {len(lista_motivos)}',
                    extra={**default_graylog_fields, 'operacao': "gerarDTO", 'idcotacao': cotacao})
        for motivo in lista_motivos:
            dto = montar_dto_motivo(
                motivo=motivo,
                linha_result=respostas.pop(0),
                payload=payload,
                cursor=cursor,
                multipla=multipla,
                vencido=(motivo == "Vencido")
            )
            saida_list.append(dto)
    list_sem_respostas = []
    if multipla and sem_resposta and respostas:
        list_sem_respostas = utils.get_lista_sem_resposta(produtos_dto_v2(respostas[0], payload, cursor))
    for linha in utils.progress_bar(respostas, prefix='Início:', suffix='Fim', length=50):
        if multipla or combo:
            dto = get_list_multipla_v2(
                linha_result=linha,
                payload=payload,
                cursor=cursor,
                list_sem_respostas=list_sem_respostas,
                tipo_pagamento=tipo_pagamento,
                randomizar=randomizar
            )
        else:
            dto = juntar_nivel_fornecedor_com_itens_dto_full_dt_v2(
                linha_result=linha,
                versao_arquivo=versao,
                list_dict_itens=produtos_dto_v2(linha_result=linha, payload_prod=payload, cursor_db=cursor),
                minimo=minimo_faturamento,
                cursor=cursor,
                tipoPagamento=tipo_pagamento,
                randomizar=randomizar
            )
        saida_list.append(dto)
    return saida_list


def func_filial_dto(cnpj_cliente_filial_dto, motivo):
    if motivo:
        list_filial_dto = [{
            "cnpj": f'{cnpj_cliente_filial_dto}',
            "atende": "N",
            "motivo": f'{motivo}'
        }]
    else:
        list_filial_dto = [{
            "cnpj": f"{cnpj_cliente_filial_dto}",
            "atende": "S",
            "motivo": "Sucesso"
        }]
    return list_filial_dto


def itens_dto(produto,
              qtd_problema_minimo=False,
              qtd_problema_embalagem=False,
              qtd_sem_st=False,
              com_st=False,
              nao_encontrado=False,
              sem_estoque=False,
              oportunidades=False,
              oportunidades_fixada=False,
              atende=0,
              cash_back=False,
              looping=0.0,
              zero_esquerda=False,
              combo=False):
    codbarras = produto[0]
    qtd_cotada = produto[1]
    valor_historico = produto[2]
    medicamento = produto[3]
    if float(valor_historico) <= 0:
        valor_historico = random.randint(2, 20)
    desconto = random.randint(0, 20)
    products_dto = {
        "codigoBarras": "0" + codbarras if zero_esquerda else codbarras,
        "qtde": qtd_cotada,
        "quantidadeMinima": 0,
        "minimoUnidades": 0,
        "descontoInformado": desconto,
        "precoFabrica": 0,
        "tipoLista": "N",
        "controlePreco": 'L',
        "tipoEmbalagem": "U",
        "qtdeCaixa": 1,
        "descontoAdicionalOl": random.randint(0, 20),
        "descontoBonificado": random.randint(0, 20),
        "percentualRepasse": 0.0,
        "valorSemST": valor_historico,
        "descontoMidia": 0.0,
        "codFaturamentoPromocao": "Padrao",
        "analisePreco": "1"
    }
    if medicamento:
        products_dto['controlePreco'] = 'M'
    if looping:
        products_dto['preco'] = valor_historico
    else:
        products_dto['preco'] = float(valor_historico) + random.uniform(1, 2)  # Simulando um cenario real
    if qtd_problema_minimo:
        products_dto['quantidadeMinima'] = int(qtd_cotada) + 5
        products_dto['minimoUnidades'] = int(qtd_cotada) + 5
    elif qtd_problema_embalagem:
        products_dto['tipoEmbalagem'] = 'C'
        products_dto['qtdeCaixa'] = int(qtd_cotada) + 11
    elif qtd_sem_st:
        products_dto['analisePreco'] = '0'
        products_dto['preco'] = valor_historico
    elif com_st:
        products_dto['valorSemST'] = 0
    elif nao_encontrado:
        products_dto['atende'] = 2
        products_dto['qtde'] = 0
    elif sem_estoque:
        products_dto['atende'] = 1
        products_dto['qtde'] = 0
    elif oportunidades:
        def gerar_oportunidade(cotada, desc, hist, primeira=False):
            quantidade_minima = cotada * 2 + random.randint(0, 20) + random.randint(0, 20)
            return {
                "quantidadeMinima": cotada + 3 if primeira else quantidade_minima,
                "desconto": desc + 1,
                "codigoFaturamento": "Oportunidade",
                "precoComST": hist,
                "precoSemST": float(hist) + random.uniform(1, 2),
                "quantidadeFixa": False
            }

        products_dto['oportunidades'] = [
            gerar_oportunidade(qtd_cotada, desconto, valor_historico, primeira=True),
            gerar_oportunidade(qtd_cotada, desconto, valor_historico),
            gerar_oportunidade(qtd_cotada, desconto, valor_historico)
        ]
    elif oportunidades_fixada:
        products_dto['oportunidades'] = [{
            "quantidadeMinima": qtd_cotada + 3,
            "desconto": desconto + 1,
            "codigoFaturamento": "Oportunidade",  # codigo|dia
            "precoComST": valor_historico,
            "precoSemST": float(valor_historico) + random.uniform(1, 2),
            "quantidadeFixa": True
        }]
    try:
        if cash_back is True:
            products_dto['valorProdutoBoleto'] = float(valor_historico) / random.uniform(1.01, 3)
        if cash_back > 0:
            products_dto['descontoFinanceiro'] = cash_back
        if cash_back == 0:
            products_dto['descontoFinanceiro'] = random.uniform(0, 95)
            products_dto['valorProdutoBoleto'] = float(valor_historico) / random.uniform(1.01, 3)
    except:
        pass
    if atende:
        products_dto['atende'] = atende
    if combo:
        products_dto['quantidadeMinima'] = int(qtd_cotada) + 5
        products_dto['quantidadeMaxima'] = int(qtd_cotada) + 50
        products_dto['bonificado'] = 'true'
        products_dto['obrigatorio'] = 'true'
    return products_dto


def montar_dto_motivo(motivo, linha_result, payload, cursor, multipla, vencido=False):
    versao_arquivo = payload.config_geral.versao_arquivo.versao

    minimo_de_faturamento = payload.config_geral.minimo_de_faturamento
    if motivo == 'Sucesso' or motivo == 'Vencido':  # se for sucesso o motivo
        if multipla:
            return get_list_multipla_v2(linha_result=linha_result,
                                     cursor=cursor,
                                     payload=payload,
                                     vencido=vencido)
        else:
            list_dict_itens = produtos_dto_v2(linha_result=linha_result, payload_prod=payload, cursor_db=cursor)
            motivo = None  # por que para sucesso o full DTO ja seta a string sucesso como padrão
    else:  # se nao for sucesso o motivo
        list_dict_itens = []  # lista vazia, pois com motivos nao tem itens
    return full_dto(linha_result=linha_result,
                    versao_arquivo=versao_arquivo,
                    list_dict_itens=list_dict_itens,
                    minimo=minimo_de_faturamento,
                    list_motivo=motivo,
                    cursor=cursor,
                    vencido=vencido)


def _buscar_produtos(cursor, id_cot, id_rep, cnpj_cli, relacionados):
    cursor.execute(query_get_prod_conjunta(id_cot, id_rep, cnpj_cli, relacionados))
    produtos = cursor.fetchall()
    if not produtos:
        cursor.execute(query_get_prod_simples(id_cot, id_rep, relacionados))
        produtos = cursor.fetchall()
    return produtos


def _processar_produto(produto, cfg, looping, combo):
    kwargs = {
        "cash_back": cfg['cashback_pct'],
        "looping": looping,
        "zero_esquerda": cfg['zero_esquerda']
    }
    if combo:
        kwargs["combo"] = True
        return itens_dto(produto, **kwargs)
    if cfg['minimo'] > 0:
        cfg['minimo'] -= 1
        return itens_dto(produto, qtd_problema_minimo=True, **kwargs)
    if cfg['embalagem'] > 0:
        cfg['embalagem'] -= 1
        return itens_dto(produto, qtd_problema_embalagem=True, **kwargs)
    if cfg['sem_st'] > 0:
        cfg['sem_st'] -= 1
        return itens_dto(produto, qtd_sem_st=True, **kwargs)
    if cfg['com_st'] > 0:
        cfg['com_st'] -= 1
        return itens_dto(produto, com_st=True, **kwargs)
    if cfg['sem_estoque']:
        return itens_dto(produto, sem_estoque=True, atende=cfg['atende'], **kwargs)
    if cfg['oportunidades'] > 0:
        cfg['oportunidades'] -= 1
        return itens_dto(produto, oportunidades=True, **kwargs)
    if cfg['oportunidades_fixada'] > 0:
        cfg['oportunidades_fixada'] -= 1
        return itens_dto(produto, oportunidades_fixada=True, **kwargs)
    return itens_dto(produto, **kwargs)


def _carregar_config(payload):
    config_geral = payload.config_geral
    config_prod = payload.config_produto
    if config_geral.versao_arquivo == '4.2':
        cashback_qtd = config_prod.cashback_4_2.qtd_de_itens
        cashback_pct = config_prod.cashback_4_2.porcentagem_cashback
    else:
        cashback_qtd = config_prod.cashback_4_3.qtd_de_itens
        cashback_pct = 0
    return {
        "minimo": config_prod.qtd_problema_de_minimo,
        "embalagem": config_prod.qtd_problema_de_embalagem,
        "sem_st": config_prod.produtos_sem_st,
        "com_st": config_prod.so_com_st,
        "sem_estoque": config_prod.sem_estoque,
        "resposta_parcial": config_geral.resposta_parcial_em_porcentagem,
        "atende": config_geral.atende,
        "oportunidades": config_prod.oportunidades,
        "oportunidades_fixada": config_prod.oportunidades_fixada,
        "zero_esquerda": config_prod.zero_esquerda,
        "cashback_qtd": cashback_qtd,
        "cashback_pct": cashback_pct
    }


def produtos_dto_v2(linha_result, payload_prod, cursor_db, looping=0.0, sem_resposta=False, combo=False):
    id_rep, cnpj_cliente, _, id_cotacao, *_ = linha_result
    relacionados = payload_prod.config_geral.relacionados
    produtos = _buscar_produtos(cursor_db, id_cotacao, id_rep, cnpj_cliente, relacionados)
    if sem_resposta:
        return [itens_dto(p) for p in produtos]
    config = _carregar_config(payload_prod)
    random.shuffle(produtos)
    if config['resposta_parcial']:
        produtos = utils.randomizar_lista_porcentagem(10, 75, produtos)
    saida_list = []
    for produto in produtos:
        if looping:
            produto = list(produto)
            produto[2] = looping  # mocka o preço
        dto = _processar_produto(produto, config, looping, combo)
        if dto:
            saida_list.append(dto)
        if config['cashback_qtd']:
            config['cashback_qtd'] -= 1
    return saida_list
