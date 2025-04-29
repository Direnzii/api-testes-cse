def pedido_item_sugerido_by_cotacao(cote):
    return f"""delete from PEDIDOITEMSUGERIDO pis where pis.idpedido in (select p.id from pedido p WHERE 
    p.idcotacao = {cote})"""


def pedido_item(cote):
    return f"""delete from PEDIDOITEM pi where pi.idpedido in (select p.id from pedido p WHERE p.idcotacao = {cote})"""


def pedido_item_by_cotacao(cote):
    return f""" delete from PEDIDOITEM pi where pi.idpedido in (select p.id from pedido p where p.idcotacao in (
    select c.id from COTACAO c WHERE c.ID = {cote}))
"""


def pedido_manual_item_resposta(cote):
    return f""" delete from PEDIDOMANUALITEMRESPOSTA pmir where pmir.idpedidomanualitem in (select pmi.id from 
    PEDIDOMANUALITEM pmi where pmi.idpedidomanual in (select pm.id from PEDIDOMANUAL pm where pm.idcotacao in (select 
    c.id from COTACAO c WHERE c.ID = {cote})))
"""


def pedido_manual_item(cote):
    return f""" delete from PEDIDOMANUALITEM pmi where pmi.idpedidomanual in (select pm.id from PEDIDOMANUAL pm where 
    pm.idcotacao in (select c.id from COTACAO c WHERE c.ID = {cote}))
"""


def pedido_manual(cote):
    return f"""
delete from PEDIDOMANUAL pm where pm.idcotacao in (select c.id from COTACAO c WHERE c.ID = {cote})
"""


def aliquotast(cote):
    return f""" delete from ALIQUOTAST a where a.idcotacaoitem in (select ci.id from cotacaoitem ci where 
    ci.idcotacao in (select c.id from COTACAO c WHERE c.ID = {cote}))
"""


def negociacao_detalhe_cliente(cote):
    return f""" delete from NEGOCIACAODETALHECLIENTE ndc where ndc.idrespostaclienteitem in (select rci.id from 
    RESPOSTACLIENTEITEM rci where rci.idcotacaoitem in (select ci.id from cotacaoitem ci where ci.idcotacao in (
    select c.id from COTACAO c WHERE c.ID = {cote})))
"""


def resposta_cliente_item(cote):
    return f""" delete from RESPOSTACLIENTEITEM rci where rci.idcotacaoitem in (select ci.id from cotacaoitem ci 
    where ci.idcotacao in (select c.id from COTACAO c WHERE c.ID = {cote}))
"""


def resposta_cliente(cote):
    return f"""
delete from RESPOSTACLIENTE rc where rc.idcotacao in (select c.id from COTACAO c WHERE c.ID = {cote})
"""


def cotacao_item(cote):
    return f"""
delete from cotacaoitem ci where ci.idcotacao in (select c.id from COTACAO c WHERE c.ID = {cote})
"""


def rel_cotacao_representante(cote):
    return f"""
delete from rel_cotacao_representante where idcotacao in (select c.id from COTACAO c WHERE c.ID = {cote})
"""


def itens_clonagem(cote):
    return f""" delete from ITENSCLONAGEM ic where ic.idrespostaitem in (select ri.id from RESPOSTAITEM ri where 
    ri.idresposta in (select r.id from RESPOSTA r where r.idcotacao in (select c.id from cotacao c WHERE c.ID = 
{cote})))
"""


def resposta_item(cote):
    return f""" delete from RESPOSTAITEM ri where ri.idresposta in (select r.id from RESPOSTA r where r.idcotacao in 
    (select c.id from cotacao c WHERE c.ID = {cote}))
"""


def clonagem(cote):
    return f""" delete from CLONAGEM cl where cl.idresposta in (select r.id from RESPOSTA r where r.idcotacao in (
    select c.id from cotacao c WHERE c.ID = {cote}))
"""


def filial_resposta(cote):
    return f""" delete from FILIALRESPOSTA fr where fr.idresposta in (select r.id from RESPOSTA r where r.idcotacao 
    in (select c.id from cotacao c WHERE c.ID = {cote}))
"""


def negociacao_detalhe_desconto(cote):
    return f""" delete from NEGOCIACAODETALHEDESCONTO ndd where ndd.idresposta in (select r.id from RESPOSTA r where 
    r.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote}))
"""


def resposta(cote):
    return f"""
delete from RESPOSTA r where r.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote})
"""


def produto_historico_by_cotacao(cote):
    return f""" delete from produtohistorico ph where ph.idpedido in (select p.id from pedido p where p.idcotacao in 
    (select c.id from cotacao c WHERE c.ID = {cote}))
"""


def produto_historico_by_cotacao(cote):
    return f"""delete from produtohistorico ph where ph.idpedido in 
    (select p.id from pedido p WHERE p.idcotacao = {cote})"""


def rel_cot_ped_campanha_by_cotacao_and_ped(cote):
    return f"""delete from REL_COT_PED_CAMPANHA rcpc where rcpc.idpedido in (select p.id from pedido p WHERE 
    p.idcotacao = {cote})"""


def erro_integracao_causa(cote):
    return f""" delete from ERROINTEGRACAOCAUSA eiu where eiu.iderrointegracao in (select ei.id from ERROINTEGRACAO 
    ei where ei.idpedido in (select p.id from pedido p where p.idcotacao in (select c.id from cotacao c WHERE c.ID = 
{cote})))
"""


def erro_integracao(cote):
    return f""" delete from ERROINTEGRACAO ei where ei.idpedido in (select p.id from pedido p where p.idcotacao in (
    select c.id from cotacao c WHERE c.ID = {cote}))
"""


def historico_integracao_pedido(cote):
    return f""" delete from HISTORICOINTEGRACAOPEDIDO hip where hip.idpedido in (select p.id from pedido p where 
    p.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote}))
"""


def imposto_nota_fiscal(cote):
    return f""" delete from IMPOSTONOTAFISCAL if where if.idnotafiscaleletronicaitem in (select nfei.id from 
    NOTAFISCALELETRONICAITEM nfei where nfei.idnotafiscaleletronica in (select nfe.id from NOTAFISCALELETRONICA nfe 
    where nfe.idpedido in (select p.id from pedido p where p.idcotacao in (select c.id from cotacao c WHERE c.ID = 
{cote}))))
"""


def nota_fiscal_eletronica_item(cote):
    return f""" delete from NOTAFISCALELETRONICAITEM nfei where nfei.idnotafiscaleletronica in (select nfe.id from 
    NOTAFISCALELETRONICA nfe where nfe.idpedido in (select p.id from pedido p where p.idcotacao in (select c.id from 
    cotacao c WHERE c.ID = {cote})))
"""


def duplicata_nota_fiscal(cote):
    return f""" delete from DUPLICATANOTAFISCAL df where df.idnotafiscaleletronica in (select nfe.id from 
    NOTAFISCALELETRONICA nfe where nfe.idpedido in (select p.id from pedido p where p.idcotacao in (select c.id from 
    cotacao c WHERE c.ID = {cote})))
"""


def nota_fiscal_eletronica(cote):
    return f""" delete from NOTAFISCALELETRONICA nfe where nfe.idpedido in (select p.id from pedido p where 
    p.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote}))"""


def pedido_by_cotacao(cote):
    return f"""
delete from pedido p where p.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote})
"""


def pedido():
    return """
delete from pedido p WHERE p.datacadastro < ADD_MONTHS(SYSDATE, -24)
"""


def filial_cotacao(cote):
    return f"""
delete from FILIALCOTACAO fc where fc.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote})"""


def cotacao_associada(cote):
    return f"""
delete from COTACAOASSOCIADA ca where ca.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote})
"""


def historico_integracao_cotacao(cote):
    return f"""
delete from HISTORICOINTEGRACAOCOTACAO hic where hic.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote})
"""


def pedido_cliente(cote):
    return f""" delete from PEDIDOCLIENTE pc where pc.idpedidorepresentante in (select pr.id from PEDIDOREPRESENTANTE 
    pr where pr.idpedidofornecedor in (select pf.id from PEDIDOFORNECEDOR pf where pf.idpedidoagrupado in (select 
    pa.id from PEDIDOAGRUPADO pa where pa.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote}))))
"""


def pedido_representante(cote):
    return f""" delete from PEDIDOREPRESENTANTE pr where pr.idpedidofornecedor in (select pf.id from PEDIDOFORNECEDOR 
    pf where pf.idpedidoagrupado in (select pa.id from PEDIDOAGRUPADO pa where pa.idcotacao in (select c.id from 
    cotacao c WHERE c.ID = {cote})))
"""


def pedido_fornecedor(cote):
    return f""" delete from PEDIDOFORNECEDOR pf where pf.idpedidoagrupado in (select pa.id from PEDIDOAGRUPADO pa 
    where pa.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote}))
"""


def pedido_agrupado(cote):
    return f"""
delete from PEDIDOAGRUPADO pa where pa.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote})"""


def historico_integracao_resposta(cote):
    return f"""
delete from HISTORICOINTEGRACAORESPOSTA hir where hir.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote})"""


def rep_nao_participante_cot(cote):
    return f"""
delete from REP_NAO_PARTICIPANTE_COT rnp where rnp.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote})"""


def produto_bloqueadosrf(cote):
    return f""" delete from PRODUTOSBLOQUEADOSRF pb where pb.idbloqueioretornofaturamento in (select brf.id from 
    BLOQUEIORETORNOFATURAMENTO brf where brf.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote}))
"""


def bloqueio_retorno_faturamento(cote):
    return f"""
delete from BLOQUEIORETORNOFATURAMENTO brf where brf.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote})
"""


def bloqueio_valor_referencia_item(cote):
    return f""" delete from BLOQUEIOVALORREFERENCIAITEM bri where bri.idbloqueiovalorreferencia in (select br.id from 
    BLOQUEIOVALORREFERENCIA br where br.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote}))
"""


def bloqueio_valor_referencia(cote):
    return f"""
delete from BLOQUEIOVALORREFERENCIA br where br.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote})
"""


def codigo_ref_cotacao(cote):
    return f"""
delete from CODIGOREFCOTACAO cc where cc.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote})
"""


def cotacao_cliente_sic(cote):
    return f"""
delete from COTACAOCLIENTESIC cs where cs.idcotacao in (select c.id from cotacao c WHERE c.ID = {cote})
"""


def rel_cot_campanha_by_cot(cote):
    return f"""delete FROM REL_COT_PED_CAMPANHA WHERE idcotacao = {cote}"""


def cotacao(cote):
    return f"""
delete from cotacao c WHERE c.ID = {cote}
"""
