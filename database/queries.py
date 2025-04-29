def query_get_prod_conjunta(cote, idrep, cli_cnpj, relacionados):
    if relacionados:
        return f"""SELECT DISTINCT PROD.CODBARRAS,CI.QUANTIDADE, REPLACE(CI.HISTORICO, ',', '.') AS HISTORICO,
                                                M.ID AS MEDICAMENTO
                                                FROM COTACAOITEM CI
                                                INNER JOIN REL_COTACAO_REPRESENTANTE RCR ON RCR.IDCOTACAO = CI.IDCOTACAO
                                                INNER JOIN CLIENTE CLI ON CLI.ID = CI.IDFILIALCLIENTE
                                                INNER JOIN PRODUTO PROD ON PROD.ID = CI.IDPRODUTO
                                                LEFT JOIN MEDICAMENTO M ON M.ID = PROD.ID
                                                WHERE 
                                                CI.IDCOTACAO = {cote}
                                                AND RCR.IDREPRESENTANTE = {idrep}
                                                AND CLI.CNPJ = '{cli_cnpj}'
                                                UNION 
                                                SELECT DISTINCT CREL.CODIGO, CIREL.QUANTIDADE, REPLACE(CIREL.HISTORICO, 
                                                ',', '.') AS HISTORICO, MREL.ID AS MEDICAMENTO  
                                                FROM CODIGOPRODUTO CREL
                                                INNER JOIN PRODUTOCODIGOPRODUTO PREL ON PREL.IDCODIGOPRODUTO = CREL.ID
                                                INNER JOIN COTACAOITEM CIREL ON PREL.IDPRODUTO = CIREL.IDPRODUTO 
                                                INNER JOIN REL_COTACAO_REPRESENTANTE RCRREL ON RCRREL.IDCOTACAO
                                                 = CIREL.IDCOTACAO
                                                INNER JOIN CLIENTE CLIREL ON CLIREL.ID = CIREL.IDFILIALCLIENTE
                                                LEFT JOIN MEDICAMENTO MREL ON MREL.ID = CIREL.IDPRODUTO
                                                WHERE
                                                CIREL.IDCOTACAO = {cote}
                                                AND RCRREL.IDREPRESENTANTE = {idrep}
                                                AND CLIREL.CNPJ = '{cli_cnpj}'"""
    return f"""SELECT DISTINCT PROD.CODBARRAS, CI.QUANTIDADE, REPLACE(CI.HISTORICO, ',', '.'),
                                                M.ID AS MEDICAMENTO
                                                FROM COTACAOITEM CI
                                                INNER JOIN REL_COTACAO_REPRESENTANTE RCR ON RCR.IDCOTACAO = CI.IDCOTACAO
                                                INNER JOIN CLIENTE CLI ON CLI.ID = CI.IDFILIALCLIENTE
                                                INNER JOIN PRODUTO PROD ON PROD.ID = CI.IDPRODUTO
                                                LEFT JOIN MEDICAMENTO M ON M.ID = PROD.ID
                                                WHERE 
                                                CI.IDCOTACAO = {cote}
                                                AND RCR.IDREPRESENTANTE = {idrep}
                                                AND CLI.CNPJ = '{cli_cnpj}'
                                                ORDER BY RCR.IDREPRESENTANTE DESC"""


def query_get_prod_simples(cote, idrep, relacionados):
    if relacionados:
        return f"""SELECT DISTINCT PROD.CODBARRAS,CI.QUANTIDADE, REPLACE(CI.HISTORICO, ',', '.') AS HISTORICO,
                                                            M.ID AS MEDICAMENTO
                                                            FROM COTACAOITEM CI
                                                            INNER JOIN REL_COTACAO_REPRESENTANTE RCR 
                                                            ON RCR.IDCOTACAO = CI.IDCOTACAO
                                                            INNER JOIN PRODUTO PROD ON PROD.ID = CI.IDPRODUTO
                                                            LEFT JOIN MEDICAMENTO M ON M.ID = PROD.ID
                                                            WHERE 
                                                            CI.IDCOTACAO = {cote}
                                                            AND RCR.IDREPRESENTANTE = {idrep}                                               
                                                            UNION 
                                                            SELECT DISTINCT CREL.CODIGO, CIREL.QUANTIDADE, 
                                                            REPLACE(CIREL.HISTORICO, ',', '.') AS HISTORICO,
                                                            MREL.ID AS MEDICAMENTO  
                                                            FROM CODIGOPRODUTO CREL                               
                                                            INNER JOIN PRODUTOCODIGOPRODUTO PREL ON 
                                                            PREL.IDCODIGOPRODUTO = CREL.ID
                                                            INNER JOIN COTACAOITEM CIREL ON 
                                                            PREL.IDPRODUTO = CIREL.IDPRODUTO 
                                                            INNER JOIN REL_COTACAO_REPRESENTANTE RCRREL ON 
                                                            RCRREL.IDCOTACAO = CIREL.IDCOTACAO
                                                            LEFT JOIN MEDICAMENTO MREL ON MREL.ID = CIREL.IDPRODUTO   
                                                            WHERE
                                                            CIREL.IDCOTACAO = {cote}
                                                            AND RCRREL.IDREPRESENTANTE = {idrep}"""

    return f"""SELECT DISTINCT PROD.CODBARRAS, CI.QUANTIDADE, REPLACE(CI.HISTORICO, ',', '.') AS HISTORICO,
                                                              M.ID AS MEDICAMENTO
                                                              FROM COTACAOITEM CI
                                                              INNER JOIN REL_COTACAO_REPRESENTANTE RCR ON 
                                                              RCR.IDCOTACAO = CI.IDCOTACAO
                                                              INNER JOIN PRODUTO PROD ON PROD.ID = CI.IDPRODUTO
                                                              LEFT JOIN MEDICAMENTO M ON M.ID = PROD.ID
                                                              WHERE CI.IDCOTACAO = {cote}
                                                              AND RCR.IDREPRESENTANTE = {idrep}
                                                              ORDER BY RCR.IDREPRESENTANTE DESC"""


def get_cotacao_fake_query(cote, filial):
    return f"""
            SELECT C.ID FROM COTACAO C
            INNER JOIN CLIENTE CLI ON CLI.ID = C.IDCLIENTE
            WHERE C.IDMATRIZ = '{cote}'
            AND CLI.CNPJ = '{filial}'"""


def get_todos_os_dados_para_resposta_com_base_na_cotacao(cote):
    return f"""SELECT DISTINCT RCR.IDREPRESENTANTE, CLI.CNPJ, F.CNPJ, RCR.IDCOTACAO, COALESCE(CCP.MEDIA, 10) AS DIAS,
                                F.NOMEFANTASIA
                                FROM REL_COTACAO_REPRESENTANTE RCR
                                INNER JOIN REPRESENTANTE REP ON REP.ID = RCR.IDREPRESENTANTE
                                INNER JOIN CLIENTE CLI ON CLI.ID = RCR.IDCLIENTE
                                INNER JOIN FORNECEDOR F ON F.ID = RCR.IDFORNECEDOR
                                LEFT JOIN COTACAOCONDICAOPAGAMENTO CCP ON CCP.ID = RCR.IDCONDICAOPAGAMENTOCOT
                                WHERE RCR.IDCOTACAO =  {cote} 
                                AND REP.TIPORESPOSTA != 0
                                ORDER BY F.NOMEFANTASIA ASC"""  # RESPONDER PARA TODOS MENOS REP. MANUAL


def get_vencimento(cote):
    return F'SELECT DATAVENCIMENTO FROM COTACAO WHERE ID = {cote}'


def retorno_faturamento_info(ped):
    return f"""SELECT CLI.CNPJ AS CNPJ_CLI, F.CNPJ AS CNPJ_FORN, P.IDCOTACAO, P.ID AS ID_PEDIDO FROM PEDIDO P
                                INNER JOIN FORNECEDOR F ON F.ID = P.IDFORNECEDOR
                                INNER JOIN CLIENTE CLI ON CLI.ID = P.IDCLIENTE
                                WHERE P.ID = {ped}"""


def retorno_faturamento_itens(ped, menos_qtd=False, not_fat=False):
    if menos_qtd:
        qtd = "ROUND(PI.QUANTIDADEPEDIDA / 2)"
    elif not_fat:
        qtd = "0"
    else:
        qtd = "PI.QUANTIDADEPEDIDA"
    return f"""
        SELECT '<itens><codBarras>' || PR.CODBARRAS || '</codBarras><codigoMotivo>0</codigoMotivo>
                <descontoFaturado>0</descontoFaturado>
                <motivoFalta></motivoFalta><quantidadeEmbalagem></quantidadeEmbalagem>
                <quantidadeFaturada>' || {qtd} || '</quantidadeFaturada><tipoEmbalagem></tipoEmbalagem>
                <valorFaturado>' || REPLACE(PI.VALORUNITARIO, ',', '.') || '</valorFaturado>
                <valorProdutoBoleto></valorProdutoBoleto><valorSemSt>0</valorSemSt></itens>'
        FROM PEDIDO P
        INNER JOIN PEDIDOITEM PI ON PI.IDPEDIDO = P.ID
        INNER JOIN PRODUTO PR ON PR.ID = PI.IDPRODUTO
        WHERE P.ID = {ped}
    """



def get_usuario_senha(cote):
    return f"""SELECT DISTINCT U.USUARIO, U.SENHA FROM COTACAO C
                                INNER JOIN CLIENTE CLI ON CLI.ID = C.IDCLIENTE
                                INNER JOIN CONTA CONT ON CONT.ID = C.IDCOMPRADOR
                                INNER JOIN USUARIO U ON U.ID = CONT.IDUSUARIO
                                WHERE C.ID = {cote}"""


def get_pedido_from_cotacao(cote, so_com_retorno=False, pedido_looping=False):
    if so_com_retorno:
        return f"""SELECT ID FROM PEDIDO WHERE IDCOTACAO = {cote} AND SITUACAO IN (6, 11, 12)"""
    if pedido_looping:
        return f"""SELECT ID FROM PEDIDO WHERE IDCOTACAO = {cote} AND LOOPING > 0"""
    return f"""SELECT ID FROM PEDIDO WHERE IDCOTACAO = {cote}"""


def delete_pedidoitem_from_pedido(pedido):
    return f"""DELETE PEDIDOITEM WHERE IDPEDIDO = {pedido}"""


def delete_produtohistorico_from_pedido(pedido):
    return f"""DELETE PRODUTOHISTORICO WHERE IDPEDIDO = {pedido}"""


def delete_pedido_from_cotacao(pedido):
    return f"""DELETE PEDIDO WHERE ID = {pedido}"""


def update_resposta_cliente(cote, dias, para_mais):
    if para_mais:
        return f"""UPDATE RESPOSTACLIENTE SET VALIDADE = SYSDATE+{dias} WHERE IDCOTACAO = {cote}"""
    return f"""UPDATE RESPOSTACLIENTE SET VALIDADE = SYSDATE-{dias} WHERE IDCOTACAO = {cote}"""


def update_pedidomanualitemresposta(cote, dias, para_mais):
    if para_mais:
        return f"""UPDATE PEDIDOMANUALITEMRESPOSTA SET VALIDADE = SYSDATE+{dias} WHERE ID IN (SELECT PMIR.ID
                                    FROM PEDIDOMANUAL PM
                                    INNER JOIN PEDIDOMANUALITEM PMI ON PMI.IDPEDIDOMANUAL = PM.ID
                                    LEFT JOIN PEDIDOMANUALITEMRESPOSTA PMIR ON PMIR.IDPEDIDOMANUALITEM = PMI.ID
                                    WHERE PM.IDCOTACAO = {cote})"""
    return f"""UPDATE PEDIDOMANUALITEMRESPOSTA SET VALIDADE = SYSDATE-{dias} WHERE ID IN (SELECT PMIR.ID
                                        FROM PEDIDOMANUAL PM
                                        INNER JOIN PEDIDOMANUALITEM PMI ON PMI.IDPEDIDOMANUAL = PM.ID
                                        LEFT JOIN PEDIDOMANUALITEMRESPOSTA PMIR ON PMIR.IDPEDIDOMANUALITEM = PMI.ID
                                        WHERE PM.IDCOTACAO = {cote})"""


def update_pedido(cote, dias, para_mais):
    if para_mais:
        return f"""UPDATE PEDIDO SET VALIDADE = SYSDATE+{dias} WHERE IDCOTACAO = {cote}"""
    return f"""UPDATE PEDIDO SET VALIDADE = SYSDATE-{dias} WHERE IDCOTACAO = {cote}"""


def update_cotacao(cote, dias, para_mais):
    if para_mais:
        return f"""UPDATE COTACAO SET DATAVENCIMENTO = SYSDATE+{dias} WHERE ID = {cote}"""
    return f"""UPDATE PEDIDO SET VALIDADE = SYSDATE-{dias} WHERE IDCOTACAO = {cote}"""


def update_vencido_para_respondido(cote, de, para):
    if de and para:
        return f"""UPDATE RESPOSTACLIENTE SET MOTIVORESPOSTA = '{para}' WHERE ID IN (
                                    SELECT ID FROM RESPOSTACLIENTE WHERE IDCOTACAO = {cote}
                                    AND MOTIVORESPOSTA = '{de}')"""
    return f"""UPDATE RESPOSTACLIENTE SET MOTIVORESPOSTA = 'RESPONDIDA' WHERE ID IN (
                                            SELECT ID FROM RESPOSTACLIENTE WHERE IDCOTACAO = {cote}
                                            AND MOTIVORESPOSTA = 'VENCIDA')"""


def delete_negociacaodetalhecliente(cote):
    return f"""DELETE NEGOCIACAODETALHECLIENTE N WHERE N.ID IN
                                (SELECT NDC.ID
                                FROM NEGOCIACAODETALHECLIENTE NDC
                                INNER JOIN RESPOSTACLIENTEITEM RCI ON RCI.ID = NDC.IDRESPOSTACLIENTEITEM
                                INNER JOIN RESPOSTACLIENTE RC ON RCI.IDRESPOSTACLIENTE = RC.ID
                                WHERE RC.IDCOTACAO =  {cote})"""


def delete_respostaclienteitem(cote):
    return f"""DELETE RESPOSTACLIENTEITEM RCI
                                WHERE RCI.ID IN
                                (SELECT RCI.ID
                                FROM RESPOSTACLIENTEITEM RCI
                                INNER JOIN RESPOSTACLIENTE RC ON RCI.IDRESPOSTACLIENTE = RC.ID
                                WHERE RC.IDCOTACAO =  {cote})"""


def delete_produtosemresposta(cote):
    return f"""DELETE PRODUTOSEMRESPOSTA WHERE IDCOTACAO = {cote}"""


def delete_respostacliente(cote):
    return f"""DELETE RESPOSTACLIENTE RC WHERE RC.IDCOTACAO = {cote}"""


def voltar_para_em_analise_1(cote):
    return f"""DELETE PEDIDOITEM WHERE IDPEDIDO IN (SELECT ID FROM PEDIDO WHERE IDCOTACAO = {cote})"""


def voltar_para_em_analise_2(cote):
    return f"""DELETE PRODUTOHISTORICO WHERE IDPEDIDO IN (SELECT ID FROM PEDIDO WHERE IDCOTACAO = {cote})"""


def voltar_para_em_analise_3(cote):
    return f"""DELETE HISTORICOINTEGRACAOPEDIDO WHERE IDPEDIDO IN (SELECT ID FROM PEDIDO WHERE IDCOTACAO = {cote})"""


def deletar_pedidos_by_cotacao(cote):
    return f"""DELETE PEDIDO WHERE ID IN (SELECT ID FROM PEDIDO WHERE IDCOTACAO = {cote})"""


def update_situacao_cotacao(cote, situacao):
    return f"""UPDATE COTACAO SET SITUACAO = {situacao} WHERE ID = {cote}"""


def deletar_dados_looping(cote):
    return f"""DELETE FROM DADOSLOOPING WHERE IDCOTACAO = {cote}"""


def querie_consultar_pmir_geracao(cote):
    return f"""SELECT PRO.CODBARRAS, P.QUANTIDADEPEDIDA, P.QUANTIDADE, F.NOMEFANTASIA, CT.NOME, CLI.CNPJ, 
                        CLI.NOMEFANTASIA, PE.VALORTOTAL, '|', 
                        PRO.CODBARRAS, P.QUANTIDADEPEDIDA, P.QUANTIDADE, PE.VALORTOTAL, F.NOMEFANTASIA, 
                        CT.NOME, CLI.CNPJ, CLI.NOMEFANTASIA
                        FROM PEDIDOITEM P
                        INNER JOIN PEDIDO PE ON PE.ID = P.IDPEDIDO
                        INNER JOIN PRODUTO PRO ON PRO.ID = P.IDPRODUTO
                        INNER JOIN FORNECEDOR F ON F.ID = PE.IDFORNECEDOR
                        INNER JOIN CONTA C ON C.ID = PE.IDREPRESENTANTE
                        INNER JOIN CONTATO CT ON CT.ID = C.IDCONTATO
                        INNER JOIN CLIENTE CLI ON CLI.ID = PE.IDCLIENTE
                        WHERE PE.IDCOTACAO = {cote}
                        ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16"""


def querie_deletar_pmir(cote):
    return f"""DELETE PEDIDOMANUALITEMRESPOSTA WHERE ID IN (
                        SELECT PMIR.ID FROM PEDIDOMANUAL PM 
                        INNER JOIN PEDIDOMANUALITEM PMI ON PMI.IDPEDIDOMANUAL = PM.ID
                        INNER JOIN PEDIDOMANUALITEMRESPOSTA PMIR ON PMIR.IDPEDIDOMANUALITEM = PMI.ID
                        WHERE PM.IDCOTACAO = {cote})"""


def aumentar_validade_resposta_cliente(cote):
    return f"""UPDATE RESPOSTACLIENTE SET VALIDADE = SYSDATE+60 WHERE IDCOTACAO = {cote}"""


def aumentar_validade_pmir(cote):
    return f"""UPDATE PEDIDOMANUALITEMRESPOSTA SET VALIDADE = SYSDATE+60 WHERE ID IN (SELECT PMIR.ID
                        FROM PEDIDOMANUAL PM
                        INNER JOIN PEDIDOMANUALITEM PMI ON PMI.IDPEDIDOMANUAL = PM.ID
                        LEFT JOIN PEDIDOMANUALITEMRESPOSTA PMIR ON PMIR.IDPEDIDOMANUALITEM = PMI.ID
                        WHERE PM.IDCOTACAO = {cote})"""


def aumentar_validade_pedido(cote):
    return f"""UPDATE PEDIDO SET VALIDADE = SYSDATE+60 WHERE IDCOTACAO = {cote}"""


def alterar_data_cadastro_cotacao_para_hoje(cote):
    return f"""UPDATE COTACAO SET DATACADASTRO = SYSDATE-1 WHERE ID = {cote}"""


def alterar_validade_cotacao_para_1h_no_futuro(cote):
    return f"""UPDATE COTACAO SET DATAVENCIMENTO = SYSDATE + INTERVAL '1' HOUR WHERE ID = {cote}"""


def mudar_motivo_de_vencido_para_respondido(cote):
    return f"""UPDATE RESPOSTACLIENTE SET MOTIVORESPOSTA = 'RESPONDIDA' WHERE ID IN (
                        SELECT ID FROM RESPOSTACLIENTE WHERE IDCOTACAO = {cote} AND MOTIVORESPOSTA = 'VENCIDA'
                        )"""


def atualizar_data_montagem_pedido_manual(cote):
    return f"""UPDATE PEDIDOMANUAL SET DATAMONTAGEM = SYSDATE - 1 WHERE IDCOTACAO = {cote}"""


def get_situacao_pedido_by_cotacao_querie(cote):
    return f"""SELECT P.ID, P.SITUACAO FROM PEDIDO P WHERE P.IDCOTACAO = {cote}"""


def update_cotacao_inserir_no_pedido_iniciar_montagem_to_null(cote):
    return f"""UPDATE COTACAO SET INSERIDONOPEDIDO = NULL, INICIOMONTAGEMPEDIDO = NULL,
     PEDIDOMANUAL = NULL WHERE ID = {cote} OR COTACAO.IDMATRIZ = {cote}"""


def health_check_db():
    return """SELECT 1 FROM DUAL"""


def retornar_id_representante_ftp_pelo_fornecedor(cnpj_fornecedor):
    return f"""SELECT r.id
    FROM REPRESENTANTEFORNECEDOR RF
    INNER JOIN FORNECEDOR F ON F.ID = RF.IDFORNECEDOR
    INNER JOIN CONTA C ON C.ID = RF.IDREPRESENTANTE
    INNER JOIN CONTATO CT ON CT.ID = C.IDCONTATO
    INNER JOIN REPRESENTANTE R ON R.ID = C.ID
    WHERE F.CNPJ = {cnpj_fornecedor}
    AND R.TIPORESPOSTA = 1
    AND CT.NOME LIKE '%On-line%'"""

def retornar_id_representante(cnpj_fornecedor, nome_representante, cnpjs_str):
    return f"""SELECT r.id
    FROM fornecedorcomprador fc
    INNER JOIN comprador c on c.id = fc.idcomprador
    INNER JOIN conta cc ON cc.id = c.id
    INNER JOIN usuario u ON u.id = cc.idusuario
    LEFT JOIN filialfornecedor ff on ff.idfornecedorcomprador = fc.id
    LEFT JOIN representantefilial rf on rf.idfilial = ff.id
    INNER JOIN rep_forne_comp rfc on rfc.idfornecedorcomprador = fc.id
    INNER JOIN cliente cli on cli.id = nvl(ff.idfilial, c.idcliente)
    INNER JOIN fornecedor f on f.id = fc.idfornecedor
    INNER JOIN representante r on r.id = nvl(rf.idrepresentante, rfc.idrepresentante)
    INNER JOIN conta co on co.id = r.id
    INNER JOIN contato ct on ct.id = co.idcontato
    LEFT JOIN DADOSREPRESENTANTECLIENTE d ON d.IDCLIENTE = nvl(ff.idfilial, c.idcliente) AND d.IDCOMPRADOR = c.id AND d.IDFORNECEDOR= f.id AND d.IDREPRESENTANTE = r.id 
    WHERE f.cnpj = '{cnpj_fornecedor}'
    AND ct.nome like '%{nome_representante}%'
    AND cli.cnpj = '{cnpjs_str}'
    AND r.TIPORESPOSTA IN (7, 1)
    GROUP BY r.id"""

def desativar_condicao_pagamento(cnpj_fornecedor, id_representante, cnpjs_str):
    return f"""UPDATE cotacaocondicaopagamento SET ativo = 0 where id in(
	SELECT cp.id
	from cotacaocondicaopagamento cp 
	left join cotacaocondpagamentocomprador ccpc on ccpc.idcondicaopagamentocotacao = cp.id
	inner join cliente cli ON cli.id = cp.idcliente 
	inner join fornecedor f ON f.id = cp.idfornecedor 
	inner join conta c on c.id = cp.idrepresentante 
	inner join contato con on con.id = c.idcontato 
	where cli.cnpj = {cnpjs_str} 
	and f.CNPJ = '{cnpj_fornecedor}' 
	and cp.IDREPRESENTANTE = {id_representante}
	and cp.ativo = 1
)"""

def delete_produto_sem_resposta_por_dia_de_1000_em_1000(data):
    return f"""DELETE FROM PRODUTOSEMRESPOSTA
    WHERE DATACADASTRO <= TO_TIMESTAMP('{data} 00:00:00', 'DD/MM/YY HH24:MI:SS')
    AND ROWNUM <= 1000"""


def delete_produto_sem_resposta_por_dia_de_1000_em_1000_mais_de_45_dias():
    return """DELETE FROM PRODUTOSEMRESPOSTA
    WHERE DATACADASTRO < SYSDATE - INTERVAL '45' DAY
    AND ROWNUM <= 1000"""

