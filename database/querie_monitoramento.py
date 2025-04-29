def get_respostas_by_cotacao_datavencimento_situacao_nome_cnpj_rep(cotacao=None, datavencimento=None, situacao=None, forn_nome=None, forn_cnpj=None, repre_nome=None):
    base_query = """
        SELECT rc.id, cli.nomefantasia, cli.cnpj, c.id, c.datavencimento, c.situacao, f.nomefantasia, f.cnpj, ct.nome, rc.motivoresposta
        FROM cotacao c
        INNER JOIN respostacliente rc ON rc.idcotacao = c.id
        INNER JOIN cliente cli ON cli.id = rc.idcliente
        INNER JOIN fornecedor f ON f.id = rc.idfornecedor
        INNER JOIN conta co ON co.id = rc.idrepresentante
        INNER JOIN contato ct ON ct.id = co.idcontato
        WHERE 1=1
    """

    filtros = []

    if cotacao:
        filtros.append(f"(c.id = {cotacao} OR c.id = (SELECT idmatriz FROM cotacao WHERE id = {cotacao}))")

    if datavencimento:
        filtros.append(f"EXTRACT(DAY FROM datavencimento) = {datavencimento.split('/')[0]} AND "
                       f"EXTRACT(MONTH FROM datavencimento) = {datavencimento.split('/')[1]} AND "
                       f"EXTRACT(YEAR FROM datavencimento) = {datavencimento.split('/')[2]}")

    if situacao:
        filtros.append(f"c.situacao = {situacao}")

    if forn_nome:
        filtros.append(f"f.nomefantasia LIKE '%{forn_nome.upper()}%'")

    if forn_cnpj:
        filtros.append(f"f.cnpj LIKE '%{forn_cnpj}%'")

    if repre_nome:
        filtros.append(f"ct.nome LIKE '%{repre_nome.upper()}%'")

    if filtros:
        base_query += " AND " + " AND ".join(filtros)
    return base_query

def get_dados_representante_by_id_resposta(id_resposta, id_cotacao):
    return f"""SELECT comp.nome,  u.usuario, d.CODCLIENTE, d.USUARIO, d.SENHA, d.CODAUXILIAR, d.MULTIPLASRESPOSTAS 
                FROM respostacliente rc
                INNER JOIN cotacao c ON c.id = rc.IDCOTACAO
                INNER JOIN conta co ON co.id = c.IDCOMPRADOR
                INNER JOIN contato comp ON comp.id = co.IDCONTATO
                INNER JOIN usuario u ON u.id = co.IDUSUARIO
                INNER JOIN DADOSREPRESENTANTECLIENTE d ON d.IDCOMPRADOR = c.IDCOMPRADOR AND d.IDFORNECEDOR = rc.IDFORNECEDOR AND d.IDREPRESENTANTE = rc.IDREPRESENTANTE
                WHERE rc.id = {id_resposta}
                AND c.id = {id_cotacao}"""
# ////PAREI AQUI, basicamente, esta retornando mais de uma resposta por vez o que esta errado, preciso achar um jeito de sempre trazer um unico retorno
# passando cliente talvez, cnpj ou id, algo assim
