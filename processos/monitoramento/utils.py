from database.conectar import conectar_db
from database.querie_monitoramento import get_respostas_by_cotacao_datavencimento_situacao_nome_cnpj_rep, \
    get_dados_representante_by_id_resposta


def consulta_respostas(filtros):
    cotacao = filtros['cotacao']
    data_vencimento = filtros['data_vencimento']
    situacao = filtros['situacao']
    nome_fornecedor = filtros['nome_fornecedor']
    cnpj_forn = filtros['cnpj_forn']
    nome_representante = filtros['nome_representante']
    with conectar_db(True) as cursor:
        try:
            cursor.execute(get_respostas_by_cotacao_datavencimento_situacao_nome_cnpj_rep(cotacao=cotacao,
                                                                                datavencimento=data_vencimento,
                                                                                          situacao=situacao,
                                                                                          forn_cnpj=cnpj_forn,
                                                                                    forn_nome=nome_fornecedor,
                                                                                    repre_nome=nome_representante))
            respostas = cursor.fetchall()
            list_saida = []
            for resposta in respostas:
                objeto_resposta = {
                    "id": resposta[0],
                    "nome_cliente": resposta[1],
                    "cnpj_cliente": resposta[2],
                    "id_cotacao": resposta[3],
                    "data_vencimento": resposta[4].strftime("%d/%m/%y %H:%M:%S") + ",000000000",
                    "situacao": resposta[5],
                    "nome_fornecedor": resposta[6],
                    "cnpj_fornecedor": resposta[7],
                    "nome_representante": resposta[8],
                    "motivo": resposta[9]
                }
                list_saida.append(objeto_resposta)
            return list_saida
        except Exception as E:
            raise

def consultar_dados_representante(id_resposta, id_cotacao):
    with conectar_db(True) as cursor:
        try:
            cursor.execute(get_dados_representante_by_id_resposta(id_resposta=id_resposta, id_cotacao=id_cotacao))
            respostas = cursor.fetchall()
            list_saida = []
            for resposta in respostas:
                objeto_resposta = {
                    "nome_comprador": resposta[0],
                    "usuario_cote": resposta[1],
                    "codigo_cliente": resposta[2],
                    "usuario": resposta[3],
                    "senha": resposta[4],
                    "codigo_auxiliar": resposta[5],
                    "multipla_resposta": resposta[6],
                }
                list_saida.append(objeto_resposta)
            return list_saida
        except Exception as E:
            raise