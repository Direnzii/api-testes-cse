from database.conectar import conectar_db
from database import queries, queries_deletar_tudo
from server.instance import log
import json

logger = log.logger
default_graylog_fields = log.default_graylog_fields


def consultar_pmir_db(idcotacao, oficial=False):
    with conectar_db(oficial) as cursor:
        cursor.execute(queries.querie_consultar_pmir_geracao(idcotacao))
        result_cru = cursor.fetchall()
        result_tratado = []
        for linha in result_cru:
            json_saida = {
                "ean": linha[9],
                "qtd_pedida": linha[10],
                "qtd": linha[11],
                "valor_total": linha[12],
                "forn_nome": linha[13],
                "repre_nome": linha[14],
                "cliente_cnpj": linha[15],
                "cliente_nome": linha[16]
            }
            result_tratado.append(json_saida)
        return result_tratado


def deletar_pmir_db(idcotacao, oficial):
    with conectar_db(oficial) as cursor:
        cursor.execute(queries.querie_deletar_pmir(idcotacao))
        cursor.connection.commit()


def atualizar_todos_testes_automaticos(cote=False, oficial=False):
    if not cote:
        cotacoes = [113907, 114020, 114032, 114361, 114386, 114479, 126579, 131518]
    else:
        cotacoes = [cote]
    with conectar_db(oficial) as cursor:
        try:
            for cote in cotacoes:
                cursor.execute(queries.aumentar_validade_resposta_cliente(cote))
                cursor.execute(queries.aumentar_validade_pmir(cote))
                cursor.execute(queries.aumentar_validade_pedido(cote))
                cursor.execute(queries.alterar_data_cadastro_cotacao_para_hoje(cote))
                cursor.execute(queries.alterar_validade_cotacao_para_1h_no_futuro(cote))
                cursor.execute(queries.mudar_motivo_de_vencido_para_respondido(cote))
            cursor.connection.commit()
        except Exception as Error:
            logger.exception(f'Erro não esperado em [/atualizar_todos_testes_automaticos]: {Error}',
                             extra={**default_graylog_fields,
                                    'SourceMethodName': 'atualizar_todos_testes_automaticos'})
            raise


def get_todas_situacoes_pedido_by_cotacao(cotacao, oficial=False):
    with conectar_db(oficial) as cursor:
        json_saida = {}
        try:
            cursor.execute(queries.get_situacao_pedido_by_cotacao_querie(cotacao))
            pedidos_situacoes = cursor.fetchall()
            for pedido_situacao in pedidos_situacoes:
                json_saida[pedido_situacao[0]] = pedido_situacao[1]
            return json_saida
        except Exception as Error:
            logger.exception(f'Erro não esperado em [/get_todas_situacoes_pedido_by_cotacao]: {Error}',
                             extra={**default_graylog_fields,
                                    'SourceMethodName': 'get_todas_situacoes_pedido_by_cotacao'})
            raise


def deletar_completamente_cotacao_by_id_cotacao(cotacao, oficial):
    with conectar_db(oficial) as cursor:
        processo = 'deletar_completamente_cotacao_by_id_cotacao'
        obj_saida_linhas_deletadas = {}
        try:
            cursor.execute(queries_deletar_tudo.pedido_item_sugerido_by_cotacao(cotacao))
            pedido_item_sugerido_by_cotacao_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'pedido_item_sugerido_by_cotacao_linhas_deletadas'] = pedido_item_sugerido_by_cotacao_linhas_deletadas
            cursor.execute(queries_deletar_tudo.pedido_item_by_cotacao(cotacao))
            pedido_item_by_cotacao_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'pedido_item_by_cotacao_linhas_deletadas'] = pedido_item_by_cotacao_linhas_deletadas
            cursor.execute(queries_deletar_tudo.pedido_manual_item_resposta(cotacao))
            pedido_manual_item_resposta_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'pedido_manual_item_resposta_linhas_deletadas'] = pedido_manual_item_resposta_linhas_deletadas
            cursor.execute(queries_deletar_tudo.pedido_manual_item(cotacao))
            pedido_manual_item_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'pedido_manual_item_linhas_deletadas'] = pedido_manual_item_linhas_deletadas
            cursor.execute(queries_deletar_tudo.pedido_manual(cotacao))
            pedido_manual_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'pedido_manual_linhas_deletadas'] = pedido_manual_linhas_deletadas
            cursor.execute(queries_deletar_tudo.aliquotast(cotacao))
            aliquotast_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'aliquotast_linhas_deletadas'] = aliquotast_linhas_deletadas
            cursor.execute(queries_deletar_tudo.negociacao_detalhe_cliente(cotacao))
            negociacao_detalhe_cliente_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'negociacao_detalhe_cliente_linhas_deletadas'] = negociacao_detalhe_cliente_linhas_deletadas
            cursor.execute(queries_deletar_tudo.resposta_cliente_item(cotacao))
            resposta_cliente_item_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'resposta_cliente_item_linhas_deletadas'] = resposta_cliente_item_linhas_deletadas
            cursor.execute(queries_deletar_tudo.resposta_cliente(cotacao))
            resposta_cliente_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'resposta_cliente_linhas_deletadas'] = resposta_cliente_linhas_deletadas
            cursor.execute(queries_deletar_tudo.cotacao_item(cotacao))
            cotacao_item_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'cotacao_item_linhas_deletadas'] = cotacao_item_linhas_deletadas
            cursor.execute(queries_deletar_tudo.rel_cotacao_representante(cotacao))
            rel_cotacao_representante_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'rel_cotacao_representante_linhas_deletadas'] = rel_cotacao_representante_linhas_deletadas
            cursor.execute(queries_deletar_tudo.itens_clonagem(cotacao))
            itens_clonagem_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'itens_clonagem_linhas_deletadas'] = itens_clonagem_linhas_deletadas
            cursor.execute(queries_deletar_tudo.resposta_item(cotacao))
            resposta_item_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'resposta_item_linhas_deletadas'] = resposta_item_linhas_deletadas
            cursor.execute(queries_deletar_tudo.clonagem(cotacao))
            clonagem_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'clonagem_linhas_deletadas'] = clonagem_linhas_deletadas
            cursor.execute(queries_deletar_tudo.filial_resposta(cotacao))
            filial_resposta_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'filial_resposta_linhas_deletadas'] = filial_resposta_linhas_deletadas
            cursor.execute(queries_deletar_tudo.negociacao_detalhe_desconto(cotacao))
            negociacao_detalhe_desconto_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'negociacao_detalhe_desconto_linhas_deletadas'] = negociacao_detalhe_desconto_linhas_deletadas
            cursor.execute(queries_deletar_tudo.resposta(cotacao))
            resposta_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'resposta_linhas_deletadas'] = resposta_linhas_deletadas
            cursor.execute(queries_deletar_tudo.produto_historico_by_cotacao(cotacao))
            produto_historico_by_cotacao_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'produto_historico_by_cotacao_linhas_deletadas'] = produto_historico_by_cotacao_linhas_deletadas
            cursor.execute(queries_deletar_tudo.rel_cot_ped_campanha_by_cotacao_and_ped(cotacao))
            rel_cot_ped_campanha_by_cotacao_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'rel_cot_ped_campanha_by_cotacao_linhas_deletadas'] = rel_cot_ped_campanha_by_cotacao_linhas_deletadas
            cursor.execute(queries_deletar_tudo.erro_integracao_causa(cotacao))
            erro_integracao_causa_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'erro_integracao_causa_linhas_deletadas'] = erro_integracao_causa_linhas_deletadas
            cursor.execute(queries_deletar_tudo.erro_integracao(cotacao))
            erro_integracao_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'erro_integracao_linhas_deletadas'] = erro_integracao_linhas_deletadas
            cursor.execute(queries_deletar_tudo.historico_integracao_pedido(cotacao))
            historico_integracao_pedido_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'historico_integracao_pedido_linhas_deletadas'] = historico_integracao_pedido_linhas_deletadas
            cursor.execute(queries_deletar_tudo.imposto_nota_fiscal(cotacao))
            imposto_nota_fiscal_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'imposto_nota_fiscal_linhas_deletadas'] = imposto_nota_fiscal_linhas_deletadas
            cursor.execute(queries_deletar_tudo.nota_fiscal_eletronica_item(cotacao))
            nota_fiscal_eletronica_item_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'nota_fiscal_eletronica_item_linhas_deletadas'] = nota_fiscal_eletronica_item_linhas_deletadas
            cursor.execute(queries_deletar_tudo.duplicata_nota_fiscal(cotacao))
            duplicata_nota_fiscal_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'duplicata_nota_fiscal_linhas_deletadas'] = duplicata_nota_fiscal_linhas_deletadas
            cursor.execute(queries_deletar_tudo.nota_fiscal_eletronica(cotacao))
            nota_fiscal_eletronica_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'nota_fiscal_eletronica_linhas_deletadas'] = nota_fiscal_eletronica_linhas_deletadas
            cursor.execute(queries_deletar_tudo.pedido_by_cotacao(cotacao))
            pedido_by_cotacao_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'pedido_by_cotacao_linhas_deletadas'] = pedido_by_cotacao_linhas_deletadas
            cursor.execute(queries_deletar_tudo.filial_cotacao(cotacao))
            filial_cotacao_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'filial_cotacao_linhas_deletadas'] = filial_cotacao_linhas_deletadas
            cursor.execute(queries_deletar_tudo.cotacao_associada(cotacao))
            cotacao_associada_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'cotacao_associada_linhas_deletadas'] = cotacao_associada_linhas_deletadas
            cursor.execute(queries_deletar_tudo.historico_integracao_cotacao(cotacao))
            historico_integracao_cotacao_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'historico_integracao_cotacao_linhas_deletadas'] = historico_integracao_cotacao_linhas_deletadas
            cursor.execute(queries_deletar_tudo.pedido_cliente(cotacao))
            pedido_cliente_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'pedido_cliente_linhas_deletadas'] = pedido_cliente_linhas_deletadas
            cursor.execute(queries_deletar_tudo.pedido_representante(cotacao))
            pedido_representante_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'pedido_representante_linhas_deletadas'] = pedido_representante_linhas_deletadas
            cursor.execute(queries_deletar_tudo.pedido_fornecedor(cotacao))
            pedido_fornecedor_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'pedido_fornecedor_linhas_deletadas'] = pedido_fornecedor_linhas_deletadas
            cursor.execute(queries_deletar_tudo.pedido_agrupado(cotacao))
            pedido_agrupado_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'pedido_agrupado_linhas_deletadas'] = pedido_agrupado_linhas_deletadas
            cursor.execute(queries_deletar_tudo.historico_integracao_resposta(cotacao))
            historico_integracao_resposta_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'historico_integracao_resposta_linhas_deletadas'] = historico_integracao_resposta_linhas_deletadas
            cursor.execute(queries_deletar_tudo.rep_nao_participante_cot(cotacao))
            rep_nao_participante_cot_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'rep_nao_participante_cot_linhas_deletadas'] = rep_nao_participante_cot_linhas_deletadas
            cursor.execute(queries_deletar_tudo.produto_bloqueadosrf(cotacao))
            produto_bloqueadosrf_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'produto_bloqueadosrf_linhas_deletadas'] = produto_bloqueadosrf_linhas_deletadas
            cursor.execute(queries_deletar_tudo.bloqueio_retorno_faturamento(cotacao))
            bloqueio_retorno_faturamento_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'bloqueio_retorno_faturamento_linhas_deletadas'] = bloqueio_retorno_faturamento_linhas_deletadas
            cursor.execute(queries_deletar_tudo.bloqueio_valor_referencia_item(cotacao))
            bloqueio_valor_referencia_item_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'bloqueio_valor_referencia_item_linhas_deletadas'] = bloqueio_valor_referencia_item_linhas_deletadas
            cursor.execute(queries_deletar_tudo.bloqueio_valor_referencia(cotacao))
            bloqueio_valor_referencia_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'bloqueio_valor_referencia_linhas_deletadas'] = bloqueio_valor_referencia_linhas_deletadas
            cursor.execute(queries_deletar_tudo.codigo_ref_cotacao(cotacao))
            codigo_ref_cotacao_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'codigo_ref_cotacao_linhas_deletadas'] = codigo_ref_cotacao_linhas_deletadas
            cursor.execute(queries_deletar_tudo.cotacao_cliente_sic(cotacao))
            cotacao_cliente_sic_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'cotacao_cliente_sic_linhas_deletadas'] = cotacao_cliente_sic_linhas_deletadas
            cursor.execute(queries_deletar_tudo.rel_cot_campanha_by_cot(cotacao))
            rel_cot_campanha_by_cotacao_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'rel_cot_campanha_by_cotacao_linhas_deletadas'] = rel_cot_campanha_by_cotacao_linhas_deletadas
            cursor.execute(queries_deletar_tudo.cotacao(cotacao))
            cotacao_linhas_deletadas = cursor.rowcount
            obj_saida_linhas_deletadas[
                'cotacao_linhas_deletadas'] = cotacao_linhas_deletadas
            cursor.connection.commit()
            return obj_saida_linhas_deletadas
        except Exception as Error:
            logger.exception(f'Erro não esperado em [/{processo}]: {Error}',
                             extra={**default_graylog_fields,
                                    'SourceMethodName': processo})
            raise


def deletar_produto_sem_resposta_por_dia(oficial, data_inferior_a):
    rota_deletar_produto_sem_resposta_por_dia = 'deletar_produto_sem_resposta_por_dia'
    deletados = 0
    with conectar_db(oficial=oficial) as cursor:
        while True:
            try:
                cursor.execute(queries.delete_produto_sem_resposta_por_dia_de_1000_em_1000(data_inferior_a))
                linhas_deletadas = cursor.rowcount
                if linhas_deletadas == 0:
                    break
                cursor.connection.commit()
                deletados += linhas_deletadas
                logger.info(
                    f'{deletados} linhas deletadas com sucesso, datacadastro: {data_inferior_a}',
                    extra={**default_graylog_fields,
                           'SourceMethodName': rota_deletar_produto_sem_resposta_por_dia})
            except Exception as Error:
                logger.exception(
                    f'Erro não esperado no endpoint [{rota_deletar_produto_sem_resposta_por_dia}]: {Error}',
                    extra={**default_graylog_fields,
                           'SourceMethodName': rota_deletar_produto_sem_resposta_por_dia})
                break

def salvar_controle(dados, arquivo, lock_json):
    with lock_json:
        with open(arquivo, 'w') as f:
            json.dump(dados, f)
def ler_controle(arquivo, lock_json):
    with lock_json:
        with open(arquivo, 'r') as f:
            return json.load(f)


def deletar_produto_sem_resposta_mais_de_45_dias(oficial, lock_json):
    salvar_controle({"continuar_deletando_produto_sem_resposta": True},
                    'processos/cotacao/deletar_produto_sem_resposta_e_cotacao/'
                                           'objeto_controle_produto_sem_resposta.json', lock_json)
    deletados = 0
    with conectar_db(oficial=oficial) as cursor:
        while ler_controle('processos/cotacao/deletar_produto_sem_resposta_e_cotacao/'
                                           'objeto_controle_produto_sem_resposta.json',
                                           lock_json)['continuar_deletando_produto_sem_resposta']:
            try:
                cursor.execute(queries.delete_produto_sem_resposta_por_dia_de_1000_em_1000_mais_de_45_dias())
                linhas_deletadas = cursor.rowcount
                if linhas_deletadas == 0:
                    break
                cursor.connection.commit()
                deletados += linhas_deletadas
                logger.info(f'{deletados} linhas deletadas com sucesso da PRODUTOSEMRESPOSTA')
            except Exception as e:
                logger.exception(f'Erro ao deletar: {e}')
                break