import uvicorn
from pydantic import BaseModel
from database import queries
from database.conectar import conectar_db
from database.utils import (atualizar_todos_testes_automaticos, get_todas_situacoes_pedido_by_cotacao,
                            deletar_completamente_cotacao_by_id_cotacao, deletar_produto_sem_resposta_por_dia,
                            deletar_produto_sem_resposta_mais_de_45_dias)
from processos.condicao_de_pagamento.main_payment import (processar_condicao_e_enviar_ftp_oficial,
                                                          gera_condicao_pagamento, desativar_condicao_pagamento)
from processos.cotacao.deletar_produto_sem_resposta_e_cotacao.deletar_cotacoes_banco import main_deletar_cotacao
from processos.cotacao.main import processar_v2
from processos.cotacao.utils import utils
from processos.monitoramento.utils import consulta_respostas, consultar_dados_representante
from processos.pedido.main import consultar_pmir, deletar_pmir
from processos.retorno_de_faturamento.main import processar_retorno_pedido, processar_retorno_cotacao, deletar_retorno
from server.instance import log, server
from processos import constants
from fastapi import File, UploadFile, Query
import json

app = server.app
logger = log.logger
default_graylog_fields = log.default_graylog_fields


@app.get('/api-testes/health', tags=['Padrão'])
def health_check():
    logger.info("API-Testes rodando", extra={**default_graylog_fields, 'SourceMethodName': '/'})
    try:
        with conectar_db() as cursor:
            cursor.execute(queries.health_check_db())
            result = cursor.fetchall()
            if result[0][0] == 1:
                return {'result': 'API-Testes rodando', 'api_version': server.versao}, 200
            else:
                return {'result': 'Result do banco foi diferente de OPEN'}, 500

    except Exception as e:
        logger.error(f"Erro ao realizar health check no banco de dados: {e}",
                     extra={**default_graylog_fields, 'SourceMethodName': '/'})
        return {'result': 'Erro ao conectar ao banco de dados', 'error': str(e)}, 500


class PayloadIdCotacao(BaseModel):
    idcotacao: int
    oficial: bool


@app.get('/api-testes/banco/consultar_pmir_geracao', tags=['Banco'])
async def consultar_pmir_geracao(idcotacao: int, oficial: bool):
    rota_consultar_pmir = '/banco/consultar_pmir_geracao'
    logger.info(f'Iniciando processo [{rota_consultar_pmir}]',
                extra={**default_graylog_fields, 'SourceMethodName': rota_consultar_pmir})
    try:
        logger.info(f'idcotacao [/consultarPmirGeracao]: {idcotacao}',
                    extra={**default_graylog_fields, 'SourceMethodName': rota_consultar_pmir})
        result = consultar_pmir(idcotacao, oficial)
        return {'result': result}, 200
    except Exception as Error:
        logger.exception(f'Erro não esperado no endpoint [{rota_consultar_pmir}]: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_consultar_pmir})
        return {'result': constants.exception}, 400


@app.get('/api-testes/banco/situacao_pedidos_by_cotacao', tags=['Banco'])
async def get_situacao_pedidos_by_cotacao(idcotacao: int, oficial: bool):
    rota_get_situacao_pedidos = '/banco/situacao_pedidos_by_cotacao'
    logger.info(f'Iniciando processo [{rota_get_situacao_pedidos}]',
                extra={**default_graylog_fields, 'SourceMethodName': rota_get_situacao_pedidos})
    try:
        logger.info(f'idcotacao [/getSituacaoPedidosByCotacao]: {idcotacao}',
                    extra={**default_graylog_fields, 'SourceMethodName': rota_get_situacao_pedidos})
        result = get_todas_situacoes_pedido_by_cotacao(cotacao=idcotacao, oficial=oficial)
        return {'result': result}, 200
    except Exception as Error:
        logger.exception(f'Erro não esperado no endpoint [{rota_get_situacao_pedidos}]: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_get_situacao_pedidos})
        return {'result': constants.exception}, 400


@app.patch('/api-testes/banco/reiniciar_completamente_cotacao', tags=['Banco'])
async def reiniciar_completamente_cotacao(idcotacao: int, oficial: bool):
    rota_reiniciar_completamente_cotacao = '/banco/reiniciar_completamente_cotacao'
    logger.info(f'Iniciando processo [{rota_reiniciar_completamente_cotacao}]',
                extra={**default_graylog_fields, 'SourceMethodName': f'{rota_reiniciar_completamente_cotacao}'})
    try:
        logger.info(f'idcotacao [{rota_reiniciar_completamente_cotacao}]: {idcotacao}',
                    extra={**default_graylog_fields, 'SourceMethodName': rota_reiniciar_completamente_cotacao})
        utils.mudar_para_em_analise(idcotacao=idcotacao, oficial=oficial,
                                    situacao=1)  # coloco situacao 1 porque é a
        # cotacao em andamento
        atualizar_todos_testes_automaticos(cote=idcotacao, oficial=oficial)
        return {'result': f'Cotação {idcotacao} reiniciada completamente com sucesso'}, 200
    except Exception as Error:
        logger.exception(f'Erro não esperado no endpoint [{rota_reiniciar_completamente_cotacao}]: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_reiniciar_completamente_cotacao})
        return {'result': constants.exception}, 400


@app.head('/api-testes/banco/atualizar_testes_automaticos', tags=['Banco'])
async def atualizar_testes_automaticos():
    rota_atualizar_testes_automaticos = '/banco/atualizar_testes_automaticos'
    logger.info(f'Iniciando processo [{rota_atualizar_testes_automaticos}]',
                extra={**default_graylog_fields, 'SourceMethodName': rota_atualizar_testes_automaticos})
    try:
        atualizar_todos_testes_automaticos()
        return {'result': 'Todas as cotações configuradas internamente na API foram atualizadas'}, 200
    except Exception as Error:
        logger.exception(f'Erro não esperado no endpoint [{rota_atualizar_testes_automaticos}]: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_atualizar_testes_automaticos})
        return {'result': constants.exception}, 400


@app.delete('/api-testes/banco/deletar_pmir', tags=['Banco'])
async def deletar_pmir_rota(idcotacao: int, oficial: bool):
    rota_deletar_pmir = '/banco/deletarPmir'
    logger.info('Iniciando processo [/deletarPmir]',
                extra={**default_graylog_fields, 'SourceMethodName': rota_deletar_pmir})
    try:
        logger.info(f'idcotacao [/deletarPmir]: {idcotacao}',
                    extra={**default_graylog_fields, 'SourceMethodName': rota_deletar_pmir})
        deletar_pmir(idcotacao, oficial)
        return {'result': 'PMIR deletada com sucesso'}, 200
    except Exception as Error:
        logger.exception(f'Erro não esperado no endpoint [/deletarPmir]: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_deletar_pmir})
        return {'result': constants.exception}, 400


@app.delete('/api-testes/banco/deletar_completamente_cotacao', tags=['Banco'])
async def deletar_completamente_cotacao(idcotacao: int, oficial: bool):
    rota_deletar_completamente_cotacao = '/banco/deletar_completamente_cotacao'
    logger.info(f'Iniciando processo [{rota_deletar_completamente_cotacao}]',
                extra={**default_graylog_fields, 'SourceMethodName': rota_deletar_completamente_cotacao})
    try:
        logger.info(f'idcotacao [{rota_deletar_completamente_cotacao}]: {idcotacao}',
                    extra={**default_graylog_fields, 'SourceMethodName': rota_deletar_completamente_cotacao})
        obj_saida_linhas_deletadas = deletar_completamente_cotacao_by_id_cotacao(idcotacao, oficial)
        if oficial:
            ambiente = 'Oficial'
        else:
            ambiente = 'Demo'
        return {'result': 'Sucesso', 'ambiente': ambiente, 'linhas_deletadas': obj_saida_linhas_deletadas}, 200
    except Exception as Error:
        logger.exception(f'Erro não esperado no endpoint [{rota_deletar_completamente_cotacao}]: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_deletar_completamente_cotacao})
        return {'result': constants.exception}, 400


@app.delete('/api-testes/banco/deletar_produto_sem_resposta_por_dia', tags=['Banco'])
async def deletar_produto_sem_resposta_por_dia_rota(oficial: bool, data_inferior_a: str):
    rota_deletar_produto_sem_resposta_por_dia = '/banco/deletar_produto_sem_resposta_por_dia'
    logger.info(f'Iniciando processo [{rota_deletar_produto_sem_resposta_por_dia}]',
                extra={**default_graylog_fields, 'SourceMethodName': rota_deletar_produto_sem_resposta_por_dia})
    try:
        deletar_produto_sem_resposta_por_dia(oficial=oficial, data_inferior_a=data_inferior_a)
        return {'result': constants.sucesso}, 200
    except Exception as Error:
        logger.exception(f'Erro não esperado no endpoint [{rota_deletar_produto_sem_resposta_por_dia}]: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_deletar_produto_sem_resposta_por_dia})
        return {'result': constants.exception}, 400


import threading
import asyncio
lock_json = server.lock_json

@app.delete('/api-testes/banco/iniciar_delecao_produto_sem_resposta_mais_de_45_dias', tags=['Restrict'])
async def iniciar_delecao(oficial: bool):
    thread_delecao = threading.Thread(target=deletar_produto_sem_resposta_mais_de_45_dias, args=(oficial, lock_json))
    thread_delecao.start()
    return {'result': 'Processo de deleção iniciado'}, 202


@app.get('/api-testes/banco/parar_delecao_produto_sem_resposta_mais_de_45_dias', tags=['Restrict'])
async def parar_delecao():
    def salvar_controle(dados):
        with lock_json:
            with open(
                    'processos/cotacao/deletar_produto_sem_resposta_e_cotacao/objeto_controle_produto_sem_resposta.json', 'w') as f:
                json.dump(dados, f)
    salvar_controle({"continuar_deletando_produto_sem_resposta": False})
    return {'result': 'Parada solicitada'}, 200

@app.delete('/api-testes/banco/iniciar_delecao_cotacoes', tags=['Restrict'])
async def iniciar_delecao():
    def run_coroutine():
        asyncio.run(main_deletar_cotacao())
    thread_delecao = threading.Thread(target=run_coroutine)
    thread_delecao.start()
    return {'result': 'Processo de deleção iniciado'}, 202


@app.get('/api-testes/banco/parar_delecao_cotacoes', tags=['Restrict'])
async def parar_delecao():
    def salvar_controle(dados):
        with lock_json:
            with open(
                    'processos/cotacao/deletar_produto_sem_resposta_e_cotacao/objeto_controle_cotacao.json', 'w') as f:
                json.dump(dados, f)
    salvar_controle({"continuar_deletando_cotacao": False})
    return {'result': 'Parada solicitada'}, 200

class PayloadRetornoPedido(BaseModel):
    faturado: bool
    faturado_parcialmente_item: bool
    faturado_parcialmente_quantidade: bool
    idpedido: int
    motivo: str
    oficial: bool


@app.put('/api-testes/retorno/pedido', tags=['Retorno'])
async def retorno_pedido(payload: PayloadRetornoPedido):
    rota_retorno_pedido = '/retorno/pedido'
    logger.info(f'Iniciando processo [{rota_retorno_pedido}]',
                extra={**default_graylog_fields, 'SourceMethodName': rota_retorno_pedido})
    try:
        logger.info(f'Body [/pedido]: {payload}',
                    extra={**default_graylog_fields, 'SourceMethodName': rota_retorno_pedido})
        response = processar_retorno_pedido(payload)
        logger.info(f"Retorno[s] gerado[s] com constants.sucesso, finalizando processo, response: {response}",
                    extra={**default_graylog_fields,
                           'SourceMethodName': rota_retorno_pedido})
        return {'result': constants.sucesso}, 200
    except Exception as Error:
        logger.exception(f'Erro não esperado no endpoint [{rota_retorno_pedido}]: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_retorno_pedido})
        return {'result': constants.exception}, 400


class PayloadRetornoCotacao(BaseModel):
    faturado: bool
    faturado_parcialmente_item: bool
    faturado_parcialmente_quantidade: bool
    idcotacao: int
    motivo: str
    random: bool
    oficial: bool


@app.put('/api-testes/retorno/cotacao', tags=['Retorno'])
async def retorno_cotacao(payload: PayloadRetornoCotacao):
    rota_retorno_cotacao = '/retorno/cotacao'
    logger.info(f'Iniciando processo [{rota_retorno_cotacao}]',
                extra={**default_graylog_fields, 'SourceMethodName': rota_retorno_cotacao})
    try:
        logger.info(f'Body [{rota_retorno_cotacao}]: {payload}',
                    extra={**default_graylog_fields, 'SourceMethodName': rota_retorno_cotacao})
        response = processar_retorno_cotacao(payload)
        logger.info(f"Retorno[s] gerado[s] com constants.sucesso, finalizando processo, response: {response}",
                    extra={**default_graylog_fields,
                           'SourceMethodName': rota_retorno_cotacao})
        return {'result': constants.sucesso}, 200
    except Exception as Error:
        logger.exception(f'Erro não esperado no endpoint [{rota_retorno_cotacao}]: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_retorno_cotacao})
        return {'result': constants.exception}, 400


@app.delete('/api-testes/retorno/deletar', tags=['Retorno'])
async def deletar(idcotacao: int, oficial: bool):
    rota_retorno_deletar = '/retorno/deletar'
    logger.info(f'Iniciando processo [{rota_retorno_deletar}]',
                extra={**default_graylog_fields, 'SourceMethodName': rota_retorno_deletar})
    try:
        logger.info(f'Body [{rota_retorno_deletar}]: {idcotacao}',
                    extra={**default_graylog_fields, 'SourceMethodName': rota_retorno_deletar})
        response = deletar_retorno(idcotacao=idcotacao, oficial=oficial)
        logger.info(f"Delete de retorno[s] realizado com constants.sucesso, finalizando processo, response: {response}",
                    extra={**default_graylog_fields,
                           'SourceMethodName': rota_retorno_deletar})
        return {'result': constants.sucesso}, 200
    except Exception as Error:
        logger.exception(f'Erro não esperado no endpoint [{rota_retorno_deletar}]: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_retorno_deletar})
        return {'result': constants.exception}, 400


class PayloadConfigGeralMotivoPorResposta(BaseModel):
    aguardando_resposta: int
    motivo: bool
    quantos: int


class PayloadConfigGeralMultiplaResposta(BaseModel):
    combo: bool
    multipla: bool
    sem_resposta: bool


class PayloadOpcoesTipoPagamento(BaseModel):
    tipoPagamento: int
    random: bool

class PayloadConfigGeral(BaseModel):
    oficial: bool
    relacionados: bool
    aleatorizar_quantidade_respondida: bool
    atende: str
    minimo_de_faturamento: int
    motivo_por_resposta: PayloadConfigGeralMotivoPorResposta
    multipla_resposta: PayloadConfigGeralMultiplaResposta
    resposta_parcial_em_porcentagem: bool
    versao_arquivo: str
    opcoes_tipo_pagamento: PayloadOpcoesTipoPagamento


class PayloadConfigProdutoCashBack42(BaseModel):
    porcentagem_cashback: float
    qtd_de_itens: int


class PayloadConfigProdutoCashBack43(BaseModel):
    qtd_de_itens: int


class PayloadConfigProduto(BaseModel):
    zero_esquerda: bool
    cashback_4_2: PayloadConfigProdutoCashBack42
    cashback_4_3: PayloadConfigProdutoCashBack43
    oportunidades: int
    oportunidades_fixada: int
    produtos_sem_st: int
    qtd_problema_de_embalagem: int
    qtd_problema_de_minimo: int
    sem_estoque: bool
    so_com_st: int


class PayloadTipoTeste(BaseModel):
    looping: bool


class PayloadCotacaoResponder(BaseModel):
    idcotacao: int
    config_geral: PayloadConfigGeral
    config_produto: PayloadConfigProduto
    tipo_teste: PayloadTipoTeste


class PayloadAlterarVencimento(BaseModel):
    idcotacao: int
    quantos_dias: int
    para_mais: bool
    motivoresposta_de: str
    motivoresposta_para: str
    oficial: bool


class PayloadGerarCondicao(BaseModel):
    quantidade_de_condicoes: int
    pagamento_pendente: bool
    cnpj_fornecedor: str
    nome_representante: str
    cnpj_cliente: str


class PayloadDesativarCondicao(BaseModel):
    cnpj_cliente: str
    nome_representante: str
    cnpj_fornecedor: str


@app.post('/api-testes/cotacao/v2/responder', tags=['Cotação'])
async def responder_cotacao(payload: PayloadCotacaoResponder):
    rota_cotacao_responder = '/cotacao/v2/responder'
    try:
        logger.info(f'Iniciando processo [{rota_cotacao_responder}]',
                    extra={**default_graylog_fields,
                           'SourceMethodName': rota_cotacao_responder,
                           'idcotacao': payload.idcotacao})
        try:
            processar_v2(payload)
            return {'result': constants.sucesso}, 200
        except Exception as Error:
            logger.error(f'Payload não passou na validação de payload: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_cotacao_responder})
            return {'result': constants.exception}, 400
    except Exception as Error:
        logger.exception(f'Erro não esperado no endpoint [{rota_cotacao_responder}]: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_cotacao_responder})
        return {'result': constants.exception}, 400


@app.patch('/api-testes/cotacao/alterar_vencimento', tags=['Cotação'])
async def alterar_vencimento_rota(payload: PayloadAlterarVencimento):
    rota_alterar_vencimento = '/cotacao/alterar_vencimento'
    logger.info(f'Iniciando processo [{rota_alterar_vencimento}]',
                extra={**default_graylog_fields, 'SourceMethodName': rota_alterar_vencimento})
    try:
        utils.alterar_vencimento(payload)
        logger.info('Vencimento alterado com constants.sucesso, finalizando processo',
                    extra={**default_graylog_fields, 'SourceMethodName': rota_alterar_vencimento})
        return {'result': constants.sucesso_vencimento}, 200
    except Exception as Error:
        logger.exception(f'Erro não esperado no endpoint [{rota_alterar_vencimento}]: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_alterar_vencimento})
        return {'result': constants.exception}, 400


@app.patch('/api-testes/cotacao/mudar_para_em_analise', tags=['Cotação'])
async def mudar_para_em_analise_rota(idcotacao: int):
    rota_mudar_para_em_analise = '/cotacao/mudar_para_em_analise'
    logger.info(f'Iniciando processo [{rota_mudar_para_em_analise}]',
                extra={**default_graylog_fields, 'SourceMethodName': rota_mudar_para_em_analise})
    try:
        logger.info(f'Body [/mudar_para_em_analise]: {idcotacao}',
                    extra={**default_graylog_fields, 'SourceMethodName': rota_mudar_para_em_analise})
        utils.mudar_para_em_analise(idcotacao)
        logger.info('Mudança de estado de cotacao de finalizada para em analise realizada com constants.sucesso, '
                    'finalizando'
                    'processo',
                    extra={**default_graylog_fields, 'SourceMethodName': rota_mudar_para_em_analise})
        return {'result': constants.sucesso_em_analise}, 200
    except Exception as Error:
        logger.exception(f'Erro não esperado no endpoint [{rota_mudar_para_em_analise}]: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_mudar_para_em_analise})
        return {'result': constants.exception}, 400


@app.delete('/api-testes/cotacao/excluir_registros', tags=['Cotação'])
async def excluir_registros_rota(idcotacao: int, oficial: bool):
    rota_excluir_registros = '/cotacao/excluir_registros'
    logger.info(f'Iniciando processo [{rota_excluir_registros}]',
                extra={**default_graylog_fields, 'SourceMethodName': rota_excluir_registros})
    try:
        logger.info('excluindo registros', extra={'app': 'api-testes', 'operacao': rota_excluir_registros})
        logger.info(f'Body [/excluir_registros]: {idcotacao}',
                    extra={**default_graylog_fields, 'SourceMethodName': rota_excluir_registros})
        utils.excluir_registros(idcotacao, oficial)
        logger.info('Exclusao de registros de resposta da cotacao realizado com constants.sucesso, '
                    'finalizando processo',
                    extra={**default_graylog_fields, 'SourceMethodName': rota_excluir_registros})
        return {'result': constants.sucesso_excluir}, 200
    except Exception as Error:
        logger.exception(f'Erro não esperado no endpoint [{rota_excluir_registros}]: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_excluir_registros})
        return {'result': constants.exception}, 400


@app.post('/api-testes/condicao/subir_arquivo_ftp_oficial', tags=['Condição de pagamento'])
async def subir_arquivo_ftp_rota(file: UploadFile = File(...)):
    rota_subir_arquivo_ftp_rota = '/condicao/subir_arquivo_ftp'
    logger.info(f'Iniciando processo [{rota_subir_arquivo_ftp_rota}]',
                extra={**default_graylog_fields, 'SourceMethodName': rota_subir_arquivo_ftp_rota})
    content = await file.read()
    texto = content.decode("utf-8")
    try:
        response = processar_condicao_e_enviar_ftp_oficial(texto=texto)
        if response.status_code == 200:
            logger.info('Condição enviada para a API com sucesso',
                        extra={**default_graylog_fields, 'SourceMethodName': rota_subir_arquivo_ftp_rota})
        return {'result': constants.sucesso}, 200
    except Exception as Error:
        logger.exception(f'Erro não esperado no endpoint [{rota_subir_arquivo_ftp_rota}]: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_subir_arquivo_ftp_rota})
        return {'result': constants.exception}, 400


@app.post('/api-testes/condicao/gerar', tags=['Condição de pagamento'])
async def gerar_condicao_rota(payload: PayloadGerarCondicao):
    rota_gerar_condicao = '/condicao/gerar'
    logger.info(f'Iniciando processo [{rota_gerar_condicao}]',
                extra={**default_graylog_fields, 'SourceMethodName': rota_gerar_condicao})
    try:
        logger.info(f'Gerando condição de pagamento, Body [/condicao/gerar]: {payload}',
                    extra={**default_graylog_fields, 'SourceMethodName': rota_gerar_condicao})
        responses = await gera_condicao_pagamento(payload)
        for response in responses:
            if response.status_code == 200:
                logger.info('Condição enviada para a API com sucesso',
                            extra={**default_graylog_fields, 'SourceMethodName': rota_gerar_condicao})
        return {'result': constants.sucesso}, 200
    except Exception as Error:
        logger.exception(f'Erro não esperado no endpoint [{rota_gerar_condicao}]: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_gerar_condicao})
        return {'result': constants.exception}, 400


@app.patch('/api-testes/condicao/desativar', tags=['Condição de pagamento'])
async def desativar_condicao_rota(payload: PayloadDesativarCondicao):
    rota_desativar_condicao = '/condicao/desativar'
    logger.info(f'Iniciando processo [{rota_desativar_condicao}]',
                extra={**default_graylog_fields, 'SourceMethodName': rota_desativar_condicao})
    try:
        logger.info(f'Desativando condição de pagamento, body [/condicao/desativar]: {payload}',
                    extra={**default_graylog_fields, 'SourcheMethodName': rota_desativar_condicao})

        await desativar_condicao_pagamento(payload)
        logger.info('Desativação das condições de pagamento realizado com sucesso, '
                    'Finalizando processo ',
                    extra={**default_graylog_fields, 'SourceMethodName': rota_desativar_condicao})
        return {'result': constants.sucesso}, 200
    except Exception as Error:
        logger.exception(f'Erro não esperado no endpoint [{rota_desativar_condicao}]: {Error}',
                         extra={**default_graylog_fields,
                                'SourceMethodName': rota_desativar_condicao})
        return {'result': constants.exception}, 400


from typing import Optional
@app.get("/api-testes/monitoramento/buscar_resposta", tags=['Monitoramento'])
async def buscar_monitoramento(
    cotacao: Optional[str] = Query(None),
    data_vencimento: Optional[str] = Query(None),
    situacao: Optional[str] = Query(None),
    nome_fornecedor: Optional[str] = Query(None),
    cnpj_forn: Optional[str] = Query(None),
    nome_representante: Optional[str] = Query(None)
):
    filtros_dict = {"data_vencimento": data_vencimento, "situacao": situacao, "nome_fornecedor": nome_fornecedor,
                    "cnpj_forn": cnpj_forn, "nome_representante": nome_representante, "cotacao": cotacao}
    try:
        resultado = consulta_respostas(filtros_dict)
    except Exception as Error:
        logger.exception(f'Erro inesperado: {Error}')
        return {'result': 'Erro ao buscar dados'}, 400
    return {'result': resultado}, 200

@app.get("/api-testes/monitoramento/buscar_dados_representante", tags=['Monitoramento'])
async def buscar_monitoramento(
    id_resposta: int,
        id_cotacao: int
):
    try:
        resultado = consultar_dados_representante(id_resposta, id_cotacao)
    except Exception as Error:
        logger.exception(f'Erro inesperado: {Error}')
        return {'result': 'Erro ao buscar dados'}, 400
    return {'result': resultado}, 200


if __name__ == "__main__":
    uvicorn.run(app, port=8000, log_level="info", host="0.0.0.0")
