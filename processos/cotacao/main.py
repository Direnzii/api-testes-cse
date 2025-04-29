from processos.cotacao.dto.dto import gerar_dto_v2
from processos.cotacao.utils import utils
from server.instance import log

logger = log.logger
default_graylog_fields = log.default_graylog_fields


def processar_v2(payload):
    try:
        with utils.conectar_db(oficial=payload.config_geral.oficial) as cursor:
            if utils.checar_vencimento_v2(cursor=cursor,
                                          cotacao=payload.idcotacao):  # verificar se a cotacao esta vencida
                list_dto = gerar_dto_v2(payload, cursor)
                sucessos = []
                demais_motivos = []
                for dto in list_dto:
                    if payload.config_geral.aleatorizar_quantidade_respondida:
                        dto = utils.aleatorizar_resposta_itens_v2(dto)
                    if utils.motivo_resposta(dto) == "Sucesso":
                        sucessos.append(dto)
                    else:
                        demais_motivos.append(dto)
                utils.enviar_respostas(demais_motivos, oficial=payload.config_geral.oficial)
                utils.enviar_respostas(list_dtos=sucessos, oficial=payload.config_geral.oficial)
                logger.info('Processo finalizado com sucesso',
                            extra={**default_graylog_fields,
                                   'idcotacao': payload.idcotacao})
    except Exception:
        raise
