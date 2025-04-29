import asyncio
import sys
import aiohttp
import json
import time
import logging
import graypy
from database.conectar import conectar_db
from database.utils import salvar_controle, ler_controle
from server.instance import server

oficial_all = True
if oficial_all:
    ambiente = 'Oficial'
else:
    ambiente = 'demo'

logger = logging.getLogger()
logger.setLevel(logging.INFO)
stdout_handler = logging.StreamHandler(stream=sys.stdout)
gray_log_handler = graypy.GELFUDPHandler('logs.zitausch.com', 12201, debugging_fields=False)
gray_log_handler.setLevel(logging.INFO)
gray_log_handler.setFormatter(logging.Formatter("%(message)s"))
logging.basicConfig(format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
                    handlers=[stdout_handler, gray_log_handler])
default_graylog_fields = {'app': 'api-testes-deletar-cotacoes'}


def consultar_cotacoes(querie, num_tarefa, continuar_deletando, fake=False):
    try:
        with conectar_db(oficial=oficial_all) as cursor:
            cursor.execute(querie)
            result = cursor.fetchall()
            if not fake:
                logger.info(
                    f"Novo select de cotações com mais de 2 anos realizado, retorno de {len(result)} linhas da tabela "
                    f"para ser deletado, tarefa: {num_tarefa}, continuar_deletando: {continuar_deletando}", extra={**default_graylog_fields,
                                                 'SourceMethodName': f'deletar_cotacao_{ambiente}'})
            return result
    except Exception as Erro:
        logger.error(
            f"Erro na conexão com o banco de dados: {Erro}",
            extra={**default_graylog_fields,
                   'SourceMethodName': f'deletar_cotacao_{ambiente}'})


def appendar_lista(text, nome):
    try:
        with open(nome, 'r', encoding="utf-8") as file:
            data = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        data = []
    if isinstance(data, list) and text not in data:
        data.append(text)
        with open(nome, 'w', encoding="utf-8") as file:
            json.dump(data, file, indent=4, ensure_ascii=False)


async def deletar_cote(cotacao, oficial):
    start_time = time.perf_counter()
    params = {
        "oficial": 'true' if oficial else 'false',
        "idcotacao": cotacao
    }
    async with aiohttp.ClientSession() as session:
        async with session.delete("http://lb-api-demo-1200874622.us-east-1.elb.amazonaws.com/api-testes/banco"
                                  "/deletar_completamente_cotacao", params=params) as response:
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            response_text = await response.text()
            return response_text, elapsed_time


def arredondar(valor):
    return round(valor * 10) / 10


async def retorna_query(num: int) -> str:
    match num:
        case 1 | 2 | 3 | 4 | 5 | 6 | 7:
            like_value = f"{num}%"
        case 8:
            like_value = "12%"
        case 9:
            like_value = "13%"
        case 10:
            like_value = "14%"
        case 11:
            like_value = "15%"
        case _:
            raise ValueError(f"Valor inválido para 'num': {num}")

    query = f"""
        SELECT c.id 
        FROM cotacao c 
        WHERE c.DATACADASTRO < ADD_MONTHS(SYSDATE, -24)
        AND c.situacao = 3 
        AND LENGTH(c.id) = 7 
        AND c.id LIKE '{like_value}' 
        FETCH FIRST 1000 ROWS ONLY
    """
    return query



lock_json = server.lock_json

async def tarefa_exclusao(num_querie):
    salvar_controle({"continuar_deletando_cotacao": True},
                    'processos/cotacao/deletar_produto_sem_resposta_e_cotacao/'
                    'objeto_controle_cotacao.json', lock_json)
    contador_blocos = 1
    continuar_deletando = ler_controle('processos/cotacao/deletar_produto_sem_resposta_e_cotacao/'
                    'objeto_controle_cotacao.json', lock_json)['continuar_deletando_cotacao']
    while continuar_deletando:
        try:
            query = await retorna_query(num_querie)
            lista_cotes = consultar_cotacoes(querie=query, num_tarefa=num_querie,
                                             continuar_deletando=continuar_deletando, fake=False)
            if not lista_cotes:
                logger.warning(
                    f"Não existe mais retorno, total de blocos deletados: {contador_blocos}, tarefa: {num_querie}",
                    extra={**default_graylog_fields, 'SourceMethodName': f'deletar_cotacao_{ambiente}'})
                break

            start_time = time.perf_counter()
            contador_cotacoes = 1

            for cotacao in lista_cotes:
                cotacao = cotacao[0]
                try:
                    lista_cotes_fake = consultar_cotacoes(
                        querie=f'SELECT c.id FROM cotacao c WHERE c.idmatriz = {cotacao}',
                        num_tarefa=num_querie,
                        continuar_deletando=continuar_deletando,
                        fake=True)
                    if lista_cotes_fake:
                        logger.info(
                            f"Foi encontrado {len(lista_cotes_fake)} cotação(ões) fake para a cotação {cotacao},"
                            f"iniciando delete um a um, tarefa: {num_querie}", extra={**default_graylog_fields,
                                                                'SourceMethodName': f'deletar_cotacao_{ambiente}'})
                        for cotacao_fake in lista_cotes_fake:
                            cotacao_fake = cotacao_fake[0]
                            response, elapsed_time = await deletar_cote(cotacao_fake, oficial_all)
                            logger.info(
                                f"Cotação fake {cotacao_fake} deletada com sucesso em {elapsed_time}s, "
                                f"tarefa: {num_querie}, continuar_deletando: {continuar_deletando}",
                                extra={**default_graylog_fields,
                                       'SourceMethodName': f'deletar_cotacao_{ambiente}',
                                       'full_message': response})
                    response, elapsed_time = await deletar_cote(cotacao, oficial_all)
                    status_code = json.loads(response)[1]
                    if status_code == 200:
                        logger.info(
                            f"Cotação {cotacao} deletada com sucesso em {elapsed_time}s, "
                            f"dentro do bloco {contador_blocos} "
                            f"cotacao de numero {contador_cotacoes}, tarefa: {num_querie}, "
                            f"continuar_deletando: {continuar_deletando}",
                            extra={**default_graylog_fields,
                                   'SourceMethodName': f'deletar_cotacao_{ambiente}',
                                   'full_message': response})
                        contador_cotacoes += 1
                        continuar_deletando = ler_controle('processos/cotacao/deletar_produto_sem_resposta_e_cotacao/'
                                                           'objeto_controle_cotacao.json', lock_json)[
                            'continuar_deletando_cotacao']
                        if not continuar_deletando:
                            break
                    if status_code != 200:
                        logger.error(
                            f"Cotação {cotacao} falhou no delete, adicionando ela ao "
                            f"arquivo de cotacoes_erro.json, tarefa: {num_querie}, continuar_deletando: {continuar_deletando}",
                            extra={**default_graylog_fields,
                                   'SourceMethodName': f'deletar_cotacao_{ambiente}',
                                   'full_message': response})
                        appendar_lista(cotacao, nome='cotacoes_erro.json')
                except Exception as Erro:
                    logger.error(
                        f"Erro no processo de deletar cotação: {Erro}, tarefa: {num_querie}",
                        extra={**default_graylog_fields,
                               'SourceMethodName': f'deletar_cotacao_{ambiente}'})
                    appendar_lista(cotacao, nome='processos/cotacao/deletar_produto_sem_resposta_e_cotacao/cotacoes_erro.json')
            contador_blocos += 1
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            print(f"Todo o processo demorou {elapsed_time:.2f} segundos para terminar.")
            logger.info(
                f"Bloco {contador_blocos} com {len(lista_cotes)} cotações demorou {elapsed_time:.2f}"
                f" segundos para terminar de deletar, tarefa: {num_querie}", extra={**default_graylog_fields,
                                                              'SourceMethodName': f'deletar_cotacao_{ambiente}'})
        except Exception as Erro:
            logger.error(
                f"Erro no loop geral de deletar cotação: {Erro}, tarefa: {num_querie}",
                extra={**default_graylog_fields,
                       'SourceMethodName': f'deletar_cotacao_{ambiente}'})


async def main_deletar_cotacao():
    await asyncio.gather(
        tarefa_exclusao(1),
        tarefa_exclusao(2),
        tarefa_exclusao(3),
        tarefa_exclusao(4),
        tarefa_exclusao(5),
        tarefa_exclusao(6),
        tarefa_exclusao(7),
        tarefa_exclusao(8),
        tarefa_exclusao(9),
        tarefa_exclusao(10),
        tarefa_exclusao(11)
    )


if __name__ == '__main__':
    asyncio.run(main_deletar_cotacao())
