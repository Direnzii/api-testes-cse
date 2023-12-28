import json

from cotefacilib.dtos.payments import *


def abrirArquivo(nome):
    with open(nome, 'r') as file:
        return json.load(file)


def processar_condicao(conf):
    cnpjFornecedor = conf["cnpjFornecedor"]
    codigoRepresentante = conf["codigoRepresentante"]
    cnpjCliente = conf["cnpjCliente"]
    quantidade = conf["quantidade"]
    itens = payment_item_data_to_ctf_dto()
    headerDTO = payments_data_to_ctf_dto(supplier_cnpj={cnpjFornecedor,
                                                        itens,
                                                        codigoRepresentante})
    return


config_gerais = abrirArquivo('arquivos_config/config_geral_condicao.json')
processar_condicao(config_gerais)
