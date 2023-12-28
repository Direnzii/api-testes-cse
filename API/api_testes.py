import json
from flask import Flask, request, make_response
from flask_cors import CORS
import io
import main_invoice_response
from main_response_quotation import processar

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


def abrirArquivo(nome):
    with open(nome, 'r') as file:
        return json.load(file)


config_gerais_ex = abrirArquivo('arquivos_config/config_geral.example.json')


def validar_json(json_inval):
    try:
        id_cotacao = json_inval["id_cotacao"]
        config_geral = json_inval["config_geral"]
        multipla_resposta = config_geral["multipla_resposta"]["multipla"]
        qtd_problema_de_minimo = json_inval["config_produto"]['qtd_problema_de_minimo']
        qtd_problema_de_embalagem = json_inval["config_produto"]['qtd_problema_de_embalagem']
        oportunidades = json_inval["config_produto"]['oportunidades']
        oportunidades_fixada = json_inval["config_produto"]['oportunidades_fixada']
        produtos_sem_st = json_inval["config_produto"]['produtos_sem_st']
        so_com_st = json_inval["config_produto"]['so_com_st']
        sem_estoque = json_inval["config_produto"]['sem_estoque']
        resposta_parcial_em_porcentagem = json_inval["config_geral"]['resposta_parcial_em_porcentagem']
        minimo_de_faturamento = json_inval["config_geral"]['minimo_de_faturamento']
        oportunidade = json_inval['tipo_teste']['looping']
        motivo_por_resposta = config_geral['motivo_por_resposta']['quantos']
        aguardando_resposta = config_geral['motivo_por_resposta']['aguardando_resposta']
        if all([id_cotacao,
                qtd_problema_de_minimo or qtd_problema_de_minimo == 0,
                qtd_problema_de_embalagem or qtd_problema_de_embalagem == 0,
                oportunidades or oportunidades == 0,
                oportunidades_fixada or oportunidades_fixada == 0,
                produtos_sem_st or produtos_sem_st == 0,
                so_com_st or so_com_st == 0,
                multipla_resposta in [True, False],
                sem_estoque in [True, False],
                resposta_parcial_em_porcentagem in [True, False],
                not minimo_de_faturamento or minimo_de_faturamento > 0,
                oportunidade in [True, False],
                motivo_por_resposta >= 0,
                aguardando_resposta >= 0]):
            return True
    except ValueError:
        return False


def gravar_na_fila(mensagem_json):
    try:
        with io.open('../arquivos_config/fila.json', 'r', encoding='utf-8') as file:
            lista_atual = json.load(file)
            lista_atual.append(mensagem_json)
            with open('../arquivos_config/fila.json', 'w', encoding='utf-8') as f:
                json.dump(lista_atual, f, ensure_ascii=False)
        return True
    except ValueError:
        return False


def responder():
    return make_response({'result': 'Solicitação de resposta enviada para a fila com sucesso'}, 200)


@app.route('/', methods=['GET', ])
def status_ok():
    return make_response({'result': 'API rodando'}, 200)


@app.route('/cotacao/example', methods=['GET', ])
def example():
    return make_response(config_gerais_ex, 200)


@app.route('/cotacao/fila_responder', methods=['POST', ])
def gravar_fila():
    try:
        body = request.get_data()
        config_gerais = json.loads(body)
        if validar_json(config_gerais):
            if gravar_na_fila(config_gerais):
                return responder()
            else:
                return responder()
        else:
            return make_response({'result': 'json inesperado, siga o json de exemplo'}, 400)
    except Exception as E:
        print(E)
        return make_response({'result': 'json inesperado, siga o json de exemplo'}, 400)


@app.route('/cotacao/responder', methods=['POST', ])
def responder_cotacao():
    try:
        body = request.get_data()
        config_gerais = json.loads(body)
        if validar_json(config_gerais):
            processar(config_gerais)
            return make_response({'result': 'mensagens enviadas para a fila'}, 200)
        else:
            return make_response({'result': 'json inesperado, siga o json de exemplo'}, 400)
    except Exception as E:
        print(E)
        return make_response({'result': 'json inesperado, siga o json de exemplo'}, 400)


@app.route('/retorno/pedido', methods=['POST', ])
def retorno_cotacao():
    body = request.get_data()
    body = json.loads(body)
    main_invoice_response.processar_retorno(body)
    return make_response({"return": "deu bom"}, 200)


app.run(host='0.0.0.0', port=80)
