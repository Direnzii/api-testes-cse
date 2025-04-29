from fastapi.testclient import TestClient
from tests.utils import checar_422_oficial_idcotacao, validar_campo_obrigatorio, deletar_retorno_test


def test_retorno_cotacao(client: TestClient) -> None:
    rota = '/api-testes/retorno/cotacao'
    request_body = {
        "faturado": True,
        "faturado_parcialmente_item": True,
        "faturado_parcialmente_quantidade": True,
        "idcotacao": 131518,
        "motivo": "Não faturado",
        "random": True,
        "oficial": True
    }
    response = client.put(rota, json=request_body)
    body = response.json()[0]
    assert response.status_code == 200
    assert isinstance(body['result'], object)
    request_body["oficial"] = False
    deletar_retorno_test(client, request_body['idcotacao'], request_body['oficial'])
    response = client.put(rota, json=request_body)
    body = response.json()[0]
    assert response.status_code == 200
    assert isinstance(body['result'], object)
    for key in request_body.keys():
        validar_campo_obrigatorio(client, rota, request_body, key, metodo="put")


def test_retorno_pedido(client: TestClient) -> None:
    idpedido = 438240
    rota = '/api-testes/retorno/pedido'
    request_body = {
        "faturado": True,
        "faturado_parcialmente_item": True,
        "faturado_parcialmente_quantidade": True,
        "idpedido": idpedido,
        "motivo": "Não faturado",
        "oficial": False
    }
    response = client.put(rota, json=request_body)
    body = response.json()[0]
    assert response.status_code == 200
    assert isinstance(body['result'], object)
    request_body["oficial"] = True
    deletar_retorno_test(client, request_body['idpedido'], request_body['oficial'])
    response = client.put(rota, json=request_body)
    body = response.json()[0]
    assert response.status_code == 200
    assert isinstance(body['result'], object)
    for key in request_body.keys():
        validar_campo_obrigatorio(client, rota, request_body, key, metodo="put")


def test_retorno_deletar(client: TestClient) -> None:
    cotacao = 131518
    response = deletar_retorno_test(client, cotacao, False)
    print(f'DIRENZI DEBUG::::: RESPONSE: {response}')
    body = response.json()[0]
    assert response.status_code == 200
    assert isinstance(body['result'], object)
    response = deletar_retorno_test(client, idcotacao=cotacao, oficial=True)
    body = response.json()[0]
    assert response.status_code == 200
    assert isinstance(body['result'], object)
    response = deletar_retorno_test(client, idcotacao=cotacao)
    body = response.json()
    assert response.status_code == 422
    checar_422_oficial_idcotacao(body, 'oficial')
    response = deletar_retorno_test(client, oficial=False)
    body = response.json()
    assert response.status_code == 422
    checar_422_oficial_idcotacao(body, 'idcotacao')
