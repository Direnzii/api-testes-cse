from fastapi.testclient import TestClient


def checar_422_oficial_idcotacao(body, tipo):
    assert body['detail'][0]['type'] == 'missing'
    assert body['detail'][0]['loc'][0] == 'query'
    if tipo == 'oficial':
        assert body['detail'][0]['loc'][1] == 'oficial'
    elif tipo == 'idcotacao':
        assert body['detail'][0]['loc'][1] == 'idcotacao'
    assert body['detail'][0]['msg'] == "Field required"
    assert body['detail'][0]['input'] is None


def test_get_consultar_pmir_geracao(client: TestClient) -> None:
    rota = '/api-testes/banco/consultar_pmir_geracao'
    response = client.get(f"{rota}?idcotacao=131518&oficial=false")
    body = response.json()[0]
    assert response.status_code == 200
    assert isinstance(body['result'], list)
    response = client.get(f"{rota}?idcotacao=131518&oficial=true")
    body = response.json()[0]
    assert response.status_code == 200
    assert isinstance(body['result'], list)
    response = client.get(f"{rota}?idcotacao=131518")
    body = response.json()
    assert response.status_code == 422
    checar_422_oficial_idcotacao(body, tipo='oficial')
    response = client.get(f"{rota}?oficial=false")
    body = response.json()
    assert response.status_code == 422
    checar_422_oficial_idcotacao(body, tipo='idcotacao')


def test_get_situacao_pedidos_by_cotacao(client: TestClient) -> None:
    rota = '/api-testes/banco/situacao_pedidos_by_cotacao'
    response = client.get(f"{rota}?idcotacao=131518&oficial=false")
    body = response.json()[0]
    assert response.status_code == 200
    assert isinstance(body['result'], object)
    response = client.get(f"{rota}?idcotacao=131518&oficial=true")
    body = response.json()[0]
    assert response.status_code == 200
    assert isinstance(body['result'], object)
    response = client.get(f"{rota}?idcotacao=131518")
    body = response.json()
    assert response.status_code == 422
    checar_422_oficial_idcotacao(body, tipo='oficial')
    response = client.get(f"{rota}?oficial=false")
    body = response.json()
    assert response.status_code == 422
    checar_422_oficial_idcotacao(body, tipo='idcotacao')


def test_reiniciar_completamente_cotacao(client: TestClient) -> None:
    rota = '/api-testes/banco/reiniciar_completamente_cotacao'
    response = client.patch(f"{rota}?idcotacao=131518&oficial=false")
    body = response.json()[0]
    assert response.status_code == 200
    assert isinstance(body['result'], object)
    response = client.patch(f"{rota}?idcotacao=131518&oficial=true")
    body = response.json()[0]
    assert response.status_code == 200
    assert isinstance(body['result'], object)
    response = client.patch(f"{rota}?idcotacao=131518")
    body = response.json()
    assert response.status_code == 422
    checar_422_oficial_idcotacao(body, tipo='oficial')
    response = client.patch(f"{rota}?oficial=false")
    body = response.json()
    assert response.status_code == 422
    checar_422_oficial_idcotacao(body, tipo='idcotacao')


def test_atualizar_testes_automaticos(client: TestClient) -> None:
    rota = '/api-testes/banco/atualizar_testes_automaticos'
    response = client.head(rota)
    assert response.status_code == 200


def test_deletar_pmir(client: TestClient) -> None:
    rota = '/api-testes/banco/deletar_pmir'
    response = client.delete(f"{rota}?idcotacao=131518&oficial=false")
    body = response.json()[0]
    assert response.status_code == 200
    assert isinstance(body['result'], object)
    response = client.delete(f"{rota}?oficial=false")
    body = response.json()
    assert response.status_code == 422
    checar_422_oficial_idcotacao(body, tipo='idcotacao')
    response = client.delete(f"{rota}?idcotacao=131518")
    body = response.json()
    assert response.status_code == 422
    checar_422_oficial_idcotacao(body, tipo='oficial')
