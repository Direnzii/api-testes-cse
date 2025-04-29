from fastapi.testclient import TestClient
from tests.utils import checar_422_oficial_idcotacao, validar_campo_obrigatorio


def test_alterar_vencimento(client: TestClient) -> None:
    rota = '/api-testes/cotacao/alterar_vencimento'
    request_body = {
        "idcotacao": 130432,
        "quantos_dias": 30,
        "para_mais": True,
        "motivoresposta_de": "VENCIDA",
        "motivoresposta_para": "RESPONDIDA",
        "oficial": False
    }
    response = client.patch(rota, json=request_body)
    body = response.json()[0]
    assert response.status_code == 200
    assert isinstance(body['result'], object)
    for key in request_body.keys():
        validar_campo_obrigatorio(client, rota, request_body, key, metodo="patch")


def test_mudar_para_em_analise(client: TestClient) -> None:
    rota = '/api-testes/cotacao/mudar_para_em_analise'
    response = client.patch(f"{rota}?idcotacao=130432&oficial=false")
    body = response.json()[0]
    assert response.status_code == 200
    assert isinstance(body['result'], object)
    response = client.patch(f"{rota}?idcotacao=130432&oficial=true")
    body = response.json()[0]
    assert response.status_code == 200
    assert isinstance(body['result'], object)
    response = client.patch(f"{rota}?idcotacao=130432")
    assert response.status_code == 200
    response = client.patch(f"{rota}?oficial=false")
    body = response.json()
    assert response.status_code == 422
    checar_422_oficial_idcotacao(body=body, tipo="idcotacao")


def test_excluir_registros(client: TestClient) -> None:
    rota = '/api-testes/cotacao/excluir_registros'
    response = client.delete(f"{rota}?idcotacao=130908&oficial=false")
    body = response.json()[0]
    assert response.status_code == 200
    assert isinstance(body['result'], object)
    response = client.delete(f"{rota}")
    assert response.status_code == 422
