def checar_422_oficial_idcotacao(body, tipo) -> None:
    assert body['detail'][0]['type'] == 'missing'
    assert body['detail'][0]['loc'][0] == 'query'
    if tipo == 'oficial':
        assert body['detail'][0]['loc'][1] == 'oficial'
    elif tipo == 'idcotacao':
        assert body['detail'][0]['loc'][1] == 'idcotacao'
    assert body['detail'][0]['msg'] == "Field required"
    assert body['detail'][0]['input'] is None


def validar_campo_obrigatorio(client, rota, json_data, campo, metodo="patch") -> None:
    json_sem_campo = json_data.copy()
    json_sem_campo.pop(campo, None)
    metodo_http = getattr(client, metodo, None)
    response = metodo_http(rota, json=json_sem_campo)
    assert response.status_code == 422


def deletar_retorno_test(client, idcotacao=None, oficial=None):
    rota = '/api-testes/retorno/deletar'
    if idcotacao is None:
        response = client.delete(f"{rota}?oficial={oficial}")
        return response
    elif oficial is None:
        response = client.delete(f"{rota}?idcotacao={idcotacao}")
        return response
    response = client.delete(f"{rota}?idcotacao={idcotacao}&oficial={oficial}")
    assert response.status_code == 200
    return response
