from fastapi.testclient import TestClient
from server.instance import server
from dotenv import load_dotenv
load_dotenv()



def test_get_health(client: TestClient) -> None:
    response = client.get("/api-testes/health")
    body = response.json()[0]
    assert response.status_code == 200
    print('DEBUGGG AAAAA ', response, body)
    assert body['result'] == 'API-Testes rodando'
    assert body['api_version'] == server.versao
