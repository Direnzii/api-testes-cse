from dotenv import load_dotenv
load_dotenv()
from typing import Iterator
from fastapi.testclient import TestClient
import pytest
from main import app


@pytest.fixture(scope='function')
def client() -> Iterator[TestClient]:
    with TestClient(app=app) as test:
        yield test
