# API-Testes

Esta API foi criada com o intuito de facilitar na realização de testes, automatizando resposta de cotação, retorno de faturamento e alterações no banco. Criada com Python, Docker e Pipenv

## Pré-requisitos

- [Docker](https://www.docker.com/get-started) instalado na sua máquina.

## Instalação

Use o gerenciador de pacotes [pip](https://pip.pypa.io/en/stable/).

**Clone este repositório:**
```bash
git clone <URL_DO_REPOSITORIO>
cd <NOME_DO_REPOSITORIO>
pip install pipenv
pipenv install
```

## Uso

```python
python3 api_testes.py
localhost:8000/api-testes/health
```

## Tecnologias e Site

[Py](https://www.python.org/), [DB](https://www.oracle.com/br/database/), [Flask](https://flask.palletsprojects.com/en/stable/)

## Status CI/CD
[![Deploy](https://github.com/devcotefacil/api-testes/actions/workflows/docker-image.yml/badge.svg)](https://github.com/devcotefacil/api-testes/actions/workflows/docker-image.yml)
