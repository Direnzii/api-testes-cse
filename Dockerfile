# Dockerfile, Image, Container
FROM python:3.12.7-slim AS base

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

FROM base AS build

WORKDIR /app

RUN pip install pipenv

COPY Pipfile /app
COPY Pipfile.lock /app
RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy

FROM base AS runtime

WORKDIR /app

COPY --from=build /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY . /app

ENTRYPOINT ["python", "main.py"]
