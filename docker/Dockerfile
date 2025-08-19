FROM python:3.11-slim-buster

WORKDIR /app

ENV PYTHONUNBUFFERED 1

COPY pyproject.toml poetry.lock* ./ # Copy poetry.lock if you are using poetry

RUN pip install poetry && poetry install --no-root --no-dev

COPY . .
