FROM python:3.11.10-bullseye AS base-python

RUN apt-get update
RUN apt-get install -y --no-install-recommends build-essential gcc

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

FROM python:3.11.10-bullseye AS base

COPY --from=base-python /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app
COPY source source
COPY requirements/prod.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir --upgrade -r prod.txt


ENV AIRBYTE_API_URL "http:/airbyte-server:8001"
ENV AIRBYTE_ENTRYPOINT "python -m source.main stream"
ENTRYPOINT ["python", "-m", "source.main", "spec"]
