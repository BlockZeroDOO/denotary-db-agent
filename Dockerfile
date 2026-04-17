FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY denotary_db_agent /app/denotary_db_agent
COPY docs /app/docs
COPY examples /app/examples

RUN pip install --no-cache-dir .

ENTRYPOINT ["denotary-db-agent"]

