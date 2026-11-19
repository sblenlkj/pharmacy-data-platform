# ingest-сервис ДЗ-2: Kafka → iceberg.raw.cdc_events.
# (единый источник: dwh/ddl/00_raw_ctl.sql, scripts/infra/cdc_sources.json).
FROM python:3.12-slim-bookworm

COPY infra/ingest/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY infra/ingest/ingest_cdc.py /app/ingest_cdc.py
# 00_raw_ctl.sql и cdc_sources.json НЕ запекаем: compose монтирует их
# read-only в /app — образ не зависит от содержимого (меняются без rebuild)

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT ["python", "/app/ingest_cdc.py"]
