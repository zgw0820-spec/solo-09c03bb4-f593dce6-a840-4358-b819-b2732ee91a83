FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY gateway ./gateway

RUN mkdir -p /srv/data
ENV GATEWAY_STATE_PATH=/srv/data/gateway.json

EXPOSE 9000
CMD ["uvicorn", "gateway.main:app", "--host", "0.0.0.0", "--port", "9000"]
