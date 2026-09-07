# syntax=docker/dockerfile:1

FROM python:3.12-slim

# CIS Docker Benchmark 4.1 準拠: 非 root ユーザ (UID 1000) を作成
RUN groupadd --system --gid 1000 app \
    && useradd --system --uid 1000 --gid app --home /app --shell /sbin/nologin app

WORKDIR /app
COPY --chown=app:app requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --chown=app:app . .

USER app

EXPOSE 8001
CMD ["gunicorn", "--bind", "0.0.0.0:8001", "--workers", "2", "--access-logfile", "-", "app:app"]
