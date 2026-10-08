# Ingest service image. Build from the repo root:
#   docker build -f services/ingest/Dockerfile -t rfam-ingest .

# Stage 1: install dependencies into a venv (build tools stay out of the final image).
FROM python:3.12-slim AS builder
RUN python -m venv /venv
COPY services/ingest/requirements.txt /tmp/requirements.txt
RUN /venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt

# Stage 2: runtime. Only the venv and the code this service needs.
FROM python:3.12-slim
ENV PATH=/venv/bin:$PATH \
    PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RULES_FILE=/app/config/rules.yaml
RUN useradd --system --uid 10001 --no-create-home app
COPY --from=builder /venv /venv
WORKDIR /app
# Shared package (schemas, message contract, DB models) is copied into both images.
COPY models ./models
COPY db ./db
COPY shared/rfam_common ./rfam_common
COPY services/ingest/ingest ./ingest
COPY services/ingest/config ./config
USER app
EXPOSE 8000
# slim has no curl: use Python's urllib. /health checks DB + Kafka.
HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=5 \
  CMD ["python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=4).status == 200 else 1)"]
CMD ["uvicorn", "ingest.main:app_factory", "--factory", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
