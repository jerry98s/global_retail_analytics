# Streamlit dashboard for the Global Retail Analytics platform.
# Built for AWS App Runner (see infra/terraform/modules/streamlit) but runnable
# anywhere: `docker run -p 8501:8501 -e DASHBOARD_MODE=redshift ... <image>`.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY data.py app.py ./

EXPOSE 8501

# App Runner health check hits "/"; Streamlit also exposes /_stcore/health.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')" || exit 1

ENTRYPOINT ["streamlit", "run", "app.py"]
