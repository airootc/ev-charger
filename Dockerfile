FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY api_server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

# Copy application code
COPY api_server/ ./api_server/
COPY frontend/ ./frontend/

# Remove backup files and __pycache__
RUN find . -name "*.bak.*" -delete && \
    find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null; true

EXPOSE 10000

CMD gunicorn api_server.server:app \
    -k uvicorn.workers.UvicornWorker \
    --bind 0.0.0.0:${PORT:-10000} \
    --workers 1 \
    --timeout 180
