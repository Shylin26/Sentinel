FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir \
    fastapi==0.111.0 \
    uvicorn[standard]==0.29.0 \
    kafka-python==2.0.2 \
    prometheus-client==0.20.0 \
    python-dotenv==1.0.1

# Copy service code
COPY services/webhook/main.py services/webhook/main.py

# Expose webhook + metrics ports
EXPOSE 8000

CMD ["uvicorn", "services.webhook.main:app", "--host", "0.0.0.0", "--port", "8000"]
