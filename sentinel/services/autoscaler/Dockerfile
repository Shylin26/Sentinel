FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    docker==7.1.0 \
    prometheus-client==0.20.0

COPY services/autoscaler/autoscaler.py services/autoscaler/autoscaler.py

EXPOSE 9090

CMD ["python3", "services/autoscaler/autoscaler.py"]
