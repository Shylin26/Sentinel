FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    redis==5.0.4 \
    prometheus-client==0.20.0 \
    python-dotenv==1.0.1

COPY services/publisher/result_publisher.py services/publisher/result_publisher.py

EXPOSE 9102

CMD ["python3", "services/publisher/result_publisher.py"]
