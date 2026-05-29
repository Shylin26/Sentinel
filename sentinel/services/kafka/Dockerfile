FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    grpcio==1.62.0 \
    grpcio-tools==1.62.0 \
    python-dotenv==1.0.1

COPY proto/ proto/
COPY services/kafka/diff_extractor.py services/kafka/diff_extractor.py

RUN python -m grpc_tools.protoc -I proto --python_out=proto --grpc_python_out=proto proto/inference.proto

CMD ["python3", "services/kafka/diff_extractor.py"]
