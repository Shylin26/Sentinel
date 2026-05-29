FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    torch==2.3.0 --index-url https://download.pytorch.org/whl/cpu \
    sentencepiece==0.2.0 \
    grpcio==1.62.0 \
    grpcio-tools==1.62.0 \
    prometheus-client==0.20.0

COPY proto/ proto/
COPY model/src/ model/src/
COPY model/checkpoints/sentinel.pt model/checkpoints/sentinel.pt

RUN python -m grpc_tools.protoc -I proto --python_out=proto --grpc_python_out=proto proto/inference.proto

EXPOSE 50051 9101

CMD ["python3", "services/inference/server.py"]
