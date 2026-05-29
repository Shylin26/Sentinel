import time
import docker
from kafka import KafkaConsumer, TopicPartition
from prometheus_client import start_http_server, Gauge

# Prometheus metrics
active_workers_gauge = Gauge("sentinel_active_workers", "Number of active inference workers")
kafka_lag_gauge = Gauge("sentinel_kafka_lag", "Kafka consumer lag for diff-chunks")

# Config
MIN_WORKERS = 1
MAX_WORKERS = 5
LAG_HIGH_WATERMARK = 10
LAG_LOW_WATERMARK = 2
TARGET_LAG_PER_WORKER = 5
COOLDOWN_SECONDS = 15
POLL_INTERVAL = 5

client = docker.from_env()
last_scale_time = 0

def get_kafka_lag() -> int:
    consumer = KafkaConsumer(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        group_id="lag-checker",
        enable_auto_commit=False,
    )
    partitions = consumer.partitions_for_topic("diff-chunks")
    if not partitions:
        consumer.close()
        return 0

    tps = [TopicPartition("diff-chunks", p) for p in partitions]
    consumer.assign(tps)

    end_offsets = consumer.end_offsets(tps)
    committed = {}
    for tp in tps:
        committed_offset = consumer.committed(tp)
        committed[tp] = committed_offset or 0

    total_lag = sum(
        end_offsets[tp] - committed[tp]
        for tp in tps
    )
    consumer.close()
    return total_lag

def count_workers() -> int:
    try:
        containers = client.containers.list(
            filters={"name": "sentinel-inference", "status": "running"}
        )
        return len(containers)
    except Exception:
        return 0

def spawn_workers(n: int):
    for i in range(n):
        print(f"  Spawning worker {i+1}/{n}...")
        client.containers.run(
            "python:3.11-slim",
            command="echo 'worker placeholder'",
            name=f"sentinel-inference-{int(time.time())}-{i}",
            detach=True,
            remove=True,
            labels={"sentinel": "inference"},
        )

def kill_workers(n: int):
    containers = client.containers.list(
        filters={"name": "sentinel-inference", "status": "running"}
    )
    for container in containers[:n]:
        print(f"  Killing worker {container.name}...")
        container.stop(timeout=5)

def autoscale():
    global last_scale_time

    lag = get_kafka_lag()
    active = count_workers()

    active_workers_gauge.set(active)
    kafka_lag_gauge.set(lag)

    print(f"Lag: {lag} | Workers: {active}")

    now = time.time()
    if now - last_scale_time < COOLDOWN_SECONDS:
        print(f"  Cooldown active, skipping scale decision")
        return

    if lag > LAG_HIGH_WATERMARK and active < MAX_WORKERS:
        needed = min(lag // TARGET_LAG_PER_WORKER, MAX_WORKERS) - active
        if needed > 0:
            print(f"  Scaling UP — spawning {needed} workers")
            spawn_workers(needed)
            last_scale_time = now

    elif lag < LAG_LOW_WATERMARK and active > MIN_WORKERS:
        to_kill = active - MIN_WORKERS
        print(f"  Scaling DOWN — killing {to_kill} workers")
        kill_workers(to_kill)
        last_scale_time = now

print("Autoscaler starting — metrics on :9090")
start_http_server(9090)

print("Autoscaler running...")
while True:
    try:
        autoscale()
    except Exception as e:
        print(f"Error: {e}")
    time.sleep(POLL_INTERVAL)