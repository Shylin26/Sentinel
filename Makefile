# SENTINEL — single command setup
# Usage:
#   make infra     start Kafka + Redis + Prometheus + Grafana
#   make topics    create Kafka topics
#   make run       start all 5 services (background)
#   make stop      kill all services
#   make test      run chaos test suite
#   make load      run 60s load test (50 users)
#   make logs      tail all service logs
#   make status    show what's running

PYTHON = .venv/bin/python3
SENTINEL = sentinel

.PHONY: infra topics run stop test load logs status

infra:
	cd $(SENTINEL)/infra && docker compose up -d
	@echo "Waiting for Kafka to be ready..."
	@sleep 10
	@echo "Infra ready."

topics:
	docker exec infra-kafka-1 kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic push-events --partitions 12 --replication-factor 1
	docker exec infra-kafka-1 kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic diff-chunks --partitions 12 --replication-factor 1
	docker exec infra-kafka-1 kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic reviews --partitions 12 --replication-factor 1
	docker exec infra-kafka-1 kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic reviews-dlq --partitions 12 --replication-factor 1
	@echo "Topics ready."

run: stop
	@echo "Starting webhook..."
	cd $(SENTINEL) && $(PYTHON) -m uvicorn services.webhook.main:app --port 8000 > /tmp/sentinel-webhook.log 2>&1 &
	@echo "Starting diff extractor..."
	cd $(SENTINEL) && $(PYTHON) services/kafka/diff_extractor.py > /tmp/sentinel-extractor.log 2>&1 &
	@echo "Starting inference server..."
	cd $(SENTINEL) && $(PYTHON) services/inference/server.py > /tmp/sentinel-inference.log 2>&1 &
	@echo "Starting result publisher..."
	cd $(SENTINEL) && $(PYTHON) services/publisher/result_publisher.py > /tmp/sentinel-publisher.log 2>&1 &
	@echo "Starting autoscaler..."
	cd $(SENTINEL) && $(PYTHON) services/autoscaler/autoscaler.py > /tmp/sentinel-autoscaler.log 2>&1 &
	@sleep 3
	@echo ""
	@echo "All services started. Logs in /tmp/sentinel-*.log"
	@echo "Dashboard: http://localhost:3000"
	@echo "Metrics:   http://localhost:8000/metrics"

stop:
	@echo "Stopping all SENTINEL services..."
	@pkill -f "uvicorn services.webhook" 2>/dev/null || true
	@pkill -f "diff_extractor.py" 2>/dev/null || true
	@pkill -f "inference/server.py" 2>/dev/null || true
	@pkill -f "result_publisher.py" 2>/dev/null || true
	@pkill -f "autoscaler.py" 2>/dev/null || true
	@sleep 1
	@echo "Done."

test:
	cd $(SENTINEL) && $(PYTHON) tests/chaos_test.py

load:
	cd $(SENTINEL) && ../.venv/bin/locust -f tests/locustfile.py --headless -u 50 -r 5 --run-time 60s --host http://127.0.0.1:8000

logs:
	@tail -f /tmp/sentinel-webhook.log /tmp/sentinel-extractor.log /tmp/sentinel-inference.log /tmp/sentinel-publisher.log /tmp/sentinel-autoscaler.log

status:
	@echo "=== SENTINEL service status ==="
	@pgrep -f "uvicorn services.webhook" > /dev/null && echo "webhook     UP" || echo "webhook     DOWN"
	@pgrep -f "diff_extractor.py"        > /dev/null && echo "extractor   UP" || echo "extractor   DOWN"
	@pgrep -f "inference/server.py"      > /dev/null && echo "inference   UP" || echo "inference   DOWN"
	@pgrep -f "result_publisher.py"      > /dev/null && echo "publisher   UP" || echo "publisher   DOWN"
	@pgrep -f "autoscaler.py"            > /dev/null && echo "autoscaler  UP" || echo "autoscaler  DOWN"
	@echo ""
	@docker ps --format "  {{.Names}}: {{.Status}}" | grep infra || echo "  infra: not running"
