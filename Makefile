.PHONY: build test lint bench up down docker-build

SERVICES := validator enricher aggregator raw-store-writer loadgen dlq-replayer

build:
	@for svc in $(SERVICES); do \
		go build -o bin/$$svc ./cmd/$$svc; \
	done

test:
	go test ./... -v -race -count=1

lint:
	golangci-lint run ./...

bench:
	./bin/loadgen --mode=steady --rate=100000 --duration=60s --report-dir=./benchmarks

docker-build:
	@for svc in $(SERVICES); do \
		docker build -f deploy/docker/Dockerfile.$$svc -t distributed-collector-$$svc:latest .; \
	done

up:
	docker compose -f deploy/docker/docker-compose.yaml up -d

down:
	docker compose -f deploy/docker/docker-compose.yaml down -v

k8s-deploy:
	kubectl apply -f deploy/k8s/namespace.yaml
	kubectl apply -f deploy/k8s/ --recursive
