IMAGE_NAME         ?= lineage-service
TABLE_IMAGE_NAME   ?= table-service
IMAGE_TAG          ?= latest

.PHONY: generate build vendor test coverage lint clean \
        docker-build docker-run \
        proto-export rust-build rust-test \
        docker-build-table docker-compose-up docker-compose-down \
        spark-plugin-build spark-plugin-test spark-plugin-clean

generate:
	buf generate

vendor:
	go mod tidy
	go mod vendor

build: vendor
	go build -mod=vendor ./...

test:
	go test ./... -v -coverprofile=coverage.out

coverage: test
	go tool cover -func=coverage.out

lint:
	buf lint

# Export all protos (with resolved buf deps) into the Rust project so
# connectrpc-build can compile them without needing buf at build time.
proto-export:
	rm -rf open-lakehouse-table-service/proto-export
	buf export --output=open-lakehouse-table-service/proto-export

rust-build: proto-export
	cd open-lakehouse-table-service && cargo build

rust-test: proto-export
	cd open-lakehouse-table-service && cargo test

docker-build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

docker-build-table:
	docker build -t $(TABLE_IMAGE_NAME):$(IMAGE_TAG) -f open-lakehouse-table-service/Dockerfile .

docker-run: docker-build
	docker run --rm -p 8090:8090 $(IMAGE_NAME):$(IMAGE_TAG)

docker-compose-up:
	docker compose up --build -d

docker-compose-down:
	docker compose down -v

# Spark plugin (Scala 2.13 / Spark 4.x / JDK 17). Requires sbt on PATH.
spark-plugin-build:
	cd spark-openlineage-plugin && sbt assembly

spark-plugin-test:
	cd spark-openlineage-plugin && sbt test

spark-plugin-clean:
	cd spark-openlineage-plugin && sbt clean

clean:
	rm -rf gen coverage.out open-lakehouse-table-service/proto-export
	cd spark-openlineage-plugin && sbt clean 2>/dev/null || true
