IMAGE_NAME         ?= lineage-service
TABLE_IMAGE_NAME   ?= table-service
IMAGE_TAG          ?= latest

TF_DIR := terraform/ecs-lineage

.PHONY: generate build vendor test coverage lint clean \
        docker-build docker-run \
        proto-export rust-build rust-test \
        docker-build-table docker-compose-up docker-compose-down \
        spark-plugin-build spark-plugin-test spark-plugin-clean spark-connect-e2e \
        ecr-push-lineage ecr-push-table deploy-ecs-lineage \
        ecs-lineage-outputs ecs-lineage-destroy

generate:
	buf generate

vendor:
	cd services/lineage && go mod tidy
	cd services/lineage && go mod vendor

build: vendor
	cd services/lineage && go build -mod=vendor ./...

test:
	cd services/lineage && go test ./... -v -coverprofile=coverage.out

coverage: test
	cd services/lineage && go tool cover -func=coverage.out

lint:
	buf lint

# Export all protos (with resolved buf deps) into the Rust project so
# connectrpc-build can compile them without needing buf at build time.
proto-export:
	rm -rf crates/table-service/proto-export
	buf export --output=crates/table-service/proto-export

rust-build: proto-export
	cargo build -p table-service

rust-test: proto-export
	cargo test -p table-service

docker-build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) services/lineage

docker-build-table:
	docker build -t $(TABLE_IMAGE_NAME):$(IMAGE_TAG) -f crates/table-service/Dockerfile .

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

# True Spark Connect server end-to-end: boots a real Connect server with the
# plugin and drives it from a remote session. Heavier than `spark-plugin-test`;
# requires java 17, python3, sbt, and $SPARK_HOME (or network to download Spark).
spark-connect-e2e:
	./scripts/spark-connect-e2e.sh

# ----- ECS / Terraform deployment (terraform/ecs-lineage) --------------------
# Build + push images to ECR (linux/arm64). Tags come from .env
# (LINEAGE_IMAGE_TAG / TABLE_IMAGE_TAG) unless overridden on the CLI.
ecr-push-lineage:
	./scripts/ecr-push-lineage.sh $(LINEAGE_IMAGE_TAG)

ecr-push-table:
	./scripts/ecr-push-table.sh $(TABLE_IMAGE_TAG)

# Full deploy: build + push both images, then terraform apply.
deploy-ecs-lineage:
	./scripts/deploy-ecs-lineage.sh

ecs-lineage-outputs:
	terraform -chdir=$(TF_DIR) output

ecs-lineage-destroy:
	terraform -chdir=$(TF_DIR) destroy

clean:
	rm -rf services/lineage/gen services/lineage/coverage.out crates/table-service/proto-export
	cd spark-openlineage-plugin && sbt clean 2>/dev/null || true
