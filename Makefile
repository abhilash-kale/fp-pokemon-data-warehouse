.PHONY: build extract load transform analyze run shell clean

# ==========================================
# CONFIGURATION
# ==========================================
IMAGE_NAME = fp-pokemon-pipeline
DOCKER_RUN = docker run --rm -v $(PWD)/data:/app/data $(IMAGE_NAME)

# ==========================================
# PIPELINE STAGES
# ==========================================
build:
	@echo "Building the Docker image..."
	docker build -t $(IMAGE_NAME) .

extract:
	@echo "Running API Extraction (API -> Data Lake)..."
	$(DOCKER_RUN) python -m src.extract

load:
	@echo "Running DuckDB Load (Data Lake -> Bronze)..."
	$(DOCKER_RUN) python -m src.load

transform:
	@echo "Running dbt transformations and data quality checks (Silver & Gold)..."
	$(DOCKER_RUN) bash -c "cd dbt_pokemon && dbt deps && dbt build"

analyze:
	@echo "Generating Analytics Report..."
	$(DOCKER_RUN) python -m src.analyze

# ==========================================
# ORCHESTRATION & UTILITIES
# ==========================================
run: build extract load transform analyze
	@echo "End-to-End Pipeline Completed Successfully!"

shell:
	@echo "Opening an interactive shell inside the container..."
	docker run -it --rm -v $(PWD)/data:/app/data $(IMAGE_NAME) bash

clean:
	@echo "Cleaning up local data and DuckDB files..."
	rm -rf data/*
	mkdir -p data/raw/pokemon data/raw/types data/raw/abilities data/reports
