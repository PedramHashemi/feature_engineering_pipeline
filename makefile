install:
	pip install -r requirements.txt
lint:
	flake8 src/

run tests:
	pytest tests/

run_ingestion:
	python pipeline/ingestion.py

run_feature_engineering:
	python pipeline/feature_engineering.py

run_model_training:
	python pipeline/model_training.py

all: install lint run_ingestion run_feature_engineering run_model_training