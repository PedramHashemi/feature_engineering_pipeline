install:
	pip install -r requirements.txt
lint:
	flake8 src/

run tests:
	pytest tests/

run_pipeline:
	python src/pipeline.py

all: install lint run_pipeline