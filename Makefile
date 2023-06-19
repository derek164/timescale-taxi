build:
	@docker build --no-cache -t pyspark-sedona .

start:
	@docker run --rm -it --user 0 -v ${PWD}:/app --entrypoint bash pyspark-sedona

run:
	@python3 taxi/main.py

preview:
	@python3 taxi/preview.py

format:
	@isort .
	@black .
