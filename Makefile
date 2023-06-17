watch: venv/touchfile

venv/touchfile: requirements.txt
	@test -d venv
	@. venv/bin/activate; pip install --upgrade pip; pip install -Ur requirements.txt
	@touch venv/touchfile

freeze:
	@pip freeze > requirements.txt

env:
	@python3 -m venv venv
	@. venv/bin/activate; pip install --upgrade pip; pip install -Ur requirements.txt

install: env

run:
	@. venv/bin/activate; python3 taxi/main.py

format:
	@. venv/bin/activate; isort .
	@. venv/bin/activate; black .
