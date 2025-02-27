
all: clean install test

.PHONY: build

build-docker:
	docker compose build --no-rm --parallel

install:
	pip install -q --config-settings editable_mode=compat --use-pep517 -e .

dev:
	python3 -m pip install --upgrade pip setuptools
	python3 -m pip install -q -r requirements.txt
	python3 -m pip install -q -r requirements-dev.txt

test:
	docker compose run --rm shell bash -c "make install && make test-local"
	@echo "⚠️ you might notice a warning about a fairy from SQLAlchemy"
	@echo "this is fixed in a newer release -- see https://github.com/sqlalchemy/sqlalchemy/issues/10414"
	@echo "we are ignoring this for now"

test-local:
	pytest --cov=servicelayer


lint:
	ruff check .

format:
	black .

format-check:
	black --check .

shell:
	docker compose run --rm shell

build:
	python3 setup.py sdist bdist_wheel

release: clean build
	twine upload dist/*

clean:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
