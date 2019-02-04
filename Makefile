
all: clean install test

install:
	pip install -q -e .
	pip install -q twine coverage nose

test:
	pytest --cov=servicelayer

build:
	python setup.py sdist bdist_wheel

generate: install
	python -m grpc_tools.protoc -Iprotos --python_out=. --grpc_python_out=. ./protos/servicelayer/rpc/*.proto

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
