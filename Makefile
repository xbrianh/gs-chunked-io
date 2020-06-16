include common.mk

MODULES=gs_chunked_io

test: lint mypy tests

lint:
	flake8 $(MODULES) *.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

tests:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run --source=gs_chunked_io \
		-m unittest discover --start-directory tests --top-level-directory . --verbose

version: gs_chunked_io/version.py

gs_chunked_io/version.py: setup.py
	echo "__version__ = '$$(python setup.py --version)'" > $@

clean:
	git clean -dfx

build: clean version
	python setup.py bdist_wheel

install: build
	pip install --upgrade dist/*.whl

.PHONY: test lint mypy tests gs_chunked_io/version.py clean build install
