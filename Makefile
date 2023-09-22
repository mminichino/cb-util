.PHONY:	setup push all version test_cli
export PYTHONPATH := $(shell pwd)/tests:$(shell pwd):$(PYTHONPATH)

version:
		bumpversion patch
setup:
		python setup.py sdist
push:
		$(eval REV_FILE := $(shell ls dist/*.gz | tail -1))
		twine upload $(REV_FILE)
all: setup push
test_cli:
		python -m pytest tests/test_3.py
