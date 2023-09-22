.PHONY:	setup push all version test_sync_drv test_async_drv test_cbc_cli test_random test_sgw_cli
export PYTHONPATH := $(shell pwd)/tests:$(shell pwd):$(PYTHONPATH)

version:
		bumpversion patch
setup:
		python setup.py sdist
push:
		$(eval REV_FILE := $(shell ls dist/*.gz | tail -1))
		twine upload $(REV_FILE)
all: setup push
test_sync_drv:
		python -m pytest tests/test_1.py
test_async_drv:
		python -m pytest tests/test_2.py
test_cbc_cli:
		python -m pytest tests/test_3.py
test_random:
		python -m pytest tests/test_4.py
test_sgw_cli:
		python -m pytest tests/test_5.py
test:
		python -m pytest tests/test_1.py tests/test_2.py tests/test_3.py tests/test_4.py tests/test_5.py
