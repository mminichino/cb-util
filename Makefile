.PHONY:	setup push pypi patch minor major test_sync_drv test_async_drv test_cbc_cli test_random test_sgw_cli
export PYTHONPATH := $(shell pwd)/tests:$(shell pwd):$(PYTHONPATH)

build:
		bumpversion build
		git push
patch:
		bumpversion patch
		git push
minor:
		bumpversion minor
		git push
major:
		bumpversion major
		git push
setup:
		python setup.py sdist
push:
		$(eval REV_FILE := $(shell ls -tr dist/*.gz | tail -1))
		twine upload $(REV_FILE)
pypi: setup push
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
test_capella:
		python -m pytest tests/test_6.py
test:
		python -m pytest \
		tests/test_1.py \
		tests/test_2.py \
		tests/test_3.py \
		tests/test_4.py \
		tests/test_5.py \
		tests/test_6.py
