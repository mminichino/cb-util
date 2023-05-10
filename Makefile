.PHONY:	setup push all

setup:
		python setup.py sdist
push:
		$(eval REV_FILE := $(shell ls dist/*.gz | tail -1))
		twine upload $(REV_FILE)
all: setup push
