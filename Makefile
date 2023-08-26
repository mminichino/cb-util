.PHONY:	setup push all

version:
		bumpversion patch
setup:
		python setup.py sdist
push:
		$(eval REV_FILE := $(shell ls dist/*.gz | tail -1))
		twine upload $(REV_FILE)
all: setup push
