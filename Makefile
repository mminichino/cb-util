.PHONY:	push

push:
		python setup.py sdist
		sleep 1
		$(eval REV_FILE := $(shell ls dist/*.gz | tail -1))
		twine upload $(REV_FILE)
