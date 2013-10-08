.PHONY: all test pylint flake8

PACKAGE=whip

all: test pylint flake8

test:
	nosetests --verbose --with-coverage --cover-erase --cover-package=${PACKAGE} --cover-html --cover-html-dir coverage/

pylint:
	-pylint \
		--report=no \
		--msg-template='{path}:{line}:{column} [{msg_id}/{symbol}] {msg}' \
		--disable=bad-builtin \
		--disable=fixme \
		--disable=invalid-name \
		--disable=locally-disabled \
		--disable=star-args \
		--disable=too-few-public-methods \
		--disable=too-many-return-statements \
		${PACKAGE}

flake8:
	-flake8 ${PACKAGE}
