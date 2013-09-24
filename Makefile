.PHONY: test pylint

all: test pylint

test:
	nosetests --verbose --with-coverage

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
		whip
