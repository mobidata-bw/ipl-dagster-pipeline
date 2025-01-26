MAIN_MODULE=./pipeline ./tests

.PHONY: lint-fix
lint-fix:
	ruff format ${MAIN_MODULE}
	ruff check --fix ${MAIN_MODULE}
	# mypy has no fix mode, we run it anyway to report (unfixable) errors
	mypy ${MAIN_MODULE}

.PHONY: lint-check
lint-check:
	$(PROXY_RUN) ruff ${MAIN_MODULE}
	mypy ${MAIN_MODULE}
