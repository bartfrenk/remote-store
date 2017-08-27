.PHONY: doc help clean
.DEFAULT_GOAL := help

help:
	@echo "Makefile for RemoteStore Python package\n"
	@fgrep -h "##" $(MAKEFILE_LIST) | \
	fgrep -v fgrep | sed -e 's/## */##/' | column -t -s##

doc: ## Build documentation
doc: doc/build/index.html


clean: ## Remove all generated files
	@rm -rf doc/build

doc/build/index.html: doc/source/*
	sphinx-build -b html doc/source doc/build
