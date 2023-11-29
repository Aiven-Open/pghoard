short_ver = $(shell git describe --abbrev=0)
long_ver = $(shell git describe --long 2>/dev/null || echo $(short_ver)-0-unknown-g`git describe --always`)

PYTHON ?= python3
PYTHON_SOURCE_DIRS = pghoard/ test/
PYTEST_ARG ?= -v

.PHONY: pghoard/version.py
python-build:
	$(PYTHON) -m build

.PHONY: dev-deps
dev-deps:
	pip install .
	pip install ".[dev]"

.PHONY: unittest
unittest: dev-deps
	$(PYTHON) -m pytest -vv test/

.PHONY: lint
lint: dev-deps
	$(PYTHON) -m pylint --rcfile .pylintrc $(PYTHON_SOURCE_DIRS)

.PHONY: mypy
mypy: dev-deps
	$(PYTHON) -m mypy $(PYTHON_SOURCE_DIRS)

.PHONY: fmt
fmt: dev-deps
	unify --quote '"' --recursive --in-place $(PYTHON_SOURCE_DIRS)
	isort $(PYTHON_SOURCE_DIRS)
	yapf --parallel --recursive --in-place $(PYTHON_SOURCE_DIRS)

.PHONY: coverage
coverage: dev-deps
	$(PYTHON) -m pytest $(PYTEST_ARG) --cov-report term-missing --cov-report xml:coverage.xml \
		--cov pghoard test/

.PHONY: clean
clean:
	$(RM) -r *.egg-info/ build/ dist/ rpm/
	$(RM) ../pghoard_* test-*.xml coverage.xml pghoard/version.py


.PHONY: deb
deb:
	cp debian/changelog.in debian/changelog
	dch -v $(long_ver) --distribution unstable "Automatically built .deb"
	dpkg-buildpackage -A -uc -us

.PHONY: rpm
rpm: python-build
	git archive --output=pghoard-rpm-src.tar --prefix=pghoard/ HEAD
	# add generated files to the tar, they're not in git repository
	tar -r -f pghoard-rpm-src.tar --transform=s,pghoard/,pghoard/pghoard/, pghoard/version.py
	rpmbuild -bb pghoard.spec \
		--define '_topdir $(PWD)/rpm' \
		--define '_sourcedir $(CURDIR)' \
		--define 'major_version $(short_ver)' \
		--define 'minor_version $(subst -,.,$(subst $(short_ver)-,,$(long_ver)))'
	$(RM) pghoard-rpm-src.tar

.PHONY: build-dep-fed
build-dep-fed:
	sudo dnf -y install 'dnf-command(builddep)' tar rpm-build
	sudo dnf -y builddep pghoard.spec
