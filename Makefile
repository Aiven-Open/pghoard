short_ver = $(shell git describe --abbrev=0)
long_ver = $(shell git describe --long 2>/dev/null || echo $(short_ver)-0-unknown-g`git describe --always`)
generated = pghoard/version.py

all: $(generated)

PYTHON ?= python3
PYTHON_SOURCE_DIRS = pghoard/ test/
PYTEST_ARG ?= -v

.PHONY: unittest
unittest: version
	$(PYTHON) -m pytest -vv test/

.PHONY: lint
lint: version
	$(PYTHON) -m pylint --rcfile .pylintrc $(PYTHON_SOURCE_DIRS)

.PHONY: fmt
fmt: version
	unify --quote '"' --recursive --in-place $(PYTHON_SOURCE_DIRS)
	isort --recursive $(PYTHON_SOURCE_DIRS)
	yapf --parallel --recursive --in-place $(PYTHON_SOURCE_DIRS)

.PHONY: coverage
coverage: version
	$(PYTHON) -m pytest $(PYTEST_ARG) --cov-report term-missing --cov pghoard test/

.PHONY: clean
clean:
	$(RM) -r *.egg-info/ build/ dist/ rpm/
	$(RM) ../pghoard_* test-*.xml $(generated)

pghoard/version.py: version.py
	$(PYTHON) $^ $@

.PHONY: version
version: pghoard/version.py

.PHONY: deb
deb: $(generated)
	cp debian/changelog.in debian/changelog
	dch -v $(long_ver) --distribution unstable "Automatically built .deb"
	dpkg-buildpackage -A -uc -us

.PHONY: rpm
rpm: $(generated)
	git archive --output=pghoard-rpm-src.tar --prefix=pghoard/ HEAD
	# add generated files to the tar, they're not in git repository
	tar -r -f pghoard-rpm-src.tar --transform=s,pghoard/,pghoard/pghoard/, $(generated)
	rpmbuild -bb pghoard.spec \
		--define '_topdir $(PWD)/rpm' \
		--define '_sourcedir $(CURDIR)' \
		--define 'major_version $(short_ver)' \
		--define 'minor_version $(subst -,.,$(subst $(short_ver)-,,$(long_ver)))'
	$(RM) pghoard-rpm-src.tar

.PHONY: build-dep-fed
build-dep-fed:
	sudo dnf -y install --best --allowerasing \
		golang \
		postgresql-server \
		python3-botocore python3-cryptography python3-paramiko python3-dateutil python3-devel \
		python3-flake8 python3-psycopg2 python3-pylint python3-pytest python3-execnet python3-pytest-mock \
		python3-pytest-cov python3-requests python3-snappy \
		python3-azure-sdk \
		rpm-build
