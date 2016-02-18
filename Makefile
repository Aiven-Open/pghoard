short_ver = 0.9.0
long_ver = $(shell git describe --long 2>/dev/null || echo $(short_ver)-0-unknown-g`git describe --always`)

all: py-egg

PYTHON ?= python3
PYTHON_SOURCE_DIRS = pghoard/ test/
PYTEST_ARG ?= -v

clean:
	$(RM) -r *.egg-info/ build/ dist/
	$(RM) ../pghoard_* test-*.xml

deb:
	cp debian/changelog.in debian/changelog
	dpkg-buildpackage -A -uc -us

rpm:
	git archive --output=pghoard-rpm-src.tar.gz --prefix=pghoard/ HEAD
	rpmbuild -bb pghoard.spec \
		--define '_topdir $(PWD)/rpm' \
		--define '_sourcedir $(CURDIR)' \
		--define 'major_version $(short_ver)' \
		--define 'minor_version $(subst -,.,$(subst $(short_ver)-,,$(long_ver)))'
	$(RM) pghoard-rpm-src.tar.gz

build-dep-fed:
	sudo dnf -y install postgresql-server \
		python3-boto python3-cryptography python3-dateutil \
		python3-flake8 python3-psycopg2 python3-pylint python3-pytest \
		python3-pytest-cov python3-requests python3-snappy \
		rpm-build

test: flake8 pylint unittest

unittest:
	$(PYTHON) -m pytest $(PYTEST_ARG) test/

coverage:
	$(PYTHON) -m coverage run --source pghoard -m pytest $(PYTEST_ARG) test/
	$(PYTHON) -m coverage report --show-missing

pylint:
	$(PYTHON) -m pylint.lint --rcfile .pylintrc $(PYTHON_SOURCE_DIRS)

flake8:
	$(PYTHON) -m flake8 --max-line-len=125 $(PYTHON_SOURCE_DIRS)

.PHONY: rpm
