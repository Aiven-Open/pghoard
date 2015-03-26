short_ver = 0.9.0
long_ver = $(shell git describe --long 2>/dev/null || echo $(short_ver)-0-unknown-g`git describe --always`)

all: py-egg

PYTHON ?= python
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
		--define '_sourcedir $(shell pwd)' \
		--define 'major_version $(short_ver)' \
		--define 'minor_version $(subst -,.,$(subst $(short_ver)-,,$(long_ver)))'
	$(RM) pghoard-rpm-src.tar.gz

build-dep-fed:
	sudo yum -y install postgresql-server python-autopep8 \
		python3-argh python3-boto python3-dateutil python3-pep8 \
		python3-psycopg2 python3-pylint python3-pytest \
		python3-pytest-cov python3-requests \
		python-argh python-backports-lzma python-boto python-dateutil python-pep8 \
		python-psycopg2 pylint pytest python-mock \
		python-pytest-cov python-requests rpm-build

test: pep8 pylint unittest

unittest:
	$(PYTHON) -m pytest $(PYTEST_ARG) test/

coverage:
	$(PYTHON) -m pytest $(PYTEST_ARG) --cov-report term-missing --cov pghoard test/

pylint:
	$(PYTHON) -m pylint.lint --rcfile .pylintrc $(PYTHON_SOURCE_DIRS)

pep8:
	$(PYTHON) -m pep8 --ignore=E501,E123 $(PYTHON_SOURCE_DIRS)

autopep8:
	$(PYTHON) -m autopep8 --recursive --jobs=0 --in-place --max-line-length=100 --ignore E24,E265 $(PYTHON_SOURCE_DIRS)
