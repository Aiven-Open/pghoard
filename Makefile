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
	sudo yum -y install python-argh python3-argh python-devel python-nose python-psycopg2 python3-psycopg2 python-mock python-boto python3-boto

test: pep8 pylint unittest

unittest:
	$(PYTHON) -m pytest $(PYTEST_ARG) test/

pylint:
	$(PYTHON) -m pylint.lint --rcfile .pylintrc $(PYTHON_SOURCE_DIRS)

pep8:
	$(PYTHON) -m pep8 --ignore=E501,E123 $(PYTHON_SOURCE_DIRS)
