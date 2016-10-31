short_ver = 1.4.0
long_ver = $(shell git describe --long 2>/dev/null || echo $(short_ver)-0-unknown-g`git describe --always`)
generated = pghoard/version.py

all: $(generated)

PYTHON ?= python3
PYTHON_SOURCE_DIRS = pghoard/ test/
PYTEST_ARG ?= -v

clean:
	$(RM) -r *.egg-info/ build/ dist/
	$(RM) ../pghoard_* test-*.xml $(generated)

pghoard/version.py: version.py
	$(PYTHON) $^ $@

deb: $(generated)
	cp debian/changelog.in debian/changelog
	dch -v $(long_ver) --distribution unstable "Automatically built .deb"
	dpkg-buildpackage -A -uc -us

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

build-dep-fed:
	sudo dnf -y install postgresql-server \
		python3-boto python3-cryptography python3-dateutil python3-devel \
		python3-flake8 python3-psycopg2 python3-pylint python3-pytest \
		python3-pytest-cov python3-requests python3-snappy \
		rpm-build

test: flake8 pylint unittest

unittest: $(generated)
	$(PYTHON) -m pytest $(PYTEST_ARG) test/

coverage: $(generated)
	$(PYTHON) -m coverage run --source pghoard -m pytest $(PYTEST_ARG) test/
	$(PYTHON) -m coverage report --show-missing

pylint: $(generated)
	$(PYTHON) -m pylint.lint --rcfile .pylintrc $(PYTHON_SOURCE_DIRS)

flake8: $(generated)
	$(PYTHON) -m flake8 --max-line-len=125 $(PYTHON_SOURCE_DIRS)

.PHONY: rpm
