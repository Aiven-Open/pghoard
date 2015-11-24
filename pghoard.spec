%if %{?python3_sitelib:1}0
%global use_python3 1
%else
%global use_python3 0
%endif

Name:           pghoard
Version:        %{major_version}
Release:        %{minor_version}%{?dist}
Url:            http://github.com/ohmu/pghoard
Summary:        PostgreSQL streaming backup service
License:        ASL 2.0
Source0:        pghoard-rpm-src.tar.gz
Requires(pre):  shadow-utils
Requires:       postgresql-server, systemd
BuildRequires:  %{requires}
%if %{use_python3}
Requires:       python3-boto, python3-cryptography python3-dateutil
Requires:       python3-psycopg2, python3-requests, python3-snappy
BuildRequires:  python3-pep8, python3-pytest, python3-pylint, %{requires}
%else
Requires:       python-boto, python-cryptography, python-dateutil
Requires:       python-psycopg2, python-requests, python-snappy
BuildRequires:  pylint, pytest, python-pep8, %{requires}
%endif
BuildArch:      noarch

%description
pghoard is a PostgreSQL streaming backup service allowing the compression
and encryption of backup files. Also transfer of backup files to an object
storage is supported.


%prep
%setup -q -n pghoard


%install
%if %{use_python3}
python3 setup.py install --prefix=%{_prefix} --root=%{buildroot}
%else
python2 setup.py install --prefix=%{_prefix} --root=%{buildroot}
%endif
sed -e "s@#!/bin/python@#!%{_bindir}/python@" -i %{buildroot}%{_bindir}/*
%{__install} -Dm0644 pghoard.unit %{buildroot}%{_unitdir}/pghoard.service
%{__mkdir_p} %{buildroot}%{_localstatedir}/lib/pghoard


%check
%if %{use_python3}
make test PYTHON=python3
%else
make test PYTHON=python2
%endif

%files
%defattr(-,root,root,-)
%doc LICENSE README.rst pghoard.json
%{_bindir}/pghoard*
%{_unitdir}/pghoard.service
%if %{use_python3}
%{python3_sitelib}/*
%else
%{python_sitelib}/*
%endif
%attr(0755, postgres, postgres) %{_localstatedir}/lib/pghoard


%changelog
* Wed Mar 25 2015 Oskari Saarenmaa <os@ohmu.fi> - 0.9.0
- Build a single package using Python 3 if possible, Python 2 otherwise

* Thu Feb 26 2015 Oskari Saarenmaa <os@ohmu.fi> - 0.9.0
- Refactored

* Thu Feb 19 2015 Hannu Valtonen <hannu.valtonen@ohmu.fi> - 0.9.0
- Initial RPM package spec
