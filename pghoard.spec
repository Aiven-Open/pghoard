Name:           pghoard
Version:        %{major_version}
Release:        %{minor_version}%{?dist}
Url:            http://github.com/ohmu/pghoard
Summary:        PostgreSQL streaming backup service
License:        ASL 2.0
Source0:        pghoard-rpm-src.tar
Requires(pre):  shadow-utils
Requires:       postgresql-server, systemd
Requires:       python3-boto, python3-cryptography python3-dateutil
Requires:       python3-psycopg2, python3-requests, python3-snappy
BuildRequires:  python3-flake8, python3-pytest, python3-pylint, python3-devel
BuildArch:      noarch

%description
pghoard is a PostgreSQL streaming backup service allowing the compression
and encryption of backup files. Also transfer of backup files to an object
storage is supported.


%prep
%setup -q -n pghoard


%install
python3 setup.py install --prefix=%{_prefix} --root=%{buildroot}
sed -e "s@#!/bin/python@#!%{_bindir}/python@" -i %{buildroot}%{_bindir}/*
%{__install} -Dm0644 pghoard.unit %{buildroot}%{_unitdir}/pghoard.service
%{__mkdir_p} %{buildroot}%{_localstatedir}/lib/pghoard


%check
make test

%files
%defattr(-,root,root,-)
%doc LICENSE README.rst pghoard.json
%{_bindir}/pghoard*
%{_unitdir}/pghoard.service
%{python3_sitelib}/*
%attr(0755, postgres, postgres) %{_localstatedir}/lib/pghoard


%changelog
* Mon Dec 14 2015 Oskari Saarenmaa <os@ohmu.fi> - 0.9.0
- We're Python 3 only now

* Wed Mar 25 2015 Oskari Saarenmaa <os@ohmu.fi> - 0.9.0
- Build a single package using Python 3 if possible, Python 2 otherwise

* Thu Feb 26 2015 Oskari Saarenmaa <os@ohmu.fi> - 0.9.0
- Refactored

* Thu Feb 19 2015 Hannu Valtonen <hannu.valtonen@ohmu.fi> - 0.9.0
- Initial RPM package spec
