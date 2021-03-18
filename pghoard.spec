Name:           pghoard
Version:        %{major_version}
Release:        %{minor_version}%{?dist}
Url:            http://github.com/aiven/pghoard
Summary:        PostgreSQL streaming backup service
License:        ASL 2.0
Source0:        pghoard-rpm-src.tar
Requires:       systemd
Requires:       python3-botocore, python3-cryptography >= 0.8, python3-dateutil
Requires:       python3-psycopg2, python3-requests, python3-snappy, python3-zstandard, python3-pydantic,
Conflicts:      pgespresso92 < 1.2, pgespresso93 < 1.2, pgespresso94 < 1.2, pgespresso95 < 1.2
BuildRequires:  python3-flake8, python3-pytest, python3-pylint, python3-devel, golang

%undefine _missing_build_ids_terminate_build

%description
PGHoard is a PostgreSQL streaming backup service.  Backups are stored in
encrypted and compressed format in a cloud object storage.  PGHoard
currently supports Amazon Web Services S3, Google Cloud Storage, OpenStack
Swift and Ceph (using S3 or Swift interfaces with RadosGW.)
Support for Microsoft Azure is experimental.


%{?python_disable_dependency_generator}


%prep
%setup -q -n pghoard


%build
go build golang/pghoard_postgres_command_go.go


%install
sed -e s,pghoard_postgres_command,pghoard_postgres_command_go,g -i pghoard/restore.py
python3 setup.py install --prefix=%{_prefix} --root=%{buildroot}
sed -e "s@#!/bin/python@#!%{_bindir}/python@" -i %{buildroot}%{_bindir}/*
%{__install} -Dm0644 pghoard.unit %{buildroot}%{_unitdir}/pghoard.service
cp -a pghoard_postgres_command_go %{buildroot}%{_bindir}


%check
make test

%files
%defattr(-,root,root,-)
%doc LICENSE README.rst pghoard.json
%{_bindir}/pghoard*
%{_unitdir}/pghoard.service
%{python3_sitelib}/*


%changelog
* Wed Feb 11 2020 Tapio Oikarinen <tapio@aiven.io> - 2.1.1
- Security fix for gnutaremu

* Tue Sep 5 2017 Oskari Saarenmaa <os@aiven.io> - 1.4.0
- Add pghoard_postgres_command_go

* Tue Jul 26 2016 Oskari Saarenmaa <os@ohmu.fi> - 1.4.0
- Conflict with pgespresso < 1.2: older versions crash PostgreSQL
  when tablespaces are used

* Mon Dec 14 2015 Oskari Saarenmaa <os@ohmu.fi> - 0.9.0
- We're Python 3 only now

* Wed Mar 25 2015 Oskari Saarenmaa <os@ohmu.fi> - 0.9.0
- Build a single package using Python 3 if possible, Python 2 otherwise

* Thu Feb 26 2015 Oskari Saarenmaa <os@ohmu.fi> - 0.9.0
- Refactored

* Thu Feb 19 2015 Hannu Valtonen <hannu.valtonen@ohmu.fi> - 0.9.0
- Initial RPM package spec
