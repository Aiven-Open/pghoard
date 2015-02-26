Name:           pghoard
Version:        %{major_version}
Release:        %{minor_version}%{?dist}
Url:            http://github.com/ohmu/pghoard
Summary:        PostgreSQL streaming backup service
License:        ASL 2.0
Source0:        pghoard-rpm-src.tar.gz
Requires:       python-argh, python-dateutil, python-psycopg2, python-requests
Requires(pre):  shadow-utils
BuildRequires:  pylint, pytest, python-pytest-cov, systemd, %{requires}
BuildArch:      noarch

%description
pghoard is a PostgreSQL streaming backup service allowing the compression
and encryption of backup files. Also transfer of backup files to an object
storage is supported.

%pre
mkdir -p /var/lib/pghoard
getent group postgres >/dev/null || groupadd -r postgres
getent passwd pghoard >/dev/null || \
    useradd -r -g pghoard -d /var/lib/pghoard -s /usr/bin/sh \
	    -c "pghoard account" pghoard
chown pghoard.postgres /var/lib/pghoard
exit 0

%prep
%setup -q -n pghoard

%build
python setup.py build

%install
python setup.py install -O1 --skip-build --prefix=%{_prefix} --root=%{buildroot}
%{__install} -Dm0644 pghoard.unit %{buildroot}%{_unitdir}/pghoard.service

%check
make test

%files
%defattr(-,root,root,-)
%doc LICENSE README.rst pghoard.json
%{_unitdir}/pghoard.service
%{_bindir}/pghoard*
%{python_sitelib}/*

%changelog
* Thu Feb 19 2015 Hannu Valtonen <hannu.valtonen@ohmu.fi> - 0.9.0
- Initial RPM package spec
