Name:           pghoard
Version:        %{major_version}
Release:        %{minor_version}%{?dist}
Url:            http://github.com/ohmu/pghoard
Summary:        PostgreSQL streaming backup service
License:        ASL 2.0
Source0:        pghoard-rpm-src.tar.gz
Requires:       python-argh, python-dateutil, python-psycopg2, python-requests
Requires:       postgresql-server, systemd
Requires(pre):  shadow-utils
BuildRequires:  pylint, pytest, %{requires}
BuildArch:      noarch

%description
pghoard is a PostgreSQL streaming backup service allowing the compression
and encryption of backup files. Also transfer of backup files to an object
storage is supported.


%prep
%setup -q -n pghoard


%install
%{__mkdir_p} %{buildroot}%{_localstatedir}/lib/pghoard %{buildroot}%{_datadir}/pghoard
python2 setup.py install --prefix=%{_prefix} --root=%{buildroot}
sed -e 's@/usr/bin/@%{_bindir}/@g' \
    -e 's@/var/@%{_localstatedir}/@g' \
    -i pghoard.unit
%{__install} -Dm0644 pghoard.unit %{buildroot}%{_unitdir}/pghoard.service


%check
make test


%pre
getent passwd pghoard >/dev/null || \
    useradd -r -g postgres -d %{_localstatedir}/lib/pghoard -s /usr/bin/sh \
	    -c "pghoard account" pghoard


%files
%defattr(-,root,root,-)
%doc LICENSE README.rst pghoard.json
%{_unitdir}/pghoard.service
%{_bindir}/pghoard*
%{python_sitelib}/*
%attr(0755, pghoard, postgres) %{_localstatedir}/lib/pghoard


%changelog
* Thu Feb 26 2015 Oskari Saarenmaa <os@ohmu.fi> - 0.9.0
- Refactored

* Thu Feb 19 2015 Hannu Valtonen <hannu.valtonen@ohmu.fi> - 0.9.0
- Initial RPM package spec
