%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}

Name:           pghoard
Version:        %{major_version}
Release:        %{minor_version}%{?dist}
Url:            http://github.com/ohmu/pghoard
Summary:        PostgreSQL replication monitoring and failover daemon
License:        Apache V2
Source0:        pghoard-rpm-src.tar.gz
Source1:        pghoard.unit
BuildRoot:      %{_tmppath}/%{name}-%{version}-build
BuildRequires:  python-devel
BuildRequires:  python-distribute
BuildRequires:  python-nose
Requires:       python-psycopg2, python-requests, python-setuptools
Requires(pre):  shadow-utils
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
%{__mkdir_p} ${RPM_BUILD_ROOT}/usr/lib/systemd/system
%{__install} -m0644 %{SOURCE1} ${RPM_BUILD_ROOT}/usr/lib/systemd/system/pghoard.service

%check
python setup.py test

%files
/usr/lib/systemd/system/*

%defattr(-,root,root,-)

%doc LICENSE README.rst pghoard.json
%{_bindir}/pghoard*

%{python_sitelib}/*

%changelog
* Thu Feb 19 2015 Hannu Valtonen <hannu.valtonen@ohmu.fi> - 0.9.0
- Initial RPM package spec
