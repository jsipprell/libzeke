%define pkgname zeke
%define libname libzeke
%define version 1.0.1
%define release 2

Summary: High-level zookeeper toolset
Name: MI-%{pkgname}
Version: %{version}
Release: %{release}%{?dist}
URL: http://wiki.mcclatchyinteractive.com/libzeke
Vendor: McClatchy Interactive, Inc.
Source: zeke-%{version}.tar.gz
License: Apache License, Version 2.0
Packager: Jesse Sipprell <jsipprell@mcclatchyinteractive.com>
Group: System Environment/Applications
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
BuildRequires: apr-devel >= 1.2.0, apr-util >= 1.2.0, pcre-devel >= 6.5
BuildRequires: zookeeper-devel >= 3.4.5

%description
Zeke is a high-level toolset for manipulating, managing and developing software
for Apache ZooKeeper.

The toolset provides functionality such as zookeeper locking, zookeeper election
interrogation and manipulation and maintenance.

It is based on a shared libary: libzeke.

%package -n MI-%{libname}
Summary: Shared libraries required for zeke-based applications
Group: System Environment/Base

%description -n MI-%{libname}
Shared libaries required by MI-zeke and related applications.

%package devel
Summary: Files required to build software using MI-libzeke
Group: Development/Libraries

%description devel
Contains header files and libaries required to build applications that link to MI-libzeke.

%prep
%setup -q -n %{pkgname}-%{version}

%build
%configure
make

%install
make install DESTDIR=$RPM_BUILD_ROOT
rm -f $RPM_BUILD_ROOT%{_libdir}/%{libname}.so.?

%post -n MI-%{libname} -p /sbin/ldconfig
%postun -n MI-%{libname} -p /sbin/ldconfig

%clean
[ "$RPM_BUILD_ROOT" != "/" ] && rm -rf $RPM_BUILD_ROOT
[ -d "%{_builddir}/%{name}-%{version}" ] && rm -rf "%{_builddir}/%{name}-%{version}"
# Uncomment below for addl cleanup
test -d "%{_builddir}/%{pkgname}-%{version}]" && rm -rf "%{_builddir}/%{pkgname}-%{version}"

%files
%defattr(-,root,root,0755)
%doc ChangeLog AUTHORS COPYING INSTALL NEWS README
%{_bindir}/*

%files -n MI-%{libname}
%defattr(-,root,root,0755)
%{_libdir}/%{libname}.so.?.?.?

%files devel
%defattr(-,root,root,0755)
%{_libdir}/%{libname}.so
%{_libdir}/%{libname}.a
%{_libdir}/%{libname}.la
%{_libdir}/pkgconfig/*.pc

%{_includedir}/zeke

%changelog
* Thu Oct 17 2013 Jesse Sipprell <jsipprell@mcclatchyinteractive.com> - 1.0.1-1
- Rebuilt with finalized libzeke API.

* Tue Oct 15 2013 Jesse Sipprell <jsipprell@mcclatchyinteractive.com> - 1.0.0-1
- Initial release.
