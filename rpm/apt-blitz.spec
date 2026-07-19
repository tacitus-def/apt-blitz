%global debug_package %{nil}

Name:       apt-blitz
Version:    0.7.4
Release:    1%{?dist}
Summary:    APT proxy with multithreaded downloading via Range requests
Group:      Networking/Other

License:    MIT
URL:        https://github.com/tacitus-def/apt-blitz
Source0:    %{name}-%{version}.tar.gz
BuildRequires:  cargo, rust, openssl-devel
Requires(pre):  chkconfig
Requires(preun): chkconfig, initscripts
Requires(postun): chkconfig, initscripts
%{?systemd_requires}

%description
apt-blitz is an HTTP forward proxy designed for APT.
It accelerates package downloads by splitting files into segments
and downloading them concurrently using HTTP Range requests.

Features:
- Multithreaded downloads via HTTP Range requests
- SQLite-based disk cache with LRU eviction
- In-flight request coalescing (deduplication)
- FTP support
- CONNECT tunnel for HTTPS
- YAML configuration with URL mapping and upstream proxy support

%prep
%setup -q -n %{name}-%{version}

%build
cargo build --release --offline

%install
install -D -m 0755 target/release/apt-blitz %{buildroot}%{_bindir}/apt-blitz
install -D -m 0644 debian/apt-blitz.service %{buildroot}%{_unitdir}/apt-blitz.service
install -D -m 0644 debian/apt-blitz.default %{buildroot}%{_sysconfdir}/default/apt-blitz
install -D -m 0644 man/apt-blitz.1 %{buildroot}%{_mandir}/man1/apt-blitz.1
install -D -m 0755 debian/init.d/apt-blitz %{buildroot}%{_initddir}/apt-blitz
install -d -m 0750 %{buildroot}/var/cache/apt-blitz

%pre
getent group apt-blitz >/dev/null 2>&1 || groupadd --system apt-blitz
getent passwd apt-blitz >/dev/null 2>&1 || \
    useradd --system --gid apt-blitz --no-create-home \
        --home-dir /var/cache/apt-blitz \
        --shell /sbin/nologin apt-blitz
exit 0

%post
if [ -d /run/systemd/system ]; then
    %systemd_post apt-blitz.service
else
    /sbin/chkconfig --add apt-blitz >/dev/null 2>&1 || :
fi

%preun
if [ -d /run/systemd/system ]; then
    %systemd_preun apt-blitz.service
else
    if [ $1 -eq 0 ]; then
        /sbin/service apt-blitz stop >/dev/null 2>&1 || :
        /sbin/chkconfig --del apt-blitz >/dev/null 2>&1 || :
    fi
fi

%postun
if [ -d /run/systemd/system ]; then
    %systemd_postun_with_restart apt-blitz.service
else
    if [ $1 -ge 1 ]; then
        /sbin/service apt-blitz condrestart >/dev/null 2>&1 || :
    fi
fi

%files
%{_bindir}/apt-blitz
%{_mandir}/man1/apt-blitz.1*
%{_unitdir}/apt-blitz.service
%{_initddir}/apt-blitz
%config(noreplace) %{_sysconfdir}/default/apt-blitz
%attr(0750, apt-blitz, apt-blitz) %dir /var/cache/apt-blitz

%doc README.md

%changelog
* Sat Jul 04 2026 Petr Sleptsov <spetr@bk.ru> - 0.1.2-1
- Add man page
- Default bind address changed to 127.0.0.1

* Fri Jul 03 2026 Petr Sleptsov <spetr@bk.ru> - 0.1.0-1
- Initial RPM release
