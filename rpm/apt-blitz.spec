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
install -d -m 0750 %{buildroot}/var/cache/apt-blitz

%pre
getent group apt-blitz >/dev/null 2>&1 || groupadd --system apt-blitz
getent passwd apt-blitz >/dev/null 2>&1 || \
    useradd --system --gid apt-blitz --no-create-home \
        --home-dir /var/cache/apt-blitz \
        --shell /sbin/nologin apt-blitz
exit 0

%post
%systemd_post apt-blitz.service

%preun
%systemd_preun apt-blitz.service

%postun
%systemd_postun_with_restart apt-blitz.service

%files
%{_bindir}/apt-blitz
%{_mandir}/man1/apt-blitz.1*
%{_unitdir}/apt-blitz.service
%config(noreplace) %{_sysconfdir}/default/apt-blitz
%attr(0750, apt-blitz, apt-blitz) %dir /var/cache/apt-blitz

%doc README.md

%changelog
* Sat Jul 04 2026 Petr Sleptsov <spetr@bk.ru> - 0.1.2-1
- Add man page
- Default bind address changed to 127.0.0.1

* Fri Jul 03 2026 Petr Sleptsov <spetr@bk.ru> - 0.1.0-1
- Initial RPM release
