Name:           apt-blitz
Version:        0.14.6
Release:        1%{?dist}
Summary:        HTTP forward proxy for APT package managers with multithreaded downloads

License:        MIT
URL:            https://github.com/tacitus-def/apt-blitz
Source0:        %{name}-%{version}.tar.gz
Group:          System Environment/Daemons

BuildRequires:  cargo >= 1.70
BuildRequires:  rust >= 1.70
BuildRequires:  systemd-rpm-macros
BuildRequires:  pkgconfig(openssl)

Requires:       systemd

%description
HTTP forward proxy for APT package managers with multithreaded downloads,
FTP support, and disk caching.

Inspired by apt-cacher-ng and aria2 — combines request coalescing, range-based
parallel downloads, CONNECT tunneling, FTP proxying, and LRU-evicting SQLite
cache to speed up repetitive package downloads in CI or local networks.

%prep
%setup -q -n apt-blitz-%{version}

%build
export CARGO_HOME=%{_builddir}/cargo-home
cargo build --release --locked

%install
rm -rf %{buildroot}

# Binary
install -Dpm 0755 target/release/apt-blitz %{buildroot}%{_bindir}/apt-blitz

# systemd service
install -Dpm 0644 debian/apt-blitz.service %{buildroot}%{_unitdir}/apt-blitz.service

# Default config
install -Dpm 0644 debian/apt-blitz.default %{buildroot}%{_sysconfdir}/default/apt-blitz

# Config example
install -Dpm 0644 /dev/null %{buildroot}%{_sysconfdir}/apt-blitz/config.yml
echo 'connections: 16' > %{buildroot}%{_sysconfdir}/apt-blitz/config.yml
echo 'url_map:' >> %{buildroot}%{_sysconfdir}/apt-blitz/config.yml
echo '  - mirror-yandex=https://mirror.yandex.ru' >> %{buildroot}%{_sysconfdir}/apt-blitz/config.yml

# License
install -Dpm 0644 LICENSE.md %{buildroot}%{_licensedir}/apt-blitz/LICENSE.md

# Cache directory (ownership set in %post)
install -dm 0750 %{buildroot}%{_localstatedir}/cache/apt-blitz

%pre
getent group apt-blitz >/dev/null || groupadd -r apt-blitz
getent passwd apt-blitz >/dev/null || \
    useradd -r -g apt-blitz -d %{_localstatedir}/cache/apt-blitz -s /sbin/nologin apt-blitz
exit 0

%post
%systemd_post apt-blitz.service

%preun
%systemd_preun apt-blitz.service

%postun
%systemd_postun_with_restart apt-blitz.service

%files
%doc README.md
%{_bindir}/apt-blitz
%{_unitdir}/apt-blitz.service
%config(noreplace) %{_sysconfdir}/default/apt-blitz
%config(noreplace) %{_sysconfdir}/apt-blitz/config.yml
%dir %attr(0750,-,-) %{_localstatedir}/cache/apt-blitz

%changelog
* Fri Oct 23 2026 demiurg <demiurg@example.com> - 0.14.6-1
- Fix: ensure fallback to plain_proxy on If-Match 412 works (was missing in 0.14.5)
- Client Range header support: serve 206 Partial Content for resume
- If-Match 412 fallback to plain proxy: mirror re-sync no longer retries multithreaded
- All tests passing (397)

* Fri Oct 23 2026 demiurg <demiurg@example.com> - 0.14.5-1
- Client Range header support: serve 206 Partial Content for resume
- If-Match 412 retry per-segment: mirror re-sync no longer aborts full download
- All tests passing (397)

* Mon Oct 23 2026 demiurg <demiurg@example.com> - 0.14.3-1
- Fixed coalescer leader guard cleanup on HEAD error (prevents 500 errors on index files)
- All tests passing (389)