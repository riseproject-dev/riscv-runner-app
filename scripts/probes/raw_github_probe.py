#!/usr/bin/env python3
"""Probe raw.githubusercontent.com download speed for the node_exporter
textfile collector.

Hits the Fastly target the customer's CI actually downloads from and a
non-Fastly comparison endpoint. When the Fastly target sags but the
control stays flat → H9 (Scaleway↔Fastly path). When both sag → H5
(Scaleway WAN egress).

curl is shelled out so we get %{remote_ip} reflecting the IP libcurl
actually connected to — matches the customer's real traffic path more
faithfully than socket.gethostbyname would.
"""

import os
import subprocess
import tempfile
from pathlib import Path

OUT = Path("/var/lib/node_exporter/textfile_collector/raw_github_probe.prom")

TARGETS = [
    ("raw.githubusercontent.com",
     "https://raw.githubusercontent.com/usnistgov/ACVP-Server/master/README.md"),
    ("cloudflare-control",
     "https://speed.cloudflare.com/__down?bytes=1048576"),
]

CURL_FORMAT = "%{time_total} %{speed_download} %{remote_ip} %{exitcode}\n"

HEADER = """\
# HELP raw_github_probe_seconds Wallclock to download a fixed test artefact.
# TYPE raw_github_probe_seconds gauge
# HELP raw_github_probe_bytes_per_second Average download throughput in bytes/sec.
# TYPE raw_github_probe_bytes_per_second gauge
# HELP raw_github_probe_curl_exit_code Curl exit code; 0 on success.
# TYPE raw_github_probe_curl_exit_code gauge
"""


def probe(target: str, url: str) -> str:
    """Run one curl, return Prom-formatted lines for the result."""
    try:
        result = subprocess.run(
            ["curl", "-o", "/dev/null", "-s", "--max-time", "30", "-w", CURL_FORMAT, url],
            capture_output=True, text=True, timeout=35,
        )
        fields = (result.stdout.strip() or "0 0 unknown 99").split()
    except (subprocess.TimeoutExpired, FileNotFoundError):
        fields = ["0", "0", "unknown", "99"]
    seconds, bps, ip, code = (fields + ["0", "0", "unknown", "99"])[:4]
    return (
        f'raw_github_probe_seconds{{target="{target}",remote_ip="{ip}"}} {seconds}\n'
        f'raw_github_probe_bytes_per_second{{target="{target}"}} {bps}\n'
        f'raw_github_probe_curl_exit_code{{target="{target}"}} {code}\n'
    )


def write_atomic(path: Path, content: str) -> None:
    """Write content via tempfile + os.replace so node_exporter never sees a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix="." + path.name + ".")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(content)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def main() -> None:
    write_atomic(OUT, HEADER + "".join(probe(name, url) for name, url in TARGETS))


if __name__ == "__main__":
    main()
