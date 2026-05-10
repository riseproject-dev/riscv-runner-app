#!/usr/bin/env python3
"""Snapshot which IPs raw.githubusercontent.com currently resolves to, for
the node_exporter textfile collector.

Resolves via socket.getaddrinfo (the libc resolver curl uses), so the IPs
we record match the customer's real traffic path. Used to correlate slow
windows with Fastly cache-region or POP flips (H1).
"""

import os
import socket
import tempfile
from pathlib import Path

OUT = Path("/var/lib/node_exporter/textfile_collector/dns_probe.prom")
HOST = "raw.githubusercontent.com"

HEADER = """\
# HELP runner_dns_resolved_ip Info metric: 1 per IP currently resolved for HOST.
# TYPE runner_dns_resolved_ip gauge
# HELP runner_dns_resolved_ip_count Number of IPs returned for HOST.
# TYPE runner_dns_resolved_ip_count gauge
"""


def resolve(host: str) -> list[str]:
    ips: set[str] = set()
    for family in (socket.AF_INET, socket.AF_INET6):
        try:
            ips.update(info[4][0] for info in socket.getaddrinfo(host, None, family, socket.SOCK_STREAM))
        except socket.gaierror:
            pass
    return sorted(ips)


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
    ips = resolve(HOST)
    body = HEADER + "".join(
        f'runner_dns_resolved_ip{{host="{HOST}",ip="{ip}"}} 1\n' for ip in ips
    ) + f'runner_dns_resolved_ip_count{{host="{HOST}"}} {len(ips)}\n'
    write_atomic(OUT, body)


if __name__ == "__main__":
    main()
