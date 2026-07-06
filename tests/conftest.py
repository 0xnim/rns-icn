"""Shared fixtures for the rns-icn test suite.

``RNS.Reticulum`` is a hard process singleton: it can be initialised exactly
once per process and can never be re-initialised, so test files cannot each
bring up their own differently-configured instance. Every test that needs an
in-process RNS therefore shares the single session-scoped instance below. It
lives in a temporary configdir (never the user's ``~/.reticulum``) and listens
on a localhost TCP port; subprocess nodes that must reach the test process
connect *to* that port (e.g. ``_icn_origin.py --connect``) rather than the
test process dialling out, so per-test topology never requires reconfiguring
the singleton.
"""

import socket
from types import SimpleNamespace

import pytest


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def shared_rns(tmp_path_factory):
    """The process-wide Reticulum instance; ``.port`` is its TCP listen port."""
    import RNS

    if RNS.Reticulum.get_instance() is not None:
        pytest.fail(
            "Reticulum was already initialised outside the shared_rns fixture. "
            "In-process RNS tests must request this fixture instead of calling "
            "RNS.Reticulum() themselves."
        )
    port = free_port()
    configdir = tmp_path_factory.mktemp("shared_rns")
    (configdir / "config").write_text(
        f"""[reticulum]
  enable_transport = Yes
  share_instance = No
  panic_on_interface_error = No

[logging]
  loglevel = 3

[interfaces]
  [[TCP Server Interface]]
    type = TCPServerInterface
    interface_enabled = yes
    listen_ip = 127.0.0.1
    listen_port = {port}
"""
    )
    RNS.Reticulum(configdir=str(configdir))
    return SimpleNamespace(port=port, configdir=str(configdir))
