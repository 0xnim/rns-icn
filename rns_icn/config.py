"""ICN Configuration — shared config classes and TOML loader."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore

from .server import ServerRole


@dataclass
class KnownPeer:
    """Pre-configured peer for announce table injection."""

    name: str
    destination_hash: str  # 32-char hex
    identity_path: Optional[str] = None
    aliases: List[str] = field(default_factory=list)

    def destination_bytes(self) -> bytes:
        """Return destination hash as bytes."""
        return bytes.fromhex(self.destination_hash)


@dataclass
class ClientConfig:
    """ICN Client configuration."""

    identity_path: Optional[str] = None
    mesh_interfaces: List[str] = field(default_factory=lambda: ["UTN Oregon"])
    known_peers: List[KnownPeer] = field(default_factory=list)
    connect_timeout: float = 60.0
    fetch_timeout: float = 30.0
    path_request_timeout: float = 30.0
    # Retry configuration
    max_retries: int = 5
    base_retry_delay: float = 1.0
    max_retry_delay: float = 30.0
    # When True, reject Data that isn't signed-and-verified against the
    # producer identity. When False (default), verify signatures when present
    # but accept unsigned/unverifiable Data (hash-only, additive rollout).
    require_signature: bool = False
    log_level: str = "INFO"
    log_json: bool = False


@dataclass
class ServerConfig:
    """ICN Server configuration."""

    identity_path: str
    app_name: str = "icn"
    aspect: str = "default"
    # RNS configuration directory. When set, the server initialises Reticulum
    # against this directory — pointing it at a shared rnsd's configdir lets the
    # server ride that transport daemon instead of owning interfaces itself.
    # None uses RNS's default (~/.reticulum).
    rns_configdir: Optional[str] = None
    mesh_interfaces: List[str] = field(default_factory=lambda: ["UTN Oregon"])
    role: ServerRole = ServerRole.ORIGIN
    announce_interval: float = 30.0
    reannounce_on_link: bool = True
    cs_max_entries: int = 10000
    cs_ttl_seconds: Optional[int] = None
    cs_path: str = "~/.icn/content_store.db"
    cs_prefix_ttls: Dict[str, int] = field(default_factory=dict)
    # Seconds past a Data's freshness_period during which a stale cache hit is
    # served immediately while a background revalidation refreshes it. 0
    # disables stale-while-revalidate (caches forward on staleness instead).
    cs_stale_while_revalidate: int = 0
    resource_threshold: int = 100_000
    known_peers: List[KnownPeer] = field(default_factory=list)
    log_level: str = "INFO"
    log_json: bool = False
    http_enabled: bool = False
    http_host: str = "127.0.0.1"
    http_port: int = 8080


def load_client_config(path: str = "icn.toml") -> ClientConfig:
    """Load client configuration from TOML file."""
    data = _load_toml(path).get("client", {})
    return _dict_to_client_config(data, path)


def load_server_config(path: str = "icn.toml") -> ServerConfig:
    """Load server configuration from TOML file."""
    data = _load_toml(path).get("server", {})
    return _dict_to_server_config(data, path)


def _load_toml(path: str) -> Dict[str, Any]:
    """Load and parse TOML file."""
    p = Path(path).expanduser()
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(p, "rb") as f:
        return tomllib.load(f)


def _dict_to_client_config(data: Dict[str, Any], base_path: str) -> ClientConfig:
    """Convert dict to ClientConfig, expanding paths relative to config file."""
    base_dir = Path(base_path).expanduser().parent

    known_peers = []
    for kp in data.get("known_peers", []):
        identity_path = kp.get("identity_path")
        if identity_path:
            identity_path = str((base_dir / identity_path).expanduser())
        known_peers.append(
            KnownPeer(
                name=kp["name"],
                destination_hash=kp["destination_hash"],
                identity_path=identity_path,
                aliases=kp.get("aliases", []),
            )
        )

    identity_path = data.get("identity_path")
    if identity_path:
        identity_path = str((base_dir / identity_path).expanduser())

    return ClientConfig(
        identity_path=identity_path,
        mesh_interfaces=data.get("mesh_interfaces", ["UTN Oregon"]),
        known_peers=known_peers,
        connect_timeout=data.get("connect_timeout", 60.0),
        fetch_timeout=data.get("fetch_timeout", 30.0),
        path_request_timeout=data.get("path_request_timeout", 30.0),
        require_signature=data.get("require_signature", False),
        log_level=data.get("log_level", "INFO"),
        log_json=data.get("log_json", False),
    )


def _dict_to_server_config(data: Dict[str, Any], base_path: str) -> ServerConfig:
    """Convert dict to ServerConfig, expanding paths relative to config file."""
    base_dir = Path(base_path).expanduser().parent

    known_peers = []
    for kp in data.get("known_peers", []):
        identity_path = kp.get("identity_path")
        if identity_path:
            identity_path = str((base_dir / identity_path).expanduser())
        known_peers.append(
            KnownPeer(
                name=kp["name"],
                destination_hash=kp["destination_hash"],
                identity_path=identity_path,
                aliases=kp.get("aliases", []),
            )
        )

    identity_path = str((base_dir / data["identity_path"]).expanduser())

    role_name = data.get("role", "ORIGIN")
    role_map = {
        "ORIGIN": ServerRole.ORIGIN,
        "CACHE": ServerRole.CACHE,
        "PROPAGATION": ServerRole.PROPAGATION,
    }
    role = role_map.get(role_name, ServerRole.ORIGIN)

    rns_configdir = data.get("rns_configdir")
    if rns_configdir:
        rns_configdir = str(Path(rns_configdir).expanduser())

    return ServerConfig(
        identity_path=identity_path,
        app_name=data.get("app_name", "icn"),
        aspect=data.get("aspect", "default"),
        rns_configdir=rns_configdir,
        mesh_interfaces=data.get("mesh_interfaces", ["UTN Oregon"]),
        role=role,
        announce_interval=data.get("announce_interval", 30.0),
        reannounce_on_link=data.get("reannounce_on_link", True),
        cs_max_entries=data.get("cs_max_entries", 10000),
        cs_ttl_seconds=data.get("cs_ttl_seconds"),
        cs_path=str((base_dir / data.get("cs_path", "~/.icn/content_store.db")).expanduser()),
        cs_prefix_ttls=data.get("cs_prefix_ttls", {}),
        cs_stale_while_revalidate=data.get("cs_stale_while_revalidate", 0),
        resource_threshold=data.get("resource_threshold", 100_000),
        known_peers=known_peers,
        log_level=data.get("log_level", "INFO"),
        log_json=data.get("log_json", False),
        http_enabled=data.get("http_enabled", False),
        http_host=data.get("http_host", "127.0.0.1"),
        http_port=data.get("http_port", 8080),
    )