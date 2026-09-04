"""Source, wheel, and direct-sdist parity gate for offline Strimzi export."""

from __future__ import annotations

import argparse
import ast
import base64
import gzip
import hashlib
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
import venv
import zipfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

_RESOURCE_PREFIX = "streamt/integrations/gitops/schemas"
_SCHEMA_RESOURCE = "strimzi-1.2.0-kafkatopic-crd.yaml.gz.b64"
_LICENSE_RESOURCE = "strimzi-1.2.0-LICENSE.txt"
_NOTICE_RESOURCE = "strimzi-1.2.0-NOTICE.txt"
_SOURCE_DIGEST = "36390f0731c699448076d4ee739e8b7f331d083e91a7fb71500aaa830ab1127e"
_RESOURCE_EXPECTATIONS = {
    "README.md": (1_621, "b52b11dc90ec2bba8b745dc7a4673637727889d10a81bef0eb73e4e02575795f"),
    "__init__.py": (80, "14a9c9a4fc868cfd8a2a0c28948b0a97d6ecdd899ceff7be1825a57a9523b046"),
    _SCHEMA_RESOURCE: (
        2_598,
        "f1e20b00cf206d10d65bec699f6966ad5df535102360f1975bd2d6c2a2d04587",
    ),
    _LICENSE_RESOURCE: (
        11_357,
        "b40930bbcf80744c86c46a12bc9da056641d722716c378f5659b9e555ef833e1",
    ),
    _NOTICE_RESOURCE: (
        1_169,
        "3ceda40a278d56f94646305b1fc5dfb3e437b6b2663f48fefe10544df15b1053",
    ),
}
_COMPRESSED_DIGEST = "2c3773847d5f83277940551ad1805b522c79d79441a3f3d423fe6b71d0766b92"
_FORBIDDEN_NAME = re.compile(
    r"(?:^|[^a-z0-9])(?:kubernetes|openshift|strimzi|pyhelm|helm)(?:[^a-z0-9]|$)",
    re.IGNORECASE,
)
_FORBIDDEN_PACKAGES = frozenset(
    {"helm", "kubernetes", "kubernetes_asyncio", "openshift", "pyhelm", "strimzi"}
)
_PIP_VERSION = "26.2.1"
_BUILD_BACKEND = "hatchling==1.32.0"
_MAX_OUTPUT = 512 * 1024
_EXPECTED_TEXT_SHA256 = "cf3ea1f7506c91c8c7e2c0e7d1cba8695ab381d27d9e3ab8d3143e89d4ad6c10"
_EXPECTED_JSON_SHA256 = "7ec6fa6ea5c1af68f180607b50001a83f321d71ff31777fb80ccbf169baff214"
_EXPECTED_METADATA = frozenset(
    {
        "Requires-Dist: click>=8.0",
        "Requires-Dist: confluent-kafka>=2.0",
        "Requires-Dist: jinja2>=3.0",
        "Requires-Dist: jsonschema<5,>=4.18",
        "Requires-Dist: pydantic>=2.0",
        "Requires-Dist: python-dotenv>=1.0",
        "Requires-Dist: pyyaml>=6.0",
        "Requires-Dist: requests>=2.28",
        "Requires-Dist: rich>=13.0",
        "Requires-Dist: sqlglot<30,>=26.0",
        "Provides-Extra: dev",
        "Requires-Dist: black>=23.0; extra == 'dev'",
        "Requires-Dist: mypy==1.19.1; extra == 'dev'",
        "Requires-Dist: pytest-asyncio>=0.21; extra == 'dev'",
        "Requires-Dist: pytest-cov>=4.0; extra == 'dev'",
        "Requires-Dist: pytest>=7.0; extra == 'dev'",
        "Requires-Dist: ruff>=0.1; extra == 'dev'",
        "Requires-Dist: types-jsonschema>=4.26; extra == 'dev'",
        "Requires-Dist: types-pyyaml; extra == 'dev'",
        "Requires-Dist: types-requests==2.33.0.20260712; extra == 'dev'",
        "Provides-Extra: docs",
        "Requires-Dist: mkdocs-material>=9.5; extra == 'docs'",
        "Requires-Dist: mkdocs>=1.5; extra == 'docs'",
        "Requires-Dist: pymdown-extensions>=10.0; extra == 'docs'",
        "Provides-Extra: postgres",
        "Requires-Dist: psycopg[binary]<4,>=3.2; extra == 'postgres'",
    }
)
_RUNTIME_REQUIREMENTS = (
    "click>=8.0",
    "confluent-kafka>=2.0",
    "jinja2>=3.0",
    "jsonschema<5,>=4.18",
    "pydantic>=2.0",
    "python-dotenv>=1.0",
    "pyyaml>=6.0",
    "requests>=2.28",
    "rich>=13.0",
    "sqlglot<30,>=26.0",
)
_DYNAMIC_IMPORT_BOUNDARY = {
    "streamt/cli/__init__.py": (("import_module", "command.module"),),
    "streamt/compiler/__init__.py": (("import_module", "module_name"),),
    "streamt/core/__init__.py": (("import_module", "module_name"),),
}
_DYNAMIC_IMPORT_FILE_SHA256 = {
    "streamt/cli/__init__.py": (
        "907ba90e56e479a138daaa36353c87a8f005b9c4851c4beb92eac09c5585d989"
    ),
    "streamt/compiler/__init__.py": (
        "65ca0c7b7cf44612db1cc6c3dcd6d34679d961ad2ce315df32c92925fbe761f2"
    ),
    "streamt/core/__init__.py": (
        "2d3010ec3724d7c4856f18d3abbe92bc81b36b84cb1bb09fdef7c108afdf60c9"
    ),
}

_PROJECT = "public-project-strimzi-parity"
_DIRECT_OWNER = "public-owner-direct"
_HASHED_OWNER = "public-owner-hashed"
_EXTERNAL_OWNER = "public-owner-external"
_DIRECT_TOPIC = "public-topic-direct"
_HASHED_TOPIC = "Public_Topic_Hashed"
_EXTERNAL_TOPIC = "External_Public_Omitted"
_NAMESPACE = "package-parity"
_CLUSTER = "parity-kafka"
_RUNTIME_SECRET = "CONFIDENTIAL_RUNTIME_SECRET_7b61"
_CONNECTION_SECRET = "CONFIDENTIAL_CONNECTION_SECRET_0c29"
_SQL_SECRET = "CONFIDENTIAL_SQL_SECRET_f413"
_TAG_SECRET = "CONFIDENTIAL_TAG_SECRET_42ea"
_ENV_SECRET = "CONFIDENTIAL_ENV_SECRET_8d30"
_SCHEMA_REGISTRY_SECRET = "CONFIDENTIAL_SCHEMA_REGISTRY_SECRET_32ce"
_FLINK_SECRET = "CONFIDENTIAL_FLINK_SECRET_117a"
_CONNECT_SECRET = "CONFIDENTIAL_CONNECT_SECRET_a240"
_GATEWAY_SECRET = "CONFIDENTIAL_GATEWAY_SECRET_0e91"
_STATE_SECRET = "postgresql://CONFIDENTIAL_STATE_SECRET_3c62@state.invalid/streamt"
_REJECTED_SECRET = "CONFIDENTIAL_REJECTED_CONFIG_SECRET_c021"
_PATH_SECRET = "CONFIDENTIAL_PROJECT_PATH_SECRET_a301"
_OUTPUT_PATH_SECRET = "CONFIDENTIAL_OUTPUT_PATH_SECRET_e900"
_CONFIDENTIAL = (
    _RUNTIME_SECRET,
    _CONNECTION_SECRET,
    _SQL_SECRET,
    _TAG_SECRET,
    _ENV_SECRET,
    _SCHEMA_REGISTRY_SECRET,
    _FLINK_SECRET,
    _CONNECT_SECRET,
    _GATEWAY_SECRET,
    _STATE_SECRET,
    _REJECTED_SECRET,
    _PATH_SECRET,
    _OUTPUT_PATH_SECRET,
)

_GUARD_SOURCE = r'''"""Injected fail-closed guard for one exporter target process."""
import builtins
import http.client
import importlib
import os
import socket
import subprocess
import sys
import urllib.request

_FORBIDDEN_IMPORTS = (
    "streamt.deployer",
    "streamt.deployment",
    "streamt.planner",
    "streamt.provider",
    "streamt.providers",
    "streamt.state",
    "streamt.state_backend",
    "streamt.cli.helpers",
    "streamt.integrations.openlineage.transport",
    "confluent_kafka",
)
_FORBIDDEN_EVENTS = {
    "socket.__new__",
    "socket.getaddrinfo",
    "socket.connect",
    "socket.connect_ex",
    "subprocess.Popen",
    "os.system",
    "os.fork",
    "os.forkpty",
    "os.posix_spawn",
    "os.posix_spawnp",
}

def _deny(*_args, **_kwargs):
    raise RuntimeError("offline Strimzi package guard denied an operation")

def _forbidden_import(name):
    return isinstance(name, str) and any(
        name == prefix or name.startswith(prefix + ".")
        for prefix in _FORBIDDEN_IMPORTS
    )

def _audit(event, args):
    if event == "import" and args:
        name = args[0]
        if _forbidden_import(name):
            raise RuntimeError("offline Strimzi package guard denied an import")
    if event in _FORBIDDEN_EVENTS:
        raise RuntimeError("offline Strimzi package guard denied an operation")

sys.addaudithook(_audit)
socket.socket = _deny
socket.getaddrinfo = _deny
socket.create_connection = _deny
subprocess.Popen = _deny
subprocess.run = _deny
subprocess.call = _deny
subprocess.check_call = _deny
subprocess.check_output = _deny
urllib.request.urlopen = _deny
http.client.HTTPConnection.connect = _deny
http.client.HTTPSConnection.connect = _deny
for name in ("fork", "forkpty", "posix_spawn", "posix_spawnp", "system"):
    if hasattr(os, name):
        setattr(os, name, _deny)

try:
    import requests.sessions
except ImportError:
    pass
else:
    requests.sessions.Session.__init__ = _deny
    requests.sessions.Session.request = _deny

_original_import = builtins.__import__
_original_import_module = importlib.import_module

def _guarded_import(name, *args, **kwargs):
    if _forbidden_import(name):
        raise RuntimeError("offline Strimzi package guard denied an import")
    return _original_import(name, *args, **kwargs)

def _guarded_import_module(name, *args, **kwargs):
    if _forbidden_import(name):
        raise RuntimeError("offline Strimzi package guard denied an import")
    return _original_import_module(name, *args, **kwargs)

builtins.__import__ = _guarded_import
importlib.import_module = _guarded_import_module

marker = os.environ.get("STREAMT_STRIMZI_GUARD_MARKER")
if marker is None:
    raise RuntimeError("offline Strimzi package guard marker was not configured")
with open(marker, "xb") as stream:
    stream.write(b"active\n")
'''

_RESOURCE_PROBE = r'''
import base64
import gzip
import hashlib
import os
from importlib.resources import files
from pathlib import Path

import streamt

module = Path(streamt.__file__).resolve()
checkout = Path(os.environ["STREAMT_PARITY_CHECKOUT"]).resolve()
source = (checkout / "src").resolve()
mode = os.environ["STREAMT_PARITY_MODE"]
if mode == "source":
    assert source in module.parents, "source probe did not import the checkout"
else:
    assert checkout not in module.parents, "installed probe imported the checkout"
resource = files("streamt.integrations.gitops.schemas").joinpath(
    "strimzi-1.2.0-kafkatopic-crd.yaml.gz.b64"
)
decoded = gzip.decompress(base64.b64decode(b"".join(resource.read_bytes().split()), validate=True))
print(hashlib.sha256(decoded).hexdigest())
'''

_DENIAL_PROBES = {
    "socket": "import socket; socket.socket()",
    "dns": "import socket; socket.getaddrinfo('example.invalid', 443)",
    "http": "import requests; requests.Session()",
    "subprocess": "import subprocess; subprocess.Popen(['false'])",
    "fork": "import os; os.fork()",
    "admin-client": "from confluent_kafka.admin import AdminClient; AdminClient({})",
    "producer": "from confluent_kafka import Producer; Producer({})",
    "consumer": "from confluent_kafka import Consumer; Consumer({})",
    "serializing-producer": (
        "from confluent_kafka import SerializingProducer; SerializingProducer({})"
    ),
    "deserializing-consumer": (
        "from confluent_kafka import DeserializingConsumer; DeserializingConsumer({})"
    ),
    "forbidden-import": "__import__('streamt.deployer')",
    "cli-helper-import": "__import__('streamt.cli.helpers')",
    "openlineage-transport-import": (
        "__import__('streamt.integrations.openlineage.transport')"
    ),
}


@dataclass(frozen=True)
class _Target:
    name: str
    python: Path
    command: tuple[str, ...]
    pythonpath: str
    mode: str


@dataclass(frozen=True)
class _Completed:
    returncode: int
    stdout: bytes
    stderr: bytes


@dataclass(frozen=True)
class _TargetResults:
    text: _Completed
    structured: _Completed
    file_mode: _Completed
    file_bytes: bytes
    mapper_failure: _Completed
    primitive_failure: _Completed


def _member_name(resource: str, names: set[str], *, sdist: bool) -> str:
    expected = f"{_RESOURCE_PREFIX}/{resource}"
    if not sdist:
        if expected not in names:
            raise AssertionError("distribution resource is missing")
        return expected
    matches = [name for name in names if name.endswith(f"/src/{expected}")]
    if len(matches) != 1:
        raise AssertionError("source distribution resource is missing or duplicated")
    return matches[0]


def _inspect_resource_subtree(names: set[str], *, sdist: bool) -> None:
    if sdist:
        found = {
            name.rsplit("/src/", 1)[1]
            for name in names
            if f"/src/{_RESOURCE_PREFIX}/" in name
        }
    else:
        found = {name for name in names if name.startswith(f"{_RESOURCE_PREFIX}/")}
    expected = {f"{_RESOURCE_PREFIX}/{resource}" for resource in _RESOURCE_EXPECTATIONS}
    if found != expected:
        raise AssertionError("distribution schema resource subtree changed")


def _inspect_resources(
    names: set[str],
    read: Callable[[str], bytes],
    *,
    sdist: bool,
) -> None:
    _inspect_resource_subtree(names, sdist=sdist)
    resources: dict[str, bytes] = {}
    for resource, (size, digest) in _RESOURCE_EXPECTATIONS.items():
        payload = read(_member_name(resource, names, sdist=sdist))
        if len(payload) != size or hashlib.sha256(payload).hexdigest() != digest:
            raise AssertionError("distribution resource bytes changed")
        resources[resource] = payload
    compressed = base64.b64decode(
        b"".join(resources[_SCHEMA_RESOURCE].split()),
        validate=True,
    )
    if hashlib.sha256(compressed).hexdigest() != _COMPRESSED_DIGEST:
        raise AssertionError("compressed CRD bytes changed")
    if hashlib.sha256(gzip.decompress(compressed)).hexdigest() != _SOURCE_DIGEST:
        raise AssertionError("decoded CRD bytes changed")


def _metadata_boundary(metadata: bytes) -> frozenset[str]:
    selected = [
        line
        for line in metadata.decode("utf-8").splitlines()
        if line.startswith(("Requires-Dist:", "Provides-Extra:"))
    ]
    if len(selected) != len(set(selected)):
        raise AssertionError("distribution metadata contains duplicate boundary fields")
    return frozenset(selected)


def _assert_no_target_metadata(metadata: bytes) -> None:
    for line in _metadata_boundary(metadata):
        if _FORBIDDEN_NAME.search(line):
            raise AssertionError("target SDK dependency or extra is forbidden")


def _inspect_metadata(metadata: bytes) -> frozenset[str]:
    _assert_no_target_metadata(metadata)
    boundary = _metadata_boundary(metadata)
    if boundary != _EXPECTED_METADATA:
        raise AssertionError("runtime dependency or extra boundary changed")
    return boundary


def _production_python(names: set[str], *, sdist: bool) -> list[str]:
    if sdist:
        return [name for name in names if "/src/streamt/" in name and name.endswith(".py")]
    return [name for name in names if name.startswith("streamt/") and name.endswith(".py")]


def _archive_source_path(name: str, *, sdist: bool) -> str:
    if not sdist:
        return name
    marker = "/src/"
    if marker not in name:
        raise AssertionError("source distribution Python path is malformed")
    return name.split(marker, 1)[1]


def _literal_string(
    node: ast.expr,
    bindings: dict[str, set[str | None]],
) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        values = bindings.get(node.id, set())
        if len(values) == 1:
            value = next(iter(values))
            return value if isinstance(value, str) else None
        return None
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _literal_string(node.left, bindings)
        right = _literal_string(node.right, bindings)
        if left is not None and right is not None:
            return left + right
    if isinstance(node, ast.JoinedStr):
        pieces: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                pieces.append(value.value)
            elif (
                isinstance(value, ast.FormattedValue)
                and value.conversion == -1
                and value.format_spec is None
            ):
                resolved = _literal_string(value.value, bindings)
                if resolved is None:
                    return None
                pieces.append(resolved)
            else:
                return None
        return "".join(pieces)
    return None


def _assigned_names(target: ast.expr) -> set[str]:
    if isinstance(target, ast.Name):
        return {target.id}
    if isinstance(target, (ast.List, ast.Tuple)):
        return {
            name
            for element in target.elts
            for name in _assigned_names(element)
        }
    return set()


def _dynamic_callable_kind(
    node: ast.expr,
    *,
    importlib_aliases: set[str],
    import_module_aliases: set[str],
    builtins_aliases: set[str],
    builtin_import_aliases: set[str],
) -> str | None:
    if isinstance(node, ast.Name):
        if node.id in builtin_import_aliases:
            return "__import__"
        if node.id in import_module_aliases:
            return "import_module"
        return None
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        if node.attr == "__import__" and node.value.id in builtins_aliases:
            return "__import__"
        if node.attr == "import_module" and node.value.id in importlib_aliases:
            return "import_module"
        return None
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "getattr"
        and len(node.args) >= 2
        and isinstance(node.args[0], ast.Name)
        and isinstance(node.args[1], ast.Constant)
        and isinstance(node.args[1].value, str)
    ):
        module = node.args[0].id
        attribute = node.args[1].value
        if module in builtins_aliases and attribute == "__import__":
            return "__import__"
        if module in importlib_aliases and attribute == "import_module":
            return "import_module"
    return None


def _dynamic_module_kind(
    node: ast.expr,
    *,
    importlib_aliases: set[str],
    builtins_aliases: set[str],
) -> str | None:
    if not isinstance(node, ast.Name):
        return None
    if node.id in builtins_aliases:
        return "builtins"
    if node.id in importlib_aliases:
        return "importlib"
    return None


def _inspect_imports(names: set[str], read: Callable[[str], bytes], *, sdist: bool) -> None:
    python_files = _production_python(names, sdist=sdist)
    if not python_files:
        raise AssertionError("distribution contains no production Python")
    for name in python_files:
        payload = read(name)
        source_path = _archive_source_path(name, sdist=sdist)
        tree = ast.parse(payload, filename=name)
        importlib_aliases = {"importlib"}
        import_module_aliases: set[str] = set()
        builtins_aliases = {"builtins"}
        builtin_import_aliases = {"__import__"}
        string_bindings: dict[str, set[str | None]] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "importlib":
                        importlib_aliases.add(alias.asname or alias.name)
                    elif alias.name == "builtins":
                        builtins_aliases.add(alias.asname or alias.name)
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                if node.module == "importlib":
                    for alias in node.names:
                        if alias.name == "import_module":
                            import_module_aliases.add(alias.asname or alias.name)
                elif node.module == "builtins":
                    for alias in node.names:
                        if alias.name == "__import__":
                            builtin_import_aliases.add(alias.asname or alias.name)

        assignments: list[tuple[set[str], ast.expr | None]] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                value = node.value
                assigned = {name for target in node.targets for name in _assigned_names(target)}
                assignments.append((assigned, value))
            elif isinstance(node, ast.AnnAssign):
                assignments.append((_assigned_names(node.target), node.value))
            elif isinstance(node, (ast.AugAssign, ast.NamedExpr)):
                assignments.append((_assigned_names(node.target), None))
            elif isinstance(node, ast.arg):
                assignments.append(({node.arg}, None))

        for assigned, value in assignments:
            resolved = (
                _literal_string(value, string_bindings) if value is not None else None
            )
            for assigned_name in assigned:
                string_bindings.setdefault(assigned_name, set()).add(resolved)

        changed = True
        while changed:
            changed = False
            for assigned, value in assignments:
                if value is None:
                    continue
                module_kind = _dynamic_module_kind(
                    value,
                    importlib_aliases=importlib_aliases,
                    builtins_aliases=builtins_aliases,
                )
                module_destination = (
                    builtins_aliases
                    if module_kind == "builtins"
                    else importlib_aliases
                )
                if module_kind is not None:
                    before = len(module_destination)
                    module_destination.update(assigned)
                    changed = changed or len(module_destination) != before
                kind = _dynamic_callable_kind(
                    value,
                    importlib_aliases=importlib_aliases,
                    import_module_aliases=import_module_aliases,
                    builtins_aliases=builtins_aliases,
                    builtin_import_aliases=builtin_import_aliases,
                )
                destination = (
                    builtin_import_aliases if kind == "__import__" else import_module_aliases
                )
                if kind is not None:
                    before = len(destination)
                    destination.update(assigned)
                    changed = changed or len(destination) != before

        unresolved: list[tuple[str, str]] = []
        for node in ast.walk(tree):
            modules: list[str]
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                modules = [node.module]
            else:
                modules = []
            if any(module.partition(".")[0].casefold() in _FORBIDDEN_PACKAGES for module in modules):
                raise AssertionError("target SDK import is forbidden")

            if not isinstance(node, ast.Call):
                continue
            kind = _dynamic_callable_kind(
                node.func,
                importlib_aliases=importlib_aliases,
                import_module_aliases=import_module_aliases,
                builtins_aliases=builtins_aliases,
                builtin_import_aliases=builtin_import_aliases,
            )
            if kind is None:
                continue
            if not node.args:
                raise AssertionError("dynamic import target is missing")
            target = _literal_string(node.args[0], string_bindings)
            if target is not None:
                if target.partition(".")[0].casefold() in _FORBIDDEN_PACKAGES:
                    raise AssertionError("dynamic target SDK import is forbidden")
                continue
            unresolved.append((kind, ast.unparse(node.args[0])))

        expected = _DYNAMIC_IMPORT_BOUNDARY.get(source_path, ())
        if tuple(unresolved) != expected:
            raise AssertionError("unresolved dynamic import boundary changed")
        expected_digest = _DYNAMIC_IMPORT_FILE_SHA256.get(source_path)
        if expected and (
            expected_digest is None
            or hashlib.sha256(payload).hexdigest() != expected_digest
        ):
            raise AssertionError("reviewed dynamic import source bytes changed")


def _inspect_namespaces(names: set[str], *, sdist: bool) -> None:
    for name in names:
        parts = PurePosixPath(name).parts
        for part in parts:
            if part.casefold().endswith((".dist-info", ".egg-info")) and _FORBIDDEN_NAME.search(
                part
            ):
                raise AssertionError("target SDK distribution namespace is forbidden")
        if sdist:
            if len(parts) < 3 or parts[1] != "src":
                continue
            top_level = parts[2]
        else:
            if not parts:
                continue
            if (
                len(parts) >= 3
                and parts[0].casefold().endswith(".data")
                and parts[1].casefold() in {"purelib", "platlib"}
            ):
                top_level = parts[2]
            else:
                top_level = parts[0]
        normalized = top_level.casefold().replace("-", "_")
        if normalized in _FORBIDDEN_PACKAGES:
            raise AssertionError("vendored target SDK namespace is forbidden")


def _inspect_wheel(path: Path) -> frozenset[str]:
    with zipfile.ZipFile(path) as archive:
        member_names = archive.namelist()
        names = set(member_names)
        if len(names) != len(member_names):
            raise AssertionError("wheel contains duplicate members")
        _inspect_resources(names, archive.read, sdist=False)
        metadata = [name for name in names if name.endswith(".dist-info/METADATA")]
        if len(metadata) != 1:
            raise AssertionError("wheel metadata is missing or duplicated")
        boundary = _inspect_metadata(archive.read(metadata[0]))
        _inspect_imports(names, archive.read, sdist=False)
        _inspect_namespaces(names, sdist=False)
        return boundary


def _inspect_sdist(path: Path) -> frozenset[str]:
    with tarfile.open(path, "r:gz") as archive:
        member_names = archive.getnames()
        names = set(member_names)
        if len(names) != len(member_names):
            raise AssertionError("source distribution contains duplicate members")

        def read(name: str) -> bytes:
            member = archive.extractfile(name)
            if member is None:
                raise AssertionError("source distribution member is not a regular file")
            return member.read()

        _inspect_resources(names, read, sdist=True)
        metadata = [name for name in names if name.endswith("/PKG-INFO")]
        if len(metadata) != 1:
            raise AssertionError("source distribution metadata is missing or duplicated")
        boundary = _inspect_metadata(read(metadata[0]))
        _inspect_imports(names, read, sdist=True)
        _inspect_namespaces(names, sdist=True)
        return boundary


def _venv_python(root: Path) -> Path:
    return root / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def _venv_streamt(root: Path) -> Path:
    return root / ("Scripts/streamt.exe" if os.name == "nt" else "bin/streamt")


def _run_checked(command: Sequence[str], *, cwd: Path) -> None:
    completed = subprocess.run(
        list(command),
        cwd=cwd,
        capture_output=True,
        timeout=300,
        check=False,
    )
    if len(completed.stdout) > _MAX_OUTPUT or len(completed.stderr) > _MAX_OUTPUT:
        raise AssertionError("package setup output exceeded its bound")
    if completed.returncode != 0:
        raise AssertionError("package setup command failed")


def _create_environment(root: Path) -> Path:
    venv.EnvBuilder(with_pip=True, clear=False, symlinks=False).create(root)
    python = _venv_python(root)
    _run_checked(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--upgrade",
            f"pip=={_PIP_VERSION}",
        ],
        cwd=root.parent,
    )
    return python


def _install_wheel(python: Path, wheel: Path, *, cwd: Path) -> None:
    _run_checked(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--progress-bar",
            "off",
            str(wheel),
        ],
        cwd=cwd,
    )


def _install_runtime_dependencies(python: Path, *, cwd: Path) -> None:
    _run_checked(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--progress-bar",
            "off",
            *_RUNTIME_REQUIREMENTS,
        ],
        cwd=cwd,
    )


def _assert_distribution_absent(python: Path, *, cwd: Path) -> None:
    probe = (
        "import importlib.metadata as metadata\n"
        "import importlib.util\n"
        "try:\n"
        "    metadata.distribution('streamt')\n"
        "except metadata.PackageNotFoundError:\n"
        "    pass\n"
        "else:\n"
        "    raise AssertionError('source environment contains the streamt distribution')\n"
        "assert importlib.util.find_spec('streamt') is None\n"
    )
    _run_checked([str(python), "-I", "-c", probe], cwd=cwd)


def _install_sdist(python: Path, sdist: Path, *, cwd: Path) -> None:
    _run_checked(
        [str(python), "-m", "pip", "install", "--disable-pip-version-check", _BUILD_BACKEND],
        cwd=cwd,
    )
    _run_checked(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--progress-bar",
            "off",
            "--no-deps",
            "--no-build-isolation",
            str(sdist),
        ],
        cwd=cwd,
    )


def _assert_direct_sdist_origin(python: Path, sdist: Path, *, cwd: Path) -> None:
    digest = hashlib.sha256(sdist.read_bytes()).hexdigest()
    expected = {
        "archive_info": {
            "hash": f"sha256={digest}",
            "hashes": {"sha256": digest},
        },
        "url": sdist.resolve().as_uri(),
    }
    probe = (
        "import importlib.metadata as metadata\n"
        "import json\n"
        "import sys\n"
        "actual = json.loads(metadata.distribution('streamt').read_text('direct_url.json'))\n"
        "expected = json.loads(sys.argv[1])\n"
        "assert actual == expected, (actual, expected)\n"
    )
    _run_checked(
        [str(python), "-I", "-c", probe, json.dumps(expected, sort_keys=True)],
        cwd=cwd,
    )


def _pip_check(python: Path, *, cwd: Path) -> None:
    _run_checked(
        [str(python), "-m", "pip", "check", "--disable-pip-version-check"],
        cwd=cwd,
    )


def _write_projects(root: Path) -> tuple[Path, Path, Path]:
    project_root = root / _PATH_SECRET
    success = project_root / "success"
    failure = project_root / "failure"
    output = root / _OUTPUT_PATH_SECRET / "topics.yaml"
    success.mkdir(parents=True)
    failure.mkdir(parents=True)
    output.parent.mkdir(parents=True)
    common: dict[str, object] = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": _PROJECT, "version": "1.0.0"},
        "runtime": {
            "kafka": {
                "bootstrap_servers": "${STREAMT_PACKAGE_ENV_SECRET}:9092",
                "security_protocol": "SASL_PLAINTEXT",
                "sasl_mechanism": "PLAIN",
                "sasl_username": "parity-user",
                "sasl_password": _RUNTIME_SECRET,
            },
            "schema_registry": {
                "url": "https://schema-registry.invalid",
                "password": "${STREAMT_PACKAGE_SCHEMA_SECRET}",
            },
            "flink": {
                "default": "parity",
                "clusters": {
                    "parity": {
                        "rest_url": "https://flink.invalid",
                        "api_key": "${STREAMT_PACKAGE_FLINK_SECRET}",
                    }
                },
            },
            "connect": {
                "default": "parity",
                "clusters": {
                    "parity": {
                        "rest_url": "https://connect.invalid",
                        "password": "${STREAMT_PACKAGE_CONNECT_SECRET}",
                    }
                },
            },
            "conduktor": {
                "gateway": {
                    "admin_url": "https://gateway.invalid",
                    "password": "${STREAMT_PACKAGE_GATEWAY_SECRET}",
                }
            },
        },
        "deployment_state": {
            "backend": "postgres",
            "namespace": "package-parity",
            "postgres": {"dsn_env": "STREAMT_PACKAGE_STATE_DSN"},
        },
        "connections": {
            "unused": {
                "type": "opaque",
                "config": {"credential": _CONNECTION_SECRET},
            }
        },
        "sources": [{"name": "input", "topic": "upstream-input"}],
        "models": [
            {
                "name": _DIRECT_OWNER,
                "materialized": "topic",
                "sql": f"SELECT '{_SQL_SECRET}' AS marker FROM {{{{ source('input') }}}}",
                "tags": [_TAG_SECRET],
                "topic": {
                    "name": _DIRECT_TOPIC,
                    "partitions": 3,
                    "replication_factor": 2,
                    "config": {"cleanup.policy": "compact", "min.insync.replicas": 2},
                },
            },
            {
                "name": _HASHED_OWNER,
                "materialized": "topic",
                "topic": {
                    "name": _HASHED_TOPIC,
                    "partitions": 1,
                    "replication_factor": 1,
                    "config": {"unclean.leader.election.enable": False},
                },
            },
            {
                "name": _EXTERNAL_OWNER,
                "materialized": "topic",
                "ownership": {"mode": "external"},
                "topic": {"name": _EXTERNAL_TOPIC},
            },
        ],
    }
    (success / "stream_project.yml").write_text(
        json.dumps(common, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    rejected = json.loads(json.dumps(common))
    rejected["models"][0]["topic"]["config"] = {  # type: ignore[index]
        "sasl.jaas.config": _REJECTED_SECRET
    }
    (failure / "stream_project.yml").write_text(
        json.dumps(rejected, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return success, failure, output


def _write_guard(root: Path) -> Path:
    guard = root / "guard"
    guard.mkdir()
    (guard / "sitecustomize.py").write_text(_GUARD_SOURCE, encoding="utf-8")
    return guard


def _environment(target: _Target, marker: Path, source_root: Path) -> dict[str, str]:
    environment = os.environ.copy()
    environment.pop("PYTHONHOME", None)
    environment.pop("STREAMT_ENV", None)
    environment["PYTHONNOUSERSITE"] = "1"
    environment["PYTHONPATH"] = target.pythonpath
    environment["STREAMT_STRIMZI_GUARD_MARKER"] = str(marker)
    environment["STREAMT_PACKAGE_ENV_SECRET"] = _ENV_SECRET
    environment["STREAMT_PACKAGE_SCHEMA_SECRET"] = _SCHEMA_REGISTRY_SECRET
    environment["STREAMT_PACKAGE_FLINK_SECRET"] = _FLINK_SECRET
    environment["STREAMT_PACKAGE_CONNECT_SECRET"] = _CONNECT_SECRET
    environment["STREAMT_PACKAGE_GATEWAY_SECRET"] = _GATEWAY_SECRET
    environment["STREAMT_PACKAGE_STATE_DSN"] = _STATE_SECRET
    environment["STREAMT_PARITY_CHECKOUT"] = str(source_root)
    environment["STREAMT_PARITY_MODE"] = target.mode
    environment["NO_COLOR"] = "1"
    environment["COLUMNS"] = "160"
    return environment


def _invoke(
    target: _Target,
    arguments: Sequence[str],
    *,
    cwd: Path,
    guard_markers: Path,
    source_root: Path,
) -> _Completed:
    marker = guard_markers / f"{target.name}-{hashlib.sha256(repr(tuple(arguments)).encode()).hexdigest()}"
    marker.unlink(missing_ok=True)
    completed = subprocess.run(
        [*target.command, *arguments],
        cwd=cwd,
        env=_environment(target, marker, source_root),
        capture_output=True,
        timeout=60,
        check=False,
    )
    if marker.read_bytes() != b"active\n":
        raise AssertionError("target process guard was not activated")
    if len(completed.stdout) > _MAX_OUTPUT or len(completed.stderr) > _MAX_OUTPUT:
        raise AssertionError("target output exceeded its bound")
    return _Completed(completed.returncode, completed.stdout, completed.stderr)


def _probe_resource(
    target: _Target,
    *,
    cwd: Path,
    guard_markers: Path,
    source_root: Path,
) -> bytes:
    marker = guard_markers / f"{target.name}-resource"
    marker.unlink(missing_ok=True)
    completed = subprocess.run(
        [str(target.python), "-c", _RESOURCE_PROBE],
        cwd=cwd,
        env=_environment(target, marker, source_root),
        capture_output=True,
        timeout=30,
        check=False,
    )
    if completed.returncode != 0 or completed.stderr or marker.read_bytes() != b"active\n":
        raise AssertionError("target resource probe failed")
    return completed.stdout


def _probe_denials(
    target: _Target,
    *,
    cwd: Path,
    guard_markers: Path,
    source_root: Path,
) -> None:
    for name, operation in _DENIAL_PROBES.items():
        marker = guard_markers / f"{target.name}-deny-{name}"
        marker.unlink(missing_ok=True)
        probe = (
            "try:\n"
            f"    exec({operation!r})\n"
            "except RuntimeError as error:\n"
            "    assert str(error).startswith('offline Strimzi package guard denied')\n"
            "else:\n"
            "    raise AssertionError('guard permitted a forbidden operation')\n"
        )
        completed = subprocess.run(
            [str(target.python), "-c", probe],
            cwd=cwd,
            env=_environment(target, marker, source_root),
            capture_output=True,
            timeout=30,
            check=False,
        )
        if completed.returncode != 0 or completed.stdout or completed.stderr:
            raise AssertionError("target process denial probe failed")
        if marker.read_bytes() != b"active\n":
            raise AssertionError("target process denial guard was not activated")


def _arguments(project: Path, *, json_mode: bool = False) -> list[str]:
    prefix = ["--output", "json"] if json_mode else []
    return [
        *prefix,
        "export",
        "strimzi",
        "--namespace",
        _NAMESPACE,
        "--cluster-name",
        _CLUSTER,
        "--project-dir",
        str(project),
    ]


def _failure_arguments(project: Path, output: Path) -> list[str]:
    return [*_arguments(project, json_mode=True), "--output-file", str(output)]


def _file_arguments(project: Path, output: Path) -> list[str]:
    return ["--quiet", *_arguments(project), "--output-file", str(output)]


def _primitive_failure_arguments(project: Path) -> list[str]:
    return [
        "--output",
        "json",
        "export",
        "strimzi",
        "--cluster-name",
        _CLUSTER,
        "--project-dir",
        str(project),
    ]


def _all_surfaces(results: Sequence[_Completed]) -> bytes:
    return b"\n".join(
        [piece for result in results for piece in (result.stdout, result.stderr, repr(result).encode())]
    )


def _assert_secret_neutral(results: Sequence[_Completed]) -> None:
    surfaces = _all_surfaces(results)
    for secret in _CONFIDENTIAL:
        if secret.encode() in surfaces:
            raise AssertionError("confidential sentinel reached a target surface")


def _assert_artifact_secret_neutral(artifacts: Sequence[bytes]) -> None:
    combined = b"\n".join(artifacts)
    for secret in _CONFIDENTIAL:
        if secret.encode() in combined:
            raise AssertionError("confidential sentinel reached an artifact")


def _collect_exact_paths(value: object, needle: str, path: str = "") -> set[str]:
    found: set[str] = set()
    if value == needle:
        found.add(path)
    if isinstance(value, dict):
        for key, item in value.items():
            child = f"{path}/{key}" if path else str(key)
            found.update(_collect_exact_paths(item, needle, child))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            child = f"{path}/{index}" if path else str(index)
            found.update(_collect_exact_paths(item, needle, child))
    return found


def _assert_public_allowlist(text: _Completed, structured: _Completed) -> None:
    payload = json.loads(structured.stdout)
    documents = payload["data"]["documents"]
    if not isinstance(documents, list) or len(documents) != 2:
        raise AssertionError("success did not contain both identity fixtures")
    by_topic = {document["spec"]["topicName"]: (index, document) for index, document in enumerate(documents)}
    if set(by_topic) != {_DIRECT_TOPIC, _HASHED_TOPIC}:
        raise AssertionError("success topic identities changed")
    direct_index = by_topic[_DIRECT_TOPIC][0]
    hashed_index = by_topic[_HASHED_TOPIC][0]
    allowed = {
        _PROJECT: {
            f"data/documents/{direct_index}/metadata/annotations/streamt.dev/project",
            f"data/documents/{hashed_index}/metadata/annotations/streamt.dev/project",
        },
        _DIRECT_OWNER: {
            f"data/documents/{direct_index}/metadata/annotations/streamt.dev/owner-name"
        },
        _HASHED_OWNER: {
            f"data/documents/{hashed_index}/metadata/annotations/streamt.dev/owner-name"
        },
        _DIRECT_TOPIC: {
            f"data/documents/{direct_index}/metadata/name",
            f"data/documents/{direct_index}/spec/topicName",
        },
        _HASHED_TOPIC: {f"data/documents/{hashed_index}/spec/topicName"},
    }
    for identity, expected in allowed.items():
        if _collect_exact_paths(payload, identity) != expected:
            raise AssertionError("public identity escaped its JSON allowlist")
    combined = text.stdout + text.stderr + structured.stdout + structured.stderr
    for omitted in (_EXTERNAL_OWNER, _EXTERNAL_TOPIC):
        if omitted.encode() in combined:
            raise AssertionError("omitted external identity reached output")
    if text.stdout.count(_PROJECT.encode()) != 2:
        raise AssertionError("project identity escaped its YAML allowlist")
    expected_counts = {
        _DIRECT_OWNER: 1,
        _HASHED_OWNER: 1,
        _DIRECT_TOPIC: 2,
        _HASHED_TOPIC: 1,
    }
    for identity, count in expected_counts.items():
        if text.stdout.count(identity.encode()) != count:
            raise AssertionError("public identity escaped its YAML allowlist")


def _assert_target_results(
    results: dict[str, _TargetResults],
) -> None:
    text_values = {value.text for value in results.values()}
    json_values = {value.structured for value in results.values()}
    file_values = {(value.file_mode, value.file_bytes) for value in results.values()}
    failure_values = {value.mapper_failure for value in results.values()}
    primitive_values = {value.primitive_failure for value in results.values()}
    if any(
        len(values) != 1
        for values in (
            text_values,
            json_values,
            file_values,
            failure_values,
            primitive_values,
        )
    ):
        raise AssertionError("source, wheel, and sdist command surfaces differ")
    selected = next(iter(results.values()))
    text = selected.text
    structured = selected.structured
    file_mode = selected.file_mode
    failure = selected.mapper_failure
    primitive = selected.primitive_failure
    if (
        text.returncode != 0
        or structured.returncode != 0
        or file_mode.returncode != 0
        or failure.returncode != 1
        or primitive.returncode != 1
    ):
        raise AssertionError("target command return code changed")
    if not text.stdout.startswith(b"---\n") or text.stderr.count(b"WARNING") != 2:
        raise AssertionError("canonical text or warning output changed")
    if hashlib.sha256(text.stdout).hexdigest() != _EXPECTED_TEXT_SHA256:
        raise AssertionError("cross-version canonical YAML bytes changed")
    if file_mode.stdout or file_mode.stderr or selected.file_bytes != text.stdout:
        raise AssertionError("quiet file artifact bytes changed")
    if structured.stderr:
        raise AssertionError("JSON success wrote stderr")
    payload = json.loads(structured.stdout)
    if payload["status"] != "ok" or payload["command"] != "export strimzi":
        raise AssertionError("JSON success envelope changed")
    if payload["data"]["target_release"] != "1.2.0":
        raise AssertionError("Strimzi target release changed")
    if payload["data"]["api_version"] != "kafka.strimzi.io/v1":
        raise AssertionError("Strimzi API version changed")
    if payload["data"]["kind"] != "KafkaTopic":
        raise AssertionError("Strimzi resource kind changed")
    if hashlib.sha256(structured.stdout).hexdigest() != _EXPECTED_JSON_SHA256:
        raise AssertionError("cross-version JSON envelope bytes changed")
    if payload["data"]["counts"] != {
        "emitted_topics": 2,
        "external_topics_omitted": 1,
        "other_artifacts_omitted": 1,
    }:
        raise AssertionError("export counts changed")
    expected_warnings = [
        {
            "code": "W121_STRIMZI_ARTIFACTS_OMITTED",
            "message": "Non-topic artifacts omitted from Strimzi export",
            "location": (
                "artifacts/omitted/schemas=0,flink_jobs=1,test_jobs=0,connectors=0,"
                "connector_removals=0,gateway_rules=0,gateway_rule_removals=0"
            ),
        },
        {
            "code": "W120_STRIMZI_EXTERNAL_TOPIC_OMITTED",
            "message": "External topic artifact omitted from Strimzi export",
            "location": "artifacts/topics/1/ownership",
        },
    ]
    if payload["warnings"] != expected_warnings:
        raise AssertionError("structured warnings changed")
    if text.stderr != (
        b"WARNING: Non-topic artifacts omitted from Strimzi export\n"
        b"WARNING: External topic artifact omitted from Strimzi export\n"
    ):
        raise AssertionError("text warning surface changed")
    failed = json.loads(failure.stdout)
    if failure.stderr != b"ERROR: Strimzi export failed safely\n":
        raise AssertionError("failure stderr changed")
    if failed != {
        "status": "error",
        "command": "export strimzi",
        "data": {},
        "errors": [
            {
                "code": "E509_STRIMZI_INVALID",
                "message": "Strimzi export failed safely",
                "location": "artifacts/topics",
            }
        ],
        "warnings": [],
    }:
        raise AssertionError("failure envelope changed")
    primitive_payload = json.loads(primitive.stdout)
    if primitive.stderr != b"ERROR: Strimzi export failed safely\n" or primitive_payload != {
        "status": "error",
        "command": "export strimzi",
        "data": {},
        "errors": [
            {
                "code": "E509_STRIMZI_INVALID",
                "message": "Strimzi export failed safely",
                "location": "target.namespace",
            }
        ],
        "warnings": [],
    }:
        raise AssertionError("primitive failure envelope changed")
    all_results = [
        item
        for value in results.values()
        for item in (
            value.text,
            value.structured,
            value.file_mode,
            value.mapper_failure,
            value.primitive_failure,
        )
    ]
    _assert_secret_neutral(all_results)
    _assert_artifact_secret_neutral([value.file_bytes for value in results.values()])
    _assert_public_allowlist(text, structured)


def _targets(
    root: Path,
    *,
    wheel: Path,
    sdist: Path,
    source_root: Path,
    guard: Path,
) -> tuple[_Target, ...]:
    source_env = root / "source-env"
    wheel_env = root / "wheel-env"
    sdist_env = root / "sdist-env"
    source_python = _create_environment(source_env)
    wheel_python = _create_environment(wheel_env)
    sdist_python = _create_environment(sdist_env)
    _install_runtime_dependencies(source_python, cwd=root)
    _assert_distribution_absent(source_python, cwd=root)
    _install_wheel(wheel_python, wheel, cwd=root)
    _install_runtime_dependencies(sdist_python, cwd=root)
    _install_sdist(sdist_python, sdist, cwd=root)
    _assert_direct_sdist_origin(sdist_python, sdist, cwd=root)
    for python in (source_python, wheel_python, sdist_python):
        _pip_check(python, cwd=root)
    source_path = os.pathsep.join((str(guard), str(source_root / "src")))
    return (
        _Target(
            "source",
            source_python,
            (str(source_python), "-c", "from streamt.cli import main; main()"),
            source_path,
            "source",
        ),
        _Target(
            "wheel",
            wheel_python,
            (str(_venv_streamt(wheel_env)),),
            str(guard),
            "installed",
        ),
        _Target(
            "sdist",
            sdist_python,
            (str(_venv_streamt(sdist_env)),),
            str(guard),
            "installed",
        ),
    )


def run_parity(*, wheel: Path, sdist: Path, source_root: Path) -> None:
    wheel = wheel.resolve(strict=True)
    sdist = sdist.resolve(strict=True)
    source_root = source_root.resolve(strict=True)
    if wheel.suffix != ".whl" or not sdist.name.endswith(".tar.gz"):
        raise AssertionError("exact wheel and source distribution are required")
    wheel_metadata = _inspect_wheel(wheel)
    sdist_metadata = _inspect_sdist(sdist)
    if wheel_metadata != sdist_metadata:
        raise AssertionError("wheel and source distribution metadata differ")
    temp_parent = Path(os.environ.get("RUNNER_TEMP", tempfile.gettempdir())).resolve()
    with tempfile.TemporaryDirectory(prefix="streamt-strimzi-package-", dir=temp_parent) as raw:
        root = Path(raw)
        work = root / "outside-checkout"
        markers = root / "guard-markers"
        work.mkdir()
        markers.mkdir()
        guard = _write_guard(root)
        success, failure, output = _write_projects(root)
        targets = _targets(
            root,
            wheel=wheel,
            sdist=sdist,
            source_root=source_root,
            guard=guard,
        )
        results: dict[str, _TargetResults] = {}
        digests: set[bytes] = set()
        for target in targets:
            _probe_denials(
                target,
                cwd=work,
                guard_markers=markers,
                source_root=source_root,
            )
            text = _invoke(
                target,
                _arguments(success),
                cwd=work,
                guard_markers=markers,
                source_root=source_root,
            )
            structured = _invoke(
                target,
                _arguments(success, json_mode=True),
                cwd=work,
                guard_markers=markers,
                source_root=source_root,
            )
            target_output = output.with_name(f"{target.name}-topics.yaml")
            target_output.unlink(missing_ok=True)
            file_mode = _invoke(
                target,
                _file_arguments(success, target_output),
                cwd=work,
                guard_markers=markers,
                source_root=source_root,
            )
            file_bytes = target_output.read_bytes()
            failed = _invoke(
                target,
                _failure_arguments(failure, output),
                cwd=work,
                guard_markers=markers,
                source_root=source_root,
            )
            if output.exists():
                raise AssertionError("failed export created its output target")
            primitive = _invoke(
                target,
                _primitive_failure_arguments(success),
                cwd=work,
                guard_markers=markers,
                source_root=source_root,
            )
            results[target.name] = _TargetResults(
                text=text,
                structured=structured,
                file_mode=file_mode,
                file_bytes=file_bytes,
                mapper_failure=failed,
                primitive_failure=primitive,
            )
            digests.add(
                _probe_resource(
                    target,
                    cwd=work,
                    guard_markers=markers,
                    source_root=source_root,
                )
            )
        _assert_target_results(results)
        if digests != {f"{_SOURCE_DIGEST}\n".encode()}:
            raise AssertionError("source, wheel, and sdist CRD digests differ")


def _self_test() -> None:
    exact_metadata = "\n".join(sorted(_EXPECTED_METADATA)).encode()
    _inspect_metadata(exact_metadata)
    for rejected in (
        b"Requires-Dist: kubernetes>=35\n",
        b"Provides-Extra: strimzi\n",
    ):
        try:
            _assert_no_target_metadata(rejected)
        except AssertionError:
            pass
        else:
            raise AssertionError("metadata guard accepted a target SDK")
    try:
        _inspect_metadata(exact_metadata + b"\nRequires-Dist: another-package\n")
    except AssertionError:
        pass
    else:
        raise AssertionError("metadata guard accepted a new runtime dependency")
    expected_runtime = {
        f"Requires-Dist: {requirement}" for requirement in _RUNTIME_REQUIREMENTS
    }
    actual_runtime = {
        line
        for line in _EXPECTED_METADATA
        if line.startswith("Requires-Dist:") and "extra ==" not in line
    }
    if actual_runtime != expected_runtime:
        raise AssertionError("runtime installation boundary differs from metadata")

    def reject_import(
        source: bytes,
        path: str = "streamt/package_probe.py",
    ) -> None:
        names = {path}
        try:
            _inspect_imports(names, lambda _name: source, sdist=False)
        except AssertionError:
            pass
        else:
            raise AssertionError("import scanner accepted a target SDK path")

    for source in (
        b"__import__('kubernetes')\n",
        b"import importlib as loader\nloader.import_module('openshift')\n",
        b"from importlib import import_module as load\nload('strimzi')\n",
        b"name = 'pyhelm'\n__import__(name)\n",
        b"load = __import__\nload('kubernetes_asyncio')\n",
        b"from builtins import __import__ as load\nload('openshift')\n",
        b"import builtins\nbuiltins.__import__('kubernetes')\n",
        b"import builtins as b\nb.__import__('openshift')\n",
        b"import builtins as b\ngetattr(b, '__import__')('strimzi')\n",
        b"import builtins\nb = builtins\nb.__import__('kubernetes')\n",
        b"import builtins\nb = builtins\nc = b\nc.__import__('strimzi')\n",
        b"import importlib\nil = importlib\nil.import_module('openshift')\n",
        b"name = 'kubernetes'\n__import__(name)\nname = 'safe'\n",
        b"import importlib\nimportlib.import_module('kuber' + 'netes')\n",
        b"import importlib\npart = 'strimzi'\nimportlib.import_module(f'{part}')\n",
        b"import importlib\ndef load(name):\n    return importlib.import_module(name)\n",
    ):
        reject_import(source)
    reject_import(
        b"from importlib import import_module\nimport_module(command.module)\n",
        "streamt/cli/__init__.py",
    )
    _inspect_imports(
        {"streamt/package_probe.py"},
        lambda _name: b"import pathlib\n",
        sdist=False,
    )

    for names in (
        {"streamt-1.data/purelib/kubernetes/client.py"},
        {"kubernetes-35.0.dist-info/METADATA"},
        {"pyhelm-2.0.egg-info/PKG-INFO"},
        {"strimzi.egg-info"},
    ):
        try:
            _inspect_namespaces(names, sdist=False)
        except AssertionError:
            pass
        else:
            raise AssertionError("namespace scanner accepted a target SDK path")
    exact_resources = {
        f"{_RESOURCE_PREFIX}/{resource}" for resource in _RESOURCE_EXPECTATIONS
    }
    _inspect_resource_subtree(exact_resources, sdist=False)
    try:
        _inspect_resource_subtree(
            exact_resources | {f"{_RESOURCE_PREFIX}/unexpected.txt"},
            sdist=False,
        )
    except AssertionError:
        pass
    else:
        raise AssertionError("resource scanner accepted an extra schema file")
    clean = _Completed(0, b"public", b"")
    _assert_secret_neutral([clean])
    contaminated = _Completed(0, _RUNTIME_SECRET.encode(), b"")
    try:
        _assert_secret_neutral([contaminated])
    except AssertionError:
        pass
    else:
        raise AssertionError("secrecy guard accepted a confidential sentinel")
    try:
        _assert_artifact_secret_neutral([_SQL_SECRET.encode()])
    except AssertionError:
        pass
    else:
        raise AssertionError("artifact scanner accepted a confidential sentinel")
    if "--no-build-isolation" not in _install_sdist.__code__.co_consts:
        # The real check below inspects source because CPython nests list constants.
        source = Path(__file__).read_text(encoding="utf-8")
        if '"--no-build-isolation"' not in source:
            raise AssertionError("direct sdist install lost its isolation boundary")
    if any(
        expected not in _GUARD_SOURCE
        for expected in (
            "subprocess.Popen",
            "socket.__new__",
            "os.fork",
            '"confluent_kafka"',
            "streamt.cli.helpers",
            "streamt.integrations.openlineage.transport",
        )
    ):
        raise AssertionError("target process guard is incomplete")


def _parse_args(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--sdist", type=Path)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--self-test", action="store_true")
    parsed = parser.parse_args(arguments)
    if not parsed.self_test and any(
        value is None for value in (parsed.wheel, parsed.sdist, parsed.source_root)
    ):
        parser.error("--wheel, --sdist, and --source-root are required")
    return parsed


def main(arguments: Sequence[str] | None = None) -> int:
    parsed = _parse_args(arguments)
    try:
        if parsed.self_test:
            _self_test()
        else:
            run_parity(
                wheel=parsed.wheel,
                sdist=parsed.sdist,
                source_root=parsed.source_root,
            )
    except Exception:
        print("Strimzi package parity failed safely", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
