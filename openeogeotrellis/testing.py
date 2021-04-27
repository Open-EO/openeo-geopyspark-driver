"""
Reusable helpers, functions, classes, fixtures for testing purposes
"""
import uuid
from pathlib import Path
from typing import List, Tuple, Union, Dict, Iterator

import kazoo
import kazoo.exceptions


def random_name(prefix: str = "") -> str:
    """Generate random name (with given prefix)"""
    return '{p}{r}'.format(p=prefix, r=uuid.uuid4().hex[:8])


class _ZNodeStat:
    def __init__(self, version: int = 1):
        self._version = version

    @property
    def version(self):
        return self._version

    def bump_version(self):
        self._version += 1

    def __repr__(self):
        return '{c}({v})'.format(c=self.__class__.__name__, v=self.version)

    def __eq__(self, other):
        return isinstance(other, _ZNodeStat) and (self.version,) == (other.version,)


class _ZNode:
    """Internal 'znode' container"""

    def __init__(self, value: bytes = b"", children: dict = None):
        self.value = value
        self.children = children or {}
        self.stat = _ZNodeStat(version=1)

    def assert_version(self, version=-1) -> '_ZNode':
        if version not in [-1, self.stat.version]:
            raise kazoo.exceptions.BadVersionError
        return self

    def dump(self, root=None) -> Iterator[Tuple[str, bytes]]:
        """Dump as iterator of (path, value) tuples for inspection"""
        root = Path(root or "/")
        yield str(root), self.value
        for name, child in self.children.items():
            yield from child.dump(root=root / name)


class KazooClientMock:
    """Simple mock for KazooClient that stores data in memory"""

    def __init__(self, root: _ZNode = None):
        """Create client and optionally initialize state of root node (and its children)."""
        self.root = root or _ZNode()

    def start(self):
        pass

    def stop(self):
        pass

    def close(self):
        pass

    @staticmethod
    def _parse_path(path: Union[str, Path]) -> Tuple[str]:
        path = Path(path)
        assert path.is_absolute()
        return path.parts[1:]

    def _get(self, path: Union[str, Path]) -> _ZNode:
        cursor = self.root
        for part in self._parse_path(path):
            try:
                cursor = cursor.children[part]
            except KeyError:
                raise kazoo.exceptions.NoNodeError
        return cursor

    def ensure_path(self, path: Union[str, Path]) -> _ZNode:
        cursor = self.root
        for part in self._parse_path(path):
            if part not in cursor.children:
                cursor.children[part] = _ZNode()
            cursor = cursor.children[part]
        return cursor

    def get_children(self, path: Union[str, Path]) -> List[str]:
        return list(self._get(path).children.keys())

    def create(self, path: Union[str, Path], value: bytes, makepath=False):
        path = Path(path)
        if makepath:
            parent = self.ensure_path(path.parent)
        else:
            parent = self._get(path.parent)

        if path.name in parent.children:
            raise kazoo.exceptions.NodeExistsError
        parent.children[path.name] = _ZNode(value)

    def get(self, path: Union[str, Path]) -> Tuple[bytes, _ZNodeStat]:
        znode = self._get(path)
        return znode.value, znode.stat

    def set(self, path: Union[str, Path], value: bytes, version: int = -1):
        znode = self._get(path).assert_version(version)
        znode.value = value
        znode.stat.bump_version()

    def delete(self, path: Union[str, Path], version: int = -1):
        path = Path(path)
        self._get(path).assert_version(version)
        parent = self._get(path.parent)
        del parent.children[path.name]

    def dump(self) -> Dict[str, bytes]:
        """Dump ZooKeeper data for inspection"""
        return dict(self.root.dump())
