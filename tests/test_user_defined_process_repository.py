import collections
from typing import Iterator
from unittest import mock

import pytest
from kazoo.exceptions import NodeExistsError, NoNodeError
from openeo_driver.backend import UserDefinedProcessMetadata
from openeo_driver.errors import ProcessGraphNotFoundException

from openeogeotrellis.user_defined_process_repository import (
    InMemoryUserDefinedProcessRepository,
    ZooKeeperUserDefinedProcessRepository,
)

PG_2PLUS3 = {
    "add": {"process_id": "add", "arguments": {"x": 2, "y": 3}, "result": True}
}



class TestInMemoryUserDefinedProcessRepository:

    def test_save(self):
        udp_db = InMemoryUserDefinedProcessRepository()
        udp_db.save(user_id="john", process_id="evi", spec={"id": "evi", "process_graph": PG_2PLUS3})

        udp = udp_db.get(user_id="john", process_id="evi")
        assert isinstance(udp, UserDefinedProcessMetadata)
        assert udp.id == "evi"
        assert udp.process_graph == PG_2PLUS3

    def test_get_for_user(self):
        udp_db = InMemoryUserDefinedProcessRepository()
        udp_db.save(user_id="alice", process_id="evi1", spec={"id": "evi1", "process_graph": PG_2PLUS3})
        udp_db.save(user_id="alice", process_id="evi2", spec={"id": "evi2", "process_graph": PG_2PLUS3})
        udp_db.save(user_id="bob", process_id="evi3", spec={"id": "evi3", "process_graph": PG_2PLUS3})

        udps = udp_db.get_for_user("alice")
        assert set(u.id for u in udps) == {"evi1", "evi2"}

        udps = udp_db.get_for_user("bob")
        assert set(u.id for u in udps) == {"evi3"}

        udps = udp_db.get_for_user("carol")
        assert set(u.id for u in udps) == set()

    def test_delete(self):
        udp_db = InMemoryUserDefinedProcessRepository()
        udp_db.save(user_id="john", process_id="evi1", spec={"id": "evi1", "process_graph": PG_2PLUS3})
        udp_db.save(user_id="john", process_id="evi2", spec={"id": "evi2", "process_graph": PG_2PLUS3})

        udps = udp_db.get_for_user("john")
        assert set(u.id for u in udps) == {"evi1", "evi2"}

        udp_db.delete(user_id="john", process_id="evi1")
        udps = udp_db.get_for_user("john")
        assert set(u.id for u in udps) == {"evi2"}

        with pytest.raises(ProcessGraphNotFoundException):
            udp_db.delete(user_id="john", process_id="evi1")
        udps = udp_db.get_for_user("john")
        assert set(u.id for u in udps) == {"evi2"}


class TestZooKeeperUserDefinedProcessRepository:
    # Simple dummy for `kazoo.protocol.states.ZnodeStat`
    ZnodeStat = collections.namedtuple("_ZStat", ["version"])

    @pytest.fixture
    def udp_db(self) -> ZooKeeperUserDefinedProcessRepository:
        udp_db = ZooKeeperUserDefinedProcessRepository(hosts=["zk1.test", "zk2.test"])
        return udp_db

    @pytest.fixture
    def kazoo_client_factory(self) -> Iterator[mock.MagicMock]:
        """
        Inspectable mock for `KazooClient` used by `ZooKeeperUserDefinedProcessRepository` to
        inspect the calls made to it (e.g. check client reuse).
        """
        with mock.patch("openeogeotrellis.user_defined_process_repository.KazooClient") as KazooClient:
            yield KazooClient

    @pytest.fixture
    def zk(self, kazoo_client_factory) -> mock.MagicMock:
        """Fixture for `KazooClient instance used by ZooKeeperUserDefinedProcessRepository """
        return kazoo_client_factory()

    def _build_get_return_value(self, *, process_id="evi", pg=PG_2PLUS3):
        """Helper to build a return value for a mocked `zk.get`"""
        data = ZooKeeperUserDefinedProcessRepository._serialize(
            spec={
                "id": process_id,
                "process_graph": pg,
            }
        )
        return (data, self.ZnodeStat(version=123))

    def test_save_create(self, udp_db, zk):
        udp_db.save(user_id="john", process_id="evi", spec={"id": "evi", "process_graph": PG_2PLUS3})
        zk.create.assert_called_with("/openeo/udps/john/evi", mock.ANY, makepath=True)

    def test_save_update(self, udp_db, zk):
        zk.create.side_effect = NodeExistsError()
        zk.get.return_value = ("dummy", self.ZnodeStat(version=123))
        udp_db.save(user_id="john", process_id="evi", spec={"id": "evi", "process_graph": PG_2PLUS3})
        zk.create.assert_called_with("/openeo/udps/john/evi", mock.ANY, makepath=True)
        zk.set.assert_called_with("/openeo/udps/john/evi", mock.ANY, version=123)

    def test_get_miss(self, udp_db, zk):
        zk.get.side_effect = NoNodeError()
        res = udp_db.get(user_id="john", process_id="evi")
        assert res is None

    def test_get(self, udp_db, zk):
        zk.get.return_value = self._build_get_return_value(process_id="evi", pg=PG_2PLUS3)
        res = udp_db.get(user_id="john", process_id="evi")
        assert res == UserDefinedProcessMetadata(process_graph=PG_2PLUS3, id="evi")

    def test_delete(self, udp_db, zk):
        udp_db.save(user_id="john", process_id="evi", spec={"id": "evi", "process_graph": PG_2PLUS3})
        data = zk.create.call_args[0][1]
        zk.get.return_value = (data, self.ZnodeStat(version=123))
        res = udp_db.get(user_id="john", process_id="evi")
        assert res == UserDefinedProcessMetadata(process_graph=PG_2PLUS3, id="evi")

        udp_db.delete(user_id="john", process_id="evi")
        zk.delete.assert_called_with("/openeo/udps/john/evi")

    def test_delete_miss(self, udp_db, zk):
        zk.delete.side_effect = NoNodeError()
        with pytest.raises(ProcessGraphNotFoundException):
            udp_db.delete(user_id="john", process_id="evi1")

    @pytest.mark.parametrize(
        ["zk_client_reuse", "expected_create_call_counts", "expected_shutdown_call_counts"],
        [
            (False, [0, 1, 2, 3], [3, 3]),
            (True, [0, 1, 1, 1], [0, 1]),
        ],
    )
    def test_client_reuse(
        self, kazoo_client_factory, zk_client_reuse, expected_create_call_counts, expected_shutdown_call_counts
    ):
        udp_db = ZooKeeperUserDefinedProcessRepository(hosts=["zk1.test", "zk2.test"], zk_client_reuse=zk_client_reuse)

        # Check reuse
        create_call_counts = []
        create_call_counts.append(kazoo_client_factory.call_count)
        udp_db.save(user_id="john", process_id="aaa", spec={"id": "aaa", "process_graph": PG_2PLUS3})
        create_call_counts.append(kazoo_client_factory.call_count)
        udp_db.save(user_id="john", process_id="bbb", spec={"id": "bbb", "process_graph": PG_2PLUS3})
        create_call_counts.append(kazoo_client_factory.call_count)

        kazoo_client_factory.return_value.get.return_value = self._build_get_return_value(process_id="ccc")
        _ = udp_db.get(user_id="john", process_id="ccc")
        create_call_counts.append(kazoo_client_factory.call_count)

        assert create_call_counts == expected_create_call_counts

        # Check stop/close operations on `del`
        stop_call_counts = []
        close_call_counts = []
        stop_call_counts.append(kazoo_client_factory.return_value.stop.call_count)
        close_call_counts.append(kazoo_client_factory.return_value.close.call_count)
        del udp_db
        stop_call_counts.append(kazoo_client_factory.return_value.stop.call_count)
        close_call_counts.append(kazoo_client_factory.return_value.close.call_count)

        assert stop_call_counts == expected_shutdown_call_counts
        assert close_call_counts == expected_shutdown_call_counts
