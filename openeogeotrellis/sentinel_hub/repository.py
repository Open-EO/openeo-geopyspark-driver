import json
import time
from abc import ABC, abstractmethod
from typing import List

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.recipe.counter import Counter


class Repository(ABC):
    @abstractmethod
    def init_rate_limits(self, rate_limits: List[dict], expires_within_ms: int):
        pass

    @abstractmethod
    def increment_counter(self, policy_id: str, amount: float) -> float:
        pass

    @abstractmethod
    def get_policy_types(self) -> dict:
        pass

    @abstractmethod
    def get_policy_refills(self) -> dict:
        pass

    @abstractmethod
    def is_syncer_alive(self) -> bool:
        pass

    @abstractmethod
    def signal_syncer_alive(self, expires_within_ms: int):
        pass


class RedisRepository:
    pass


class ZooKeeperRepository(Repository):
    def __init__(self, client: KazooClient, key_base: str):
        self._client = client
        self._remaining_key = f"{key_base}/remaining"
        self._refills_key = f"{key_base}/refill_ns"
        self._types_key = f"{key_base}/types"
        self._alive_key = f"{key_base}/syncer_alive"

    def _counter(self, policy_id: str) -> Counter:
        return self._client.Counter(f"{self._remaining_key}/{policy_id}", default=0.0)

    def init_rate_limits(self, rate_limits: List[dict], expires_within_ms: int):
        # TODO: mimic counter structure instead? (/openeo/rlguard/remaining/some_policy_id)
        policy_refills = {}
        policy_types = {}

        for policy in rate_limits:
            policy_refills[policy["id"]] = policy["nanos_between_refills"]
            policy_types[policy["id"]] = policy["type"]

            policy_remaining = self._counter(policy['id'])
            try:
                self._client.delete(policy_remaining.path)
            except NoNodeError:
                pass
            policy_remaining += float(policy["initial"])

        self._client.ensure_path(self._refills_key)
        self._client.set(self._refills_key, json.dumps(policy_refills).encode())

        self._client.ensure_path(self._types_key)
        self._client.set(self._types_key, json.dumps(policy_types).encode())

        self._client.ensure_path(self._alive_key)
        self.signal_syncer_alive(expires_within_ms)

    def increment_counter(self, policy_id: str, amount: float) -> float:
        counter = self._counter(policy_id)
        counter += amount
        return counter.value

    def get_policy_types(self) -> dict:
        return self._get_object(self._types_key)

    def get_policy_refills(self) -> dict:
        return self._get_object(self._refills_key)

    def _get_object(self, key: str) -> dict:
        data, _ = self._client.get(key)
        return json.loads(data.decode())

    def signal_syncer_alive(self, expires_within_ms: int):
        expires_at_ms = ZooKeeperRepository._now_ms() + expires_within_ms
        self._client.set(self._alive_key, repr(expires_at_ms).encode())

    def is_syncer_alive(self) -> bool:
        data, _ = self._client.get(self._alive_key)
        expires_at_ms = int(data.decode())

        return ZooKeeperRepository._now_ms() <= expires_at_ms

    @staticmethod
    def _now_ms():
        return int(time.time() * 1000)
