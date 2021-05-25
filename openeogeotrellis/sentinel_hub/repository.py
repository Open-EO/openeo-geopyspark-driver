import json
from abc import ABC, abstractmethod
from typing import List

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.recipe.counter import Counter


class Repository(ABC):
    @abstractmethod
    def init_rate_limits(self, rate_limits: List[dict]):
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


class RedisRepository:
    pass


class ZooKeeperRepository(Repository):
    def __init__(self, client: KazooClient, key_base: str):
        self._client = client
        self._remaining_key = f"{key_base}/remaining"
        self._refills_key = f"{key_base}/refill_ns"
        self._types_key = f"{key_base}/types"

    def _counter(self, policy_id: str) -> Counter:
        return self._client.Counter(f"{self._remaining_key}/{policy_id}", default=0.0)

    def init_rate_limits(self, rate_limits: List[dict]):
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
