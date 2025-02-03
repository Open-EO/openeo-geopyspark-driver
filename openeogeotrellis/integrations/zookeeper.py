from __future__ import annotations

from time import sleep
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Union, Any, Callable, Optional
    from kazoo.protocol.states import ZnodeStat

    # Helper type which corresponds to what Zookeeper for callbacks as used by watches
    ZEvent = tuple[bytes, ZnodeStat]
    ZCallback = Callable[[ZEvent], None]

from kazoo.client import KazooClient
import kazoo.exceptions
from kazoo.retry import KazooRetry
from reretry import retry


class ZookeeperClient:
    _KazooClient = KazooClient

    def __init__(
        self,
        hosts: str,
        logger: Any = None,
        attempt_timeout: float = 1.0,
        total_timeout: float = 120.0,
        tries: int = 10,
        delay: float = 0.4,
        backoff: float = 1.7,
    ):
        """
        Create a client to communicate with Zookeeper. This is meant as a long-term client and to be re-used if
        multiple zookeeper requests are to be made as part of an application lifetime.

        The resulting object is to be used similar as the KazooClient:
          > ZookeeperClient("zk1.local:2181,zk2.local:2181").get("/path/to/znode")

        attempt_timeout is how much time budget there is to perform 1 attempt at performing an operation (e.g. creating
        a connection). The default of 1 second is even sufficient when working over VPN and port-forwarding.

        total_timeout is the total time budget when things go wrong to get a working connection against the cluster.
        This is in seconds and the default budget is 2 minutes.

        tries is the total amount of connection attempts that one should try in case there are issues.

        delay is how long to wait in seconds before the next attempt

        backoff is a multiplier for subsequent delays

        Note that the default will allow for 10 attempts in 2 minutes spread in time.

        attempt delay_sec total_time(this attempt)	total_time(sum)
        1       0.4	           1.4	                    1.4
        2     	0.68	       1.68	                    3.08
        3   	1.156	       2.156	                5.236
        4       1.9652	       2.9652	                8.2012
        5   	3.34084	       4.34084	               12.54204
        6    	5.679428	   6.679428	               19.221468
        7    	9.6550276	  10.6550276	           29.8764956
        8      16.4135469	  17.41354692	           47.29004252
        9      27.9030297	  28.903029764	           76.193072284
        10     47.4351505	  48.4351505988	          124.6282228828

        """
        kz_retry = KazooRetry(max_tries=tries, delay=delay, backoff=backoff)
        self._logger = logger
        self._zkhosts = hosts
        self._zk = self._KazooClient(
            hosts=hosts,
            connection_retry=kz_retry,
            command_retry=kz_retry,
            timeout=attempt_timeout,
            logger=logger,
            read_only=True,
        )
        self._zk.start(timeout=total_timeout)

        # Create KazooClient methods:
        for kazoo_method in ["get", "get_children", "delete", "create", "set"]:
            setattr(self, kazoo_method, self._make_retrying_kazooclient_method(kazoo_method))

    # We add the kazoo methods just to avoid IDEs getting confused and giving unresolved attribute warnings
    # but these are not the actual methods as those will be created upon initialization.
    get = KazooClient.get
    get_children = KazooClient.get_children
    delete = KazooClient.delete
    create = KazooClient.create
    set = KazooClient.set

    def __del__(self):
        """
        The lifetime of the connection is tied to the object. So if the object goes out of scope the connection will be
        cleaned up as well.
        """
        # noinspection PyBroadException
        try:
            self._zk.stop()
        except Exception:
            pass  # it will stop eventually
        finally:
            # noinspection PyBroadException
            try:
                self._zk.close()
            except Exception:
                pass  # As long as it is used as documented this is fine, risk of connection leakage is very low

    class NoActiveConnectionException(Exception):
        """This is a helper exception to signal that there is no Active connection at this time."""

    def _wait_100ms_for_zookeeper_connection(self) -> None:
        """
        If no active zookeeper connection wait 100 milliseconds and see if active yet.
        The checking whether the connection is active is a cheap check, so it can be retried aggressively.

        raise ZookeeperClient.NotActiveYetException if at the end no active connection is available.
        """
        if not self._zk.connected:  # If already active do not wait
            sleep(0.1)
            if not self._zk.connected:
                if self._logger is not None:
                    self._logger.debug(f"No active connection for zookeeper client ({self._zkhosts})")
                raise ZookeeperClient.NoActiveConnectionException()

    _wait_up_to_1minute_for_zookeeper_connection = retry(
        exceptions=(NoActiveConnectionException,),
        tries=600,
        logger=None,
    )(_wait_100ms_for_zookeeper_connection)

    def _make_retrying_kazooclient_method(self, method: str, tries: int = 3):
        """
        Make a method to the ZookeeperClient which is a method that is available on KazooClient.

        The difference is that this will perform retries if exceptions are encountered. And upon exception it would also
        make sure to await an active connection before
        """

        def exception_handler(e: Exception) -> None:
            if self._logger is not None:
                self._logger.warning(f"ZookeeperClient Retry of {method} because of {type(e)}({e})")
            # KazooClient recovers automagically we can just await recovery
            self._wait_up_to_1minute_for_zookeeper_connection()

        return retry(tries=tries, exceptions=self.get_retryable_exceptions(), fail_callback=exception_handler)(
            getattr(self._zk, method)
        )

    @staticmethod
    def get_retryable_exceptions() -> tuple:
        return (
            kazoo.exceptions.SessionExpiredError,  # Can Occur for first action after connection interruption
            kazoo.exceptions.ConnectionLoss,  # If connection gets interrupted while doing an action
        )
