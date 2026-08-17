"""tinyray: a minimal, actor-only Ray for ML experiments.

The heavy lifting lives in the Rust core (``tinyray._tinyray``); this package is
the thin Python layer that owns everything with Python object semantics:
serialisation, the actor decorator, and the driver API.
"""

from . import collective, serde
from ._tinyray import (
    ActorDied,
    Backpressure,
    Decoder,
    Frame,
    Id,
    Limits,
    MessageTooLarge,
    NotFound,
    ObjectLost,
    OwnerRef,
    ProtocolError,
    RemoteCallError,
    TinyrayError,
    UserCodeError,
    decode_message,
    encode_message,
    new_id,
)
from .api import (
    ActorHandle,
    ActorMethod,
    ObjectRef,
    RemoteClass,
    actors,
    create_actors,
    get,
    get_actor,
    init,
    kill,
    launch_process,
    nodes,
    processes,
    release,
    remote,
    shutdown,
    stop_process,
    transport_stats,
    wait,
)
from .attach import RemoteWorker, connect, launch_workers
from .cluster import (
    Cluster,
    GroupView,
    Membership,
    RegistryClient,
    RegistryUnavailable,
    attach,
    join,
    serve_registry,
)
from .cluster import group as group
from .cluster import membership as membership
from .collective import CollectiveError, CollectiveGroup, GroupRebuilding
from .head import Head, PlacementFailed
from .launcher import ActorStartupError
from .mesh import NotLinked, group_size, link, my_group, my_rank, peer, peers, roster
from .pool import ActorPool
from .process import (
    HttpOk,
    LogMatch,
    ManagedProcess,
    PortOpen,
    ProcessAlive,
    ProcessStartupError,
)
from .registry import Registry
from .serve import Server, serve
from .worker_group import WorkerGroup, create_worker_group, torchrun_env

__version__ = "0.2.1"

__all__ = [
    "ActorDied",
    "ActorHandle",
    "ActorMethod",
    "ActorPool",
    "ActorStartupError",
    "Backpressure",
    "Cluster",
    "CollectiveError",
    "CollectiveGroup",
    "Decoder",
    # Low level
    "Frame",
    "GroupRebuilding",
    "GroupView",
    "Head",
    "HttpOk",
    "Id",
    "Limits",
    "LogMatch",
    "ManagedProcess",
    "Membership",
    "MessageTooLarge",
    "NotFound",
    "NotLinked",
    "ObjectLost",
    "ObjectRef",
    "OwnerRef",
    "PlacementFailed",
    "PortOpen",
    "ProcessAlive",
    "ProcessStartupError",
    "ProtocolError",
    "Registry",
    "RegistryClient",
    "RegistryUnavailable",
    "RemoteCallError",
    "RemoteClass",
    "RemoteWorker",
    "Server",
    # Errors
    "TinyrayError",
    "UserCodeError",
    "WorkerGroup",
    "actors",
    "attach",
    "collective",
    "connect",
    "create_actors",
    "create_worker_group",
    "decode_message",
    "encode_message",
    "get",
    "get_actor",
    "group",
    "group_size",
    # API
    "init",
    "join",
    "kill",
    "launch_process",
    "launch_workers",
    "link",
    "membership",
    "my_group",
    "my_rank",
    "new_id",
    "nodes",
    "peer",
    "peers",
    "processes",
    "release",
    "remote",
    "roster",
    "serde",
    "serve",
    "serve_registry",
    "shutdown",
    "stop_process",
    "torchrun_env",
    "transport_stats",
    "wait",
]
