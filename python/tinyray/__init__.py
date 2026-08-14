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
    nodes,
    release,
    remote,
    shutdown,
    wait,
)
from .collective import CollectiveError, CollectiveGroup, GroupRebuilding
from .head import Head, PlacementFailed
from .launcher import ActorStartupError
from .pool import ActorPool

__version__ = "0.0.1"

__all__ = [
    "ActorDied",
    "ActorHandle",
    "ActorMethod",
    "ActorPool",
    "ActorStartupError",
    "Backpressure",
    "CollectiveError",
    "CollectiveGroup",
    "Decoder",
    # Low level
    "Frame",
    "GroupRebuilding",
    "Head",
    "Id",
    "Limits",
    "MessageTooLarge",
    "NotFound",
    "ObjectLost",
    "ObjectRef",
    "OwnerRef",
    "PlacementFailed",
    "ProtocolError",
    "RemoteCallError",
    "RemoteClass",
    # Errors
    "TinyrayError",
    "UserCodeError",
    "actors",
    "collective",
    "create_actors",
    "decode_message",
    "encode_message",
    "get",
    "get_actor",
    # API
    "init",
    "kill",
    "new_id",
    "nodes",
    "release",
    "remote",
    "serde",
    "shutdown",
    "wait",
]
