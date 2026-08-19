"""Three exceptions, because the caller needs to tell three situations apart.

Only "did not arrive" is safe to retry blindly. Whether a business failure can
be repeated is something only the application knows, so tinyray never retries
one on its behalf.
"""

from __future__ import annotations


class TinyrayError(Exception):
    pass


class Unreachable(TinyrayError):
    """It never arrived. Retry if the operation can be repeated."""


class Fenced(TinyrayError):
    """It arrived, but a later tenure holds that seat now. Look the address up
    again -- a stale address is normal, not exceptional."""


class RemoteError(TinyrayError):
    """It arrived and the method itself raised.

    The original exception class is not reconstructed: doing so would require
    both sides to define it, which is a hidden coupling.
    """

    def __init__(self, type_name: str, message: str, traceback: str = ""):
        super().__init__(f"{type_name}: {message}")
        self.type = type_name
        self.message = message
        self.traceback = traceback


class NotFound(LookupError):
    """Nobody in the pool matched. Failure is explicit; there is no None."""


class PolicyError(ValueError):
    pass
