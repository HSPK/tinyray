"""Three exceptions, because only "did not arrive" is safe to retry blindly."""

from __future__ import annotations


class TinyrayError(Exception):
    """Base for everything tinyray raises, so one except clause catches it."""


class Unreachable(TinyrayError):
    """Never arrived. Retry if the operation can be repeated."""


class Fenced(TinyrayError):
    """Arrived, but a later tenure holds that seat. Look the address up again."""


class RemoteError(TinyrayError):
    """Arrived and the method raised. tinyray never retries this on your behalf."""

    def __init__(self, type_name: str, message: str, traceback: str = ""):
        super().__init__(f"{type_name}: {message}")
        # The original class is not reconstructed: that needs both sides to
        # define it, which is a hidden coupling.
        self.type, self.message, self.traceback = type_name, message, traceback


class Stale(TinyrayError):
    """Out of touch with the registry, so the roster cannot be trusted."""


class SeatTaken(TinyrayError):
    """Asked for a seat exclusively and somebody live already holds it."""


class NotFound(LookupError):
    """Nobody matched. Failure is explicit; there is no None."""


class PolicyError(ValueError):
    """The policy, seat or size asked for does not make sense together."""


class OversizeWarning(UserWarning):
    """A payload past the size the control plane is meant for; see SOFT_BODY.

    Silence it the usual way:

        warnings.filterwarnings("ignore", category=tinyray.OversizeWarning)
    """
