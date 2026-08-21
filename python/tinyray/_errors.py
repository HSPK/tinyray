"""Three exceptions, because only "did not arrive" is safe to retry blindly."""

from __future__ import annotations


class TinyrayError(Exception):
    """Base for everything tinyray raises, so one except clause catches it."""


class Unreachable(TinyrayError):
    """The call did not come back. Whether it ran is the subclass's business.

    Kept as the base so `except Unreachable` still catches both, but code that
    retries should catch the subclass instead: one of them is safe to repeat
    as-is and the other is not, and for a long time they were the same class
    with a docstring that told you to retry.
    """


class NotDelivered(Unreachable):
    """It never reached the far side, so the method certainly did not run.

    Safe to retry as it stands -- no request id, no idempotency key, nothing to
    reconcile. A refused connection, a name that will not resolve and a callee
    that answered "I am full" all land here.
    """


class OutcomeUnknown(Unreachable):
    """It may have run. Nobody on this side can tell.

    A read that timed out, a connection that broke mid-exchange, a callee that
    died partway through. Retrying means possibly doing it twice, so carry the
    same request id or make the operation idempotent. This is the only case
    that needs one.
    """


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
