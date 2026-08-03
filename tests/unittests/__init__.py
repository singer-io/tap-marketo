"""Test package initialization for tap_marketo unittests."""

import pendulum


if not hasattr(pendulum, "utcnow"):
    def _utcnow():
        return pendulum.now("UTC")

    pendulum.utcnow = _utcnow
