import pendulum


if not hasattr(pendulum, "utcnow"):
	pendulum.utcnow = lambda: pendulum.now("UTC")
