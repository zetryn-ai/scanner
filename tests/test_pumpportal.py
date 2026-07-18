import json

from scanner.publisher import Publisher
from scanner.pumpportal import compute_backoff_delay, run_pumpportal_scanner


def test_backoff_delay_grows_and_caps():
    assert compute_backoff_delay(0) == 1.0
    assert compute_backoff_delay(1) == 2.0
    assert compute_backoff_delay(2) == 4.0
    assert compute_backoff_delay(3) == 8.0
    assert compute_backoff_delay(4) == 16.0
    assert compute_backoff_delay(5) == 30.0  # capped
    assert compute_backoff_delay(20) == 30.0  # stays capped


class _FakeWebSocket:
    """Yields a fixed list of messages, then raises a disconnect-like error."""

    def __init__(self, messages: list[str], fail_after: bool = True):
        self._messages = list(messages)
        self._fail_after = fail_after
        self.sent: list[str] = []

    async def send(self, data: str) -> None:
        self.sent.append(data)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._messages:
            return self._messages.pop(0)
        if self._fail_after:
            raise ConnectionResetError("simulated disconnect")
        raise StopAsyncIteration

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False


class _RecordingPublisher(Publisher):
    def __init__(self):
        self.published = []

    async def publish(self, event):
        self.published.append(event)


async def _no_op_sleep(_seconds: float) -> None:
    return None


async def test_run_pumpportal_scanner_publishes_parsed_events_and_reconnects():
    connections = [
        _FakeWebSocket([json.dumps({"txType": "create", "mint": "mintA"})], fail_after=True),
        _FakeWebSocket([json.dumps({"txType": "migrate", "mint": "mintB"})], fail_after=True),
    ]

    def fake_connect_fn(_url):
        return connections.pop(0)

    publisher = _RecordingPublisher()

    await run_pumpportal_scanner(
        publisher,
        connect_fn=fake_connect_fn,
        max_iterations=2,
        _sleep_fn=_no_op_sleep,
    )

    assert len(publisher.published) == 2
    assert publisher.published[0].mint == "mintA"
    assert publisher.published[0].event_type == "new_token"
    assert publisher.published[1].mint == "mintB"
    assert publisher.published[1].event_type == "migration"


async def test_run_pumpportal_scanner_sends_both_subscriptions():
    ws = _FakeWebSocket([], fail_after=True)

    def fake_connect_fn(_url):
        return ws

    await run_pumpportal_scanner(
        _RecordingPublisher(),
        connect_fn=fake_connect_fn,
        max_iterations=1,
        _sleep_fn=_no_op_sleep,
    )

    methods = [json.loads(m)["method"] for m in ws.sent]
    assert methods == ["subscribeNewToken", "subscribeMigration"]


async def test_run_pumpportal_scanner_skips_unparseable_messages():
    connections = [
        _FakeWebSocket(
            [
                "not-json-at-all",
                json.dumps({"txType": "trade", "mint": "irrelevant"}),
                json.dumps({"txType": "create", "mint": "mintC"}),
            ],
            fail_after=True,
        ),
    ]

    def fake_connect_fn(_url):
        return connections.pop(0)

    publisher = _RecordingPublisher()

    await run_pumpportal_scanner(
        publisher,
        connect_fn=fake_connect_fn,
        max_iterations=1,
        _sleep_fn=_no_op_sleep,
    )

    assert len(publisher.published) == 1
    assert publisher.published[0].mint == "mintC"
