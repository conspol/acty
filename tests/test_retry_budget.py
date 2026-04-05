from tenacity import AsyncRetrying

from acty.retry_budget import retry_factory_from_payload
from acty_core.core import Job, JobId


def test_retry_factory_from_payload_maps_attempts() -> None:
    job = Job(id=JobId("job-1"), kind="unit", payload={"retry_attempts": 2})
    factory = retry_factory_from_payload()
    retrying = factory(job)

    assert isinstance(retrying, AsyncRetrying)
    assert retrying.stop.max_attempt_number == 3


def test_retry_factory_from_payload_clamps_to_max() -> None:
    job = Job(id=JobId("job-1"), kind="unit", payload={"retry_attempts": 9})
    factory = retry_factory_from_payload(max_attempts=3)
    retrying = factory(job)

    assert retrying.stop.max_attempt_number == 3


def test_retry_factory_from_payload_handles_invalid_values() -> None:
    job = Job(id=JobId("job-1"), kind="unit", payload={"retry_attempts": "nope"})
    factory = retry_factory_from_payload()
    retrying = factory(job)

    assert retrying.stop.max_attempt_number == 1


def test_retry_factory_from_payload_custom_key() -> None:
    job = Job(id=JobId("job-1"), kind="unit", payload={"attempts": 1})
    factory = retry_factory_from_payload(payload_key="attempts")
    retrying = factory(job)

    assert retrying.stop.max_attempt_number == 2
