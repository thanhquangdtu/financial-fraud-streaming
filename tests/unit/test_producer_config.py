import pytest
from financial_streaming.producer.config import load_config


def test_defaults():
    c = load_config({})
    assert c.bootstrap_servers == "localhost:9092"
    assert c.topic == "transactions.raw"
    assert c.rate == 10.0
    assert c.input_path.endswith("validation.csv")


def test_env_override():
    c = load_config({"REPLAY_RATE": "25", "KAFKA_TOPIC": "t"})
    assert c.rate == 25.0 and c.topic == "t"


@pytest.mark.parametrize("rate", ["0", "-1"])
def test_rate_must_be_positive(rate):
    with pytest.raises(ValueError):
        load_config({"REPLAY_RATE": rate})

