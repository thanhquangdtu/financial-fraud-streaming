import json

import pytest

from financial_streaming.producer.config import ProducerConfig
from financial_streaming.producer.message import row_to_message, to_key, to_value
from financial_streaming.producer.replay import (
    load_checkpoint,
    read_control,
    replay,
    write_control,
)

HEADER = (
    "transaction_id,customer_id,merchant_id,step,event_time,ingestion_time,amount,category,"
    "age_group,gender,zipcode_ori,zipcode_merchant,fraud_label,data_source"
)


def make_csv(path, n, bad_at=None):
    lines = [HEADER]
    for i in range(n):
        amount = "-1" if i == bad_at else "5.5"
        lines.append(
            f"T{i},C{i % 3},M1,{i},2020-01-01T00:00:00+00:00,,{amount},es_food,3,M,28007,28007,0,banksim"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class FakeSink:
    def __init__(self, fail=False):
        self.sent, self.flushes, self.fail = [], 0, fail

    def send(self, topic, key, value, on_error):
        self.sent.append((topic, key, json.loads(value)))
        if self.fail:
            on_error(RuntimeError("boom"))

    def flush(self):
        self.flushes += 1


class FakeTime:
    def __init__(self, on_sleep=None):
        self.t, self.sleeps, self.on_sleep = 0.0, [], on_sleep

    def clock(self):
        return self.t

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += s
        if self.on_sleep:
            self.on_sleep(self)


@pytest.fixture
def env(tmp_path):
    return dict(
        control_file=tmp_path / "control",
        checkpoint_file=tmp_path / "cp.json",
        input_path=tmp_path / "in.csv",
    )


def run(env, cfg, sink, ft=None, **kw):
    ft = ft or FakeTime()
    return replay(
        cfg, sink, clock=ft.clock, sleep=ft.sleep, now=lambda: "NOW", checkpoint_every=4, **env, **kw
    ), ft


def test_message_fields_and_ingestion_time():
    row = dict(
        transaction_id="T1", customer_id="C1", merchant_id="M1", step="2",
        event_time="2020-01-03T00:00:00+00:00", ingestion_time="", amount="4.5", category="es_food",
        age_group="3", gender="U", zipcode_ori="0", zipcode_merchant="0", fraud_label="",
        data_source="synthetic", scenario_anomaly="1",
    )
    m = row_to_message(row, "2026-01-01T00:00:00+00:00")
    assert m["event_time"] == row["event_time"]  # unchanged
    assert m["ingestion_time"] == "2026-01-01T00:00:00+00:00"  # assigned at ingestion
    assert m["fraud_label"] is None and m["step"] == 2 and m["amount"] == 4.5
    assert "scenario_anomaly" not in m
    assert to_key(m) == b"C1" and json.loads(to_value(m))["transaction_id"] == "T1"


def test_message_rejects_invalid():
    with pytest.raises(ValueError):
        row_to_message({"transaction_id": "T", "customer_id": "", "merchant_id": "M", "category": "c",
                        "data_source": "banksim"}, "x")


def test_rate_is_fixed_events_per_second(env):
    make_csv(env["input_path"], 20)
    sink = FakeSink()
    s, ft = run(env, ProducerConfig(rate=10), sink)
    assert s["sent"] == 20 and s["completed"] and s["next_index"] == 20
    assert ft.t == pytest.approx(1.9, abs=1e-6)  # 20 events at 10/s: last one due at 1.9 s
    assert [m[2]["transaction_id"] for m in sink.sent] == [f"T{i}" for i in range(20)]


def test_limit_then_restart_has_no_gaps_or_duplicates(env):
    make_csv(env["input_path"], 10)
    a, b = FakeSink(), FakeSink()
    s1, _ = run(env, ProducerConfig(rate=100), a, limit=6)
    assert s1["sent"] == 6 and not s1["completed"] and s1["next_index"] == 6
    s2, _ = run(env, ProducerConfig(rate=100), b)
    ids = [m[2]["transaction_id"] for m in a.sent + b.sent]
    assert ids == [f"T{i}" for i in range(10)] and s2["completed"]


def test_from_start_ignores_checkpoint(env):
    make_csv(env["input_path"], 5)
    run(env, ProducerConfig(rate=100), FakeSink())
    sink = FakeSink()
    run(env, ProducerConfig(rate=100), sink, from_start=True)
    assert len(sink.sent) == 5


def test_pause_and_resume(env):
    make_csv(env["input_path"], 6)
    write_control(env["control_file"], "pause")
    calls = {"n": 0}

    def resume_after_3(ft):
        calls["n"] += 1
        if calls["n"] == 3:
            write_control(env["control_file"], "run")

    sink = FakeSink()
    s, ft = run(env, ProducerConfig(rate=10), sink, ft=FakeTime(resume_after_3))
    assert s["sent"] == 6 and s["paused_seconds"] > 0 and read_control(env["control_file"]) == "run"


def test_failed_delivery_does_not_advance_checkpoint(env):
    make_csv(env["input_path"], 8)
    with pytest.raises(RuntimeError):
        run(env, ProducerConfig(rate=100), FakeSink(fail=True))
    assert load_checkpoint(env["checkpoint_file"], env["input_path"], "transactions.raw") is None


def test_invalid_rows_are_skipped_and_counted(env):
    make_csv(env["input_path"], 5, bad_at=2)
    sink = FakeSink()
    s, _ = run(env, ProducerConfig(rate=100), sink)
    assert s["sent"] == 4 and s["skipped"] == 1 and s["next_index"] == 5


def test_checkpoint_for_other_input_is_rejected(env, tmp_path):
    make_csv(env["input_path"], 3)
    run(env, ProducerConfig(rate=100), FakeSink())
    other = tmp_path / "other.csv"
    make_csv(other, 3)
    with pytest.raises(ValueError):
        replay(ProducerConfig(rate=100), FakeSink(), **{**env, "input_path": other})

