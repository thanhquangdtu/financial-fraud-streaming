import pytest

from financial_streaming.streaming.sql import TABLES, build_upsert, upsert_sql


def test_upsert_overwrites_values():
    sql = build_upsert("t", "stg_t", ["a", "b", "c"], ["a"], True)
    assert sql == ("INSERT INTO t (a, b, c) SELECT a, b, c FROM stg_t "
                   "ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b, c = EXCLUDED.c")


def test_upsert_do_nothing():
    assert build_upsert("t", "s", ["a", "b"], ["a"], False).endswith("ON CONFLICT (a) DO NOTHING")


def test_key_must_be_a_column():
    with pytest.raises(ValueError):
        build_upsert("t", "s", ["a"], ["x"], True)


def test_transactions_never_overwritten_and_keyed_by_id():
    sql = upsert_sql("transactions")
    assert "ON CONFLICT (transaction_id) DO NOTHING" in sql


def test_all_tables_have_valid_keys():
    for name in TABLES:
        assert upsert_sql(name).startswith(f"INSERT INTO {name} ")


def test_init_sql_has_every_table():
    from pathlib import Path
    text = (Path(__file__).resolve().parents[2] / "sql" / "init.sql").read_text(encoding="utf-8")
    for name in list(TABLES) + ["dead_letter"]:
        assert f"CREATE TABLE IF NOT EXISTS {name} " in text
    for name, t in TABLES.items():
        for col in t["columns"]:
            assert col in text, (name, col)

