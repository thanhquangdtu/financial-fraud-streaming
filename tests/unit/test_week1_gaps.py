import csv
import json

import pytest

from financial_streaming.preprocessing import validate as validate_mod
from financial_streaming.preprocessing.checksum import describe, sha256_file
from financial_streaming.preprocessing.clean import clean
from financial_streaming.preprocessing.common import COLUMNS, RAW_COLUMNS, write_csv
from financial_streaming.preprocessing.inspect import inspect
from financial_streaming.preprocessing.quality import extra_checks
from financial_streaming.preprocessing.validate import check_csv, validate


def row(i, step=0, customer="C1", amount=5.0, label="0", source="banksim", **over):
    r = {
        "transaction_id": f"T{i}", "customer_id": customer, "merchant_id": "M1", "step": step,
        "event_time": "2020-01-01T00:00:00+00:00", "ingestion_time": "", "amount": amount,
        "category": "es_food", "age_group": "3", "gender": "M", "zipcode_ori": "1",
        "zipcode_merchant": "1", "fraud_label": label, "data_source": source,
    }
    r.update(over)
    return r


def raw_row(step="0", customer="'C1'", amount="4.5", fraud="0", merchant="'M1'"):
    return {"step": step, "customer": customer, "age": "'3'", "gender": "'M'", "zipcodeOri": "'28007'",
            "merchant": merchant, "zipMerchant": "'28007'", "category": "'es_food'", "amount": amount,
            "fraud": fraud}


def write_raw(path, rows, header=RAW_COLUMNS):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


# ---------------- inspect ----------------

def test_inspect_profile(tmp_path):
    p = tmp_path / "raw.csv"
    write_raw(p, [
        raw_row(step="0", customer="'C1'", amount="0", fraud="0"),
        raw_row(step="1", customer="'C2'", amount="10", fraud="1"),
        raw_row(step="2", customer="'C2'", amount="-5", fraud="0"),  # invalid: negative
    ])
    r = inspect(p)
    assert r["rows"] == 3 and r["unique_customers"] == 2 and r["unique_merchants"] == 1
    assert r["invalid_rows"] == 1 and r["zero_amount_count"] == 1
    assert r["fraud_counts"] == {0: 1, 1: 1}
    assert r["step_min"] == 0 and r["step_max"] == 1 and r["amount_max"] == 10.0


def test_inspect_rejects_missing_columns(tmp_path):
    p = tmp_path / "raw.csv"
    write_raw(p, [], header=["step", "customer"])
    with pytest.raises(ValueError):
        inspect(p)


# ---------------- clean (file level) ----------------

def test_clean_end_to_end(tmp_path):
    src, out, bad = tmp_path / "raw.csv", tmp_path / "out.csv", tmp_path / "bad.csv"
    write_raw(src, [
        raw_row(step="0"),
        raw_row(step="1", amount="-1"),       # negative amount
        raw_row(step="2", fraud="2"),         # invalid label
        raw_row(step="3", merchant=""),       # missing value
        raw_row(step="4", amount="abc"),      # not a number
        raw_row(step="5"),
    ])
    result = clean(src, out, bad, start="2020-01-01T00:00:00+00:00", step_hours=24)
    assert result["valid_rows"] == 2 and result["invalid_rows"] == 4
    assert result["start"] == "2020-01-01T00:00:00+00:00" and result["step_hours"] == 24
    valid = list(csv.DictReader(out.open(encoding="utf-8")))
    assert [v["step"] for v in valid] == ["0", "5"]
    assert valid[1]["event_time"] == "2020-01-06T00:00:00+00:00"
    assert valid[0]["customer_id"] == "C1" and valid[0]["data_source"] == "banksim"
    invalid = list(csv.DictReader(bad.open(encoding="utf-8")))
    assert [i["line_number"] for i in invalid] == ["3", "4", "5", "6"]


def test_clean_rejects_bad_header_and_params(tmp_path):
    src = tmp_path / "raw.csv"
    write_raw(src, [], header=["step", "customer"])
    with pytest.raises(ValueError):
        clean(src, tmp_path / "o.csv", tmp_path / "b.csv")
    write_raw(src, [raw_row()])
    with pytest.raises(ValueError):
        clean(src, tmp_path / "o.csv", tmp_path / "b.csv", start="2020-01-01T00:00:00")  # no timezone
    with pytest.raises(ValueError):
        clean(src, tmp_path / "o.csv", tmp_path / "b.csv", step_hours=0)


# ---------------- validate / quality ----------------

def test_check_csv_detects_problems(tmp_path):
    p = tmp_path / "x.csv"
    write_csv(p, [row(1), row(1), row(3, label="7"), row(4, amount=-2), row(5, source="synthetic")])
    r = check_csv(p, "banksim")
    msgs = " ".join(r["errors"])
    assert "duplicate transaction_id" in msgs and "invalid fraud label" in msgs
    assert "negative amount" in msgs and "wrong data_source" in msgs


def test_check_csv_missing_columns(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("a,b\n1,2\n", encoding="utf-8")
    assert check_csv(p, "banksim")["errors"] == ["missing required columns"]


def test_extra_checks_stats_and_ok(tmp_path):
    p = tmp_path / "x.csv"
    write_csv(p, [row(1, 0, "C1", 0.0), row(2, 1, "C1", 10.0), row(3, 1, "C2", 20.0)])
    r = extra_checks(p, "banksim")
    assert r["errors"] == []
    s = r["stats"]
    assert s["amount"] == {"min": 0.0, "max": 20.0, "mean": 10.0, "zero_count": 1}
    assert s["customers"] == {"unique": 2, "tx_min": 1, "tx_max": 2, "tx_mean": 1.5}
    assert s["null_counts"] == {} and s["step_order_violations"] == 0


def test_extra_checks_flags_nulls_and_order(tmp_path):
    p = tmp_path / "x.csv"
    write_csv(p, [row(1, 5), row(2, 3, category=""), row(3, 4, amount="nan")])
    r = extra_checks(p, "banksim")
    text = " ".join(r["errors"])
    assert "empty value(s) in required column 'category'" in text
    assert "step ordering violation" in text and "non-finite amount" in text
    # synthetic files do not need global step ordering and may have empty fraud_label
    q = tmp_path / "s.csv"
    write_csv(q, [row(1, 5, label="", source="synthetic"), row(2, 3, label="", source="synthetic")])
    assert extra_checks(q, "synthetic", allow_missing_label=True, check_step_order=False)["errors"] == []


def build_data(root, overlap=False):
    clean_rows = [row(i, step=i // 2, customer=f"C{i % 3}") for i in range(12)]
    train, val, test = clean_rows[:6], clean_rows[6:8], clean_rows[8:]
    if overlap:
        val = [row(100, step=train[-1]["step"])] + val
        clean_rows = train + val + test
    for name, rows in (("banksim_clean", clean_rows), ("train", train), ("validation", val), ("test", test)):
        write_csv(root / f"data/processed/{name}.csv", rows)
    write_csv(root / "data/synthetic/transactions.csv",
              [row(1, 3, "SYN_A", label="", source="synthetic"), row(2, 1, "SYN_B", label="", source="synthetic")])


def test_validate_passes_and_reports_stats(tmp_path, monkeypatch):
    build_data(tmp_path)
    monkeypatch.setattr(validate_mod, "ROOT", tmp_path)
    report = validate()
    assert all(r["errors"] == [] for r in report.values()), report
    assert report["banksim_clean"]["rows"] == 12
    assert report["train"]["customers"]["unique"] == 3 and "amount" in report["test"]
    json.dumps(report)  # report must be serializable


def test_validate_detects_split_overlap(tmp_path, monkeypatch):
    build_data(tmp_path, overlap=True)
    monkeypatch.setattr(validate_mod, "ROOT", tmp_path)
    report = validate()
    assert any("overlap" in e for e in report["validation"]["errors"])


# ---------------- checksum ----------------

def test_sha256_file_and_describe(tmp_path):
    p = tmp_path / "a.bin"
    p.write_bytes(b"abc")
    assert sha256_file(p) == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    d = describe(p)
    assert d["bytes"] == 3 and d["file"] == "a.bin"
