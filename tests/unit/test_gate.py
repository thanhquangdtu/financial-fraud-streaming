import json

import pytest

from financial_streaming.ml.gate import check_quality, gate_and_promote


def _report(f1=0.83, pr=0.90, diff=1e-15):
    return {"test": {"f1": f1, "pr_auc": pr}, "export_max_abs_diff_vs_sklearn": diff}


def test_good_model_passes():
    assert check_quality(_report())["passed"]


@pytest.mark.parametrize("report", [_report(f1=0.79), _report(pr=0.84), _report(diff=1e-3)])
def test_bad_model_fails(report):
    verdict = check_quality(report)
    assert not verdict["passed"] and verdict["problems"]


def _files(tmp_path, report, live_text="LIVE"):
    cand_m, cand_r = tmp_path / "cand_model.json", tmp_path / "cand_report.json"
    live_m, live_r = tmp_path / "live" / "model.json", tmp_path / "live" / "report.json"
    cand_m.write_text("CANDIDATE", encoding="utf-8")
    cand_r.write_text(json.dumps(report), encoding="utf-8")
    live_m.parent.mkdir()
    live_m.write_text(live_text, encoding="utf-8")
    live_r.write_text("{}", encoding="utf-8")
    return cand_m, cand_r, live_m, live_r


def test_good_candidate_replaces_live_model(tmp_path):
    cand_m, cand_r, live_m, live_r = _files(tmp_path, _report())
    gate_and_promote(cand_m, cand_r, live_m, live_r)
    assert live_m.read_text(encoding="utf-8") == "CANDIDATE"
    assert json.loads(live_r.read_text(encoding="utf-8"))["test"]["f1"] == 0.83
    assert not list((tmp_path / "live").glob("*.tmp"))


def test_bad_candidate_keeps_live_model(tmp_path):
    cand_m, cand_r, live_m, live_r = _files(tmp_path, _report(f1=0.5))
    with pytest.raises(ValueError, match="live model kept"):
        gate_and_promote(cand_m, cand_r, live_m, live_r)
    assert live_m.read_text(encoding="utf-8") == "LIVE"
