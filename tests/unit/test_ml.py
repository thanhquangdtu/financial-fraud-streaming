import json
import math
from pathlib import Path

import numpy as np
import pytest

pytest.importorskip("sklearn")
from sklearn.ensemble import GradientBoostingClassifier  # noqa: E402

from financial_streaming.ml.export import export_gbt, predict_proba_json  # noqa: E402
from financial_streaming.ml.features import FORBIDDEN, build_feature_spec, transform  # noqa: E402
from financial_streaming.ml.sqlexpr import feature_exprs, score_sql  # noqa: E402

import pandas as pd  # noqa: E402


def _data(n=600, seed=0):
    rng = np.random.default_rng(seed)
    df = pd.DataFrame({
        "amount": np.round(rng.lognormal(3, 1.2, n), 2),
        "category": rng.choice(["es_travel", "es_food", "es_tech"], n),
        "age_group": rng.choice(["1", "2", "U"], n),
        "gender": rng.choice(["F", "M"], n),
        "merchant_id": rng.choice(["M1", "M2", "M3"], n),
    })
    df["fraud_label"] = ((df["amount"] > 80) & (df["category"] == "es_travel")).astype(int)
    return df


def _model():
    df = _data()
    spec = build_feature_spec(df)
    X = transform(df, spec)
    m = GradientBoostingClassifier(n_estimators=8, max_depth=3, random_state=1).fit(X, df["fraud_label"])
    return df, spec, X, m, export_gbt(m, spec, 0.3)


def test_json_export_matches_sklearn():
    _, _, X, m, mj = _model()
    assert np.abs(predict_proba_json(mj, X) - m.predict_proba(X)[:, 1]).max() < 1e-9


def test_generated_sql_matches_sklearn():
    df, spec, X, m, mj = _model()
    sql = score_sql(mj)
    sql = sql.replace("EXP(", "math.exp(")
    expected = m.predict_proba(X)[:, 1]
    for i in range(0, 600, 37):
        env = {f"f{j}": float(X[i, j]) for j in range(X.shape[1])}
        env.update({"IF": lambda c, a, b: a if c else b, "math": math})
        assert eval(sql, {}, env) == pytest.approx(expected[i], abs=1e-9)


def test_label_is_never_a_feature():
    _, spec, _, _, _ = _model()
    names = " ".join(f["name"] for f in spec) + " ".join(e for _, e in feature_exprs(spec))
    for bad in FORBIDDEN:
        assert bad not in names


def test_shipped_model_is_consistent():
    path = Path(__file__).resolve().parents[2] / "data" / "models" / "fraud_gbt.json"
    if not path.exists():
        pytest.skip("model not trained yet")
    mj = json.loads(path.read_text(encoding="utf-8"))
    assert mj["type"] == "gradient_boosting_binary" and 0 < mj["threshold"] < 1
    assert len(mj["trees"]) > 0 and len(mj["features"]) > 10
