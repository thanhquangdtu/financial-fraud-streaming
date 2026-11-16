"""Turn the exported model JSON into Spark SQL expressions (pure Python, no pyspark import).

Feature i becomes the column `f<i>`; trees become nested IF(cond, left, right). Tests evaluate the generated
SQL with a tiny Python `IF` to prove it equals the scikit-learn probabilities.
"""
from __future__ import annotations


def _num(x: float) -> str:
    s = repr(float(x))
    return f"({s})" if s.startswith("-") else s


def feature_exprs(spec: list[dict]) -> list[tuple[str, str]]:
    """(column name, SQL expression) for every feature. Casts to FLOAT mimic scikit-learn (float32 input)."""
    out = []
    for i, f in enumerate(spec):
        if f["kind"] == "amount":
            expr = "CAST(CAST(amount AS FLOAT) AS DOUBLE)"
        elif f["kind"] == "log_amount":
            expr = "CAST(CAST(LOG1P(amount) AS FLOAT) AS DOUBLE)"
        else:
            value = str(f["value"]).replace("\\", "\\\\").replace("'", "\\'")
            expr = f"CASE WHEN CAST({f['column']} AS STRING) = '{value}' THEN 1.0 ELSE 0.0 END"
        out.append((f"f{i}", expr))
    return out


def tree_sql(tree: dict, node: int = 0) -> str:
    if tree["left"][node] == -1:
        return _num(tree["value"][node])
    cond = f"f{tree['feature'][node]} <= {_num(tree['threshold'][node])}"
    return f"IF({cond}, {tree_sql(tree, tree['left'][node])}, {tree_sql(tree, tree['right'][node])})"


def raw_score_sql(model: dict) -> str:
    total = " + ".join(tree_sql(t) for t in model["trees"])
    return f"{_num(model['init_raw'])} + {_num(model['learning_rate'])} * ({total})"


def score_sql(model: dict) -> str:
    return f"1.0 / (1.0 + EXP(-({raw_score_sql(model)})))"
