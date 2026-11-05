"""Checksum of the raw BankSim file so later runs can prove the original dataset was not modified.

Usage: python -m financial_streaming.preprocessing.checksum [--input PATH] [--verify]
Writes data/reports/raw_checksum.json. With --verify, compares against the saved checksum and exits 1
on mismatch. The baseline is the file as first used by this project; it has NOT been compared against an
official BankSim distribution hash.
"""
import argparse
import hashlib
import json
from pathlib import Path

from .common import ROOT, source_file, write_json


def sha256_file(path, chunk=1024 * 1024):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def describe(path):
    p = Path(path)
    return {"file": p.name, "bytes": p.stat().st_size, "sha256": sha256_file(p)}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("--verify", action="store_true")
    a = parser.parse_args(argv)
    info = describe(source_file(a.input))
    report = ROOT / "data/reports/raw_checksum.json"
    if a.verify:
        saved = json.loads(report.read_text(encoding="utf-8"))
        ok = saved["sha256"] == info["sha256"] and saved["bytes"] == info["bytes"]
        print({"match": ok, "saved": saved, "current": info})
        raise SystemExit(0 if ok else 1)
    write_json(report, info)
    print(info)


if __name__ == "__main__":
    main()

