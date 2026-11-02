"""CLI: python -m financial_streaming.producer.cli {run,pause,resume,status,verify}"""
import argparse
import dataclasses
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from ..preprocessing.common import ROOT, write_json
from .config import load_config
from .replay import load_checkpoint, read_control, replay, write_control


# ==============================================================================
# Hàm: _abs
# - Mục đích: Chuẩn hóa đường dẫn tương đối thành đường dẫn tuyệt đối dựa trên thư mục gốc `ROOT`.
# - Điều cần chú ý: Tránh lỗi sai lệch thư mục làm việc khi chạy lệnh từ các thư mục con khác nhau.
# ==============================================================================
def _abs(p):
    p = Path(p)
    return p if p.is_absolute() else ROOT / p


# ==============================================================================
# Hàm: verify_topic
# - Mục đích: Kết nối trực tiếp vào Kafka Broker để kiểm tra số lượng message và trích xuất mẫu dữ liệu.
# - Điều cần chú ý:
#   + Sử dụng `KafkaConsumer` truy vấn các partition của topic.
#   + Đo lường `beginning_offsets` và `end_offsets` của từng partition để tính chính xác số lượng message.
#   + Trích xuất danh sách message mẫu (`samples`) gồm partition, offset, key (`customer_id`), value JSON.
#   + Tự động lưu báo cáo kiểm định thực tế vào `data/reports/kafka_verification.json`.
#   + Luôn đóng kết nối consumer trong khối `finally`.
# ==============================================================================
def verify_topic(cfg, sample=3):
    """Query Kafka topic partition offsets and sample messages, and save to data/reports/kafka_verification.json."""
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=cfg.bootstrap_servers,
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
    )
    try:
        parts = sorted(consumer.partitions_for_topic(cfg.topic) or [])
        tps = [TopicPartition(cfg.topic, p) for p in parts]
        beg = consumer.beginning_offsets(tps) if tps else {}
        end = consumer.end_offsets(tps) if tps else {}
        partition_offsets = {
            str(tp.partition): {
                "beginning_offset": beg.get(tp, 0),
                "end_offset": end.get(tp, 0),
                "messages": end.get(tp, 0) - beg.get(tp, 0),
            }
            for tp in tps
        }
        total_messages = sum(v["messages"] for v in partition_offsets.values())

        samples = []
        if tps and total_messages > 0 and sample > 0:
            consumer.assign(tps)
            consumer.seek_to_beginning(*tps)
            for msg in consumer:
                samples.append(
                    {
                        "partition": msg.partition,
                        "offset": msg.offset,
                        "key": msg.key.decode("utf-8") if msg.key else None,
                        "value": json.loads(msg.value.decode("utf-8")),
                    }
                )
                if len(samples) >= sample:
                    break

        report = {
            "verified_at": datetime.now(timezone.utc).isoformat(),
            "bootstrap_servers": cfg.bootstrap_servers,
            "topic": cfg.topic,
            "partition_count": len(parts),
            "total_messages": total_messages,
            "partitions": partition_offsets,
            "sample_messages": samples,
        }
        write_json(ROOT / "data/reports/kafka_verification.json", report)
        return report
    finally:
        consumer.close()


# ==============================================================================
# Hàm: main
# - Mục đích: Điểm vào thực thi dòng lệnh CLI cho module Kafka Producer.
# - Điều cần chú ý:
#   + Cung cấp 5 lệnh con:
#     * `run`: Phát lại dữ liệu CSV vào Kafka (`--input`, `--rate`, `--limit`, `--from-start`).
#              Tự động lưu báo cáo sau khi phát xong vào `data/reports/producer_report.json`.
#     * `pause`: Ghi trạng thái 'pause' vào file `data/replay/control` để tạm dừng producer đang chạy.
#     * `resume`: Ghi trạng thái 'run' vào file `data/replay/control` để tiếp tục chạy.
#     * `status`: Xem trạng thái điều khiển hiện tại và vị trí checkpoint đã lưu.
#     * `verify`: Kiểm định trực tiếp số lượng message và dữ liệu mẫu đang lưu trong Kafka topic.
#   + Tự động làm sạch cờ 'pause' cũ nếu chạy lượt mới.
#   + Luôn đảm bảo đóng kết nối `sink.close()` an toàn khi kết thúc hoặc gặp lỗi.
# ==============================================================================
def main(argv=None):
    parser = argparse.ArgumentParser(prog="producer")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="replay a CSV to Kafka")
    run.add_argument("--input", help="CSV to replay (default REPLAY_INPUT = validation.csv)")
    run.add_argument("--rate", type=float, help="events per second (default REPLAY_RATE = 10)")
    run.add_argument("--limit", type=int, help="send at most N events then stop")
    run.add_argument("--from-start", action="store_true", help="ignore the saved checkpoint")
    for name in ("pause", "resume", "status"):
        sub.add_parser(name)
    ver = sub.add_parser("verify", help="inspect Kafka topic offsets & sample messages and write report")
    ver.add_argument("--sample", type=int, default=3, help="number of sample messages to read")
    a = parser.parse_args(argv)

    cfg = load_config()
    control = _abs(os.environ.get("REPLAY_CONTROL_FILE", "data/replay/control"))
    checkpoint = _abs(os.environ.get("REPLAY_CHECKPOINT_FILE", "data/replay/checkpoint.json"))

    if a.command == "pause":
        write_control(control, "pause")
        print("replay will pause")
        return
    if a.command == "resume":
        write_control(control, "run")
        print("replay will resume")
        return
    if a.command == "status":
        cp = checkpoint.is_file() and json.loads(checkpoint.read_text(encoding="utf-8"))
        print({"control": read_control(control), "checkpoint": cp or None})
        return
    if a.command == "verify":
        report = verify_topic(cfg, sample=a.sample)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if a.rate is not None:
        cfg = dataclasses.replace(cfg, rate=a.rate)  # re-validates rate > 0
    input_path = _abs(a.input or cfg.input_path)
    if control.is_file():
        write_control(control, "run")  # a stale 'pause' must not block a new run

    from .sink import KafkaSink

    sink = KafkaSink(cfg.bootstrap_servers)
    try:
        summary = replay(
            cfg,
            sink,
            input_path=input_path,
            control_file=control,
            checkpoint_file=checkpoint,
            from_start=a.from_start,
            limit=a.limit,
        )
    finally:
        sink.close()
    write_json(ROOT / "data/reports/producer_report.json", summary)
    print(summary)


if __name__ == "__main__":
    main()

