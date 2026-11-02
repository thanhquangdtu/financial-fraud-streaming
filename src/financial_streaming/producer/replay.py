import csv
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from .message import row_to_message, to_key, to_value

log = logging.getLogger(__name__)


# ==============================================================================
# Hàm: read_control
# - Mục đích: Đọc trạng thái điều khiển từ file tín hiệu điều khiển (Control File).
# - Điều cần chú ý:
#   + Trả về chuỗi 'pause' nếu file tồn tại và nội dung chứa chữ 'pause' (không phân biệt hoa thường).
#   + Ngược lại (file không tồn tại hoặc nội dung khác) trả về 'run'.
#   + Dùng để điều khiển tạm dừng / tiếp tục luồng replay mà không cần kill process.
# ==============================================================================
def read_control(path):
    """Return 'pause' if the control file says so, otherwise 'run'."""
    p = Path(path)
    if p.is_file() and p.read_text(encoding="utf-8").strip().lower() == "pause":
        return "pause"
    return "run"


# ==============================================================================
# Hàm: write_control
# - Mục đích: Ghi trạng thái điều khiển ('pause' hoặc 'run') ra file tín hiệu điều khiển.
# - Điều cần chú ý:
#   + Ràng buộc: `state` bắt buộc phải là 'pause' hoặc 'run' (ném ValueError nếu giá trị khác).
#   + Tự động tạo thư mục chứa file nếu chưa tồn tại.
# ==============================================================================
def write_control(path, state):
    if state not in ("pause", "run"):
        raise ValueError("state must be 'pause' or 'run'")
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(state, encoding="utf-8")


# ==============================================================================
# Hàm: load_checkpoint
# - Mục đích: Nạp vị trí phát lại đã lưu từ file checkpoint (`checkpoint.json`).
# - Điều cần chú ý:
#   + Trả về dict trạng thái checkpoint nếu file tồn tại; trả về None nếu file chưa có.
#   + Cơ chế kiểm tra an toàn (Safety check): Nếu checkpoint thuộc về file input khác hoặc topic khác,
#     hàm lập tức ném ValueError để ngăn chặn replay sai vị trí giữa các file khác nhau
#     (yêu cầu người dùng truyền cờ `--from-start` nếu muốn chạy file mới).
# ==============================================================================
def load_checkpoint(path, input_path, topic):
    """Return the saved checkpoint dict, or None. Raises if it belongs to another input/topic."""
    p = Path(path)
    if not p.is_file():
        return None
    data = json.loads(p.read_text(encoding="utf-8"))
    if data.get("input") != str(input_path) or data.get("topic") != topic:
        raise ValueError(
            f"checkpoint {p} belongs to input={data.get('input')} topic={data.get('topic')}; "
            "use --from-start to replace it"
        )
    return data


# ==============================================================================
# Hàm: save_checkpoint
# - Mục đích: Lưu vị trí dòng kế tiếp (`next_index`) và trạng thái hoàn thành vào file checkpoint.
# - Điều cần chú ý:
#   + Ghi nguyên tử (Atomic write): Ghi dữ liệu ra file tạm (`.tmp`) rồi mới thay thế (`replace`),
#     đảm bảo file checkpoint không bao giờ bị hỏng hoặc rỗng nếu tiến trình bị tắt đột ngột (crash/kill).
# ==============================================================================
def save_checkpoint(path, input_path, topic, next_index, completed):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(
        json.dumps(
            {
                "input": str(input_path),
                "topic": topic,
                "next_index": next_index,
                "completed": completed,
            }
        ),
        encoding="utf-8",
    )
    tmp.replace(p)


# ==============================================================================
# Hàm: _utc_now
# - Mục đích: Lấy timestamp hiện tại theo chuẩn UTC định dạng chuỗi ISO 8601.
# - Điều cần chú ý: Dùng làm hàm mặc định để sinh `ingestion_time` khi gửi sự kiện.
# ==============================================================================
def _utc_now():
    return datetime.now(timezone.utc).isoformat()


# ==============================================================================
# Hàm: replay
# - Mục đích: Trái tim của Producer - Phát lại các dòng từ file CSV vào Kafka theo tốc độ cố định (events/giây).
# - Điều cần chú ý:
#   + Cơ chế kiểm soát tốc độ (Pacing):
#     * Tính `interval = 1.0 / config.rate` (ví dụ 10 eps tương ứng 0.1s mỗi message).
#     * Dùng đồng hồ đơn điệu `clock()` để sleep chính xác thời gian còn lại đến mốc `next_due`.
#     * Chống hiện tượng dồn cục (Burst protection): Nếu hệ thống bị lag quá 1.0 giây, mốc `next_due`
#       được đặt lại về thời điểm hiện tại thay vì bắn dồn dập hàng loạt message để bù giờ.
#   + Cơ chế Pause / Resume:
#     * Kiểm tra file control định kỳ mỗi `control_interval` (0.2s).
#     * Khi phát hiện 'pause': flush toàn bộ message đang đọng ra Kafka, lưu checkpoint và rơi vào vòng lặp chờ.
#     * Khi file đổi sang 'run': phục hồi mốc thời gian và tiếp tục chạy mượt mà không bị burst.
#   + Đảm bảo phân phối At-Least-Once & Checkpoint:
#     * Checkpoint chỉ được lưu sau khi gọi `sink.flush()` thành công (định kỳ mỗi `checkpoint_every=100` dòng).
#     * Nếu có lỗi gửi (Network/Broker error), checkpoint không được tăng và tiến trình ném ngoại lệ dừng lại.
#     * Khi bị crash hoặc ngắt bằng Ctrl+C: Khối `finally` đảm bảo vị trí `confirmed` cuối cùng đã flush được giữ nguyên.
#   + Chịu lỗi dòng dữ liệu: Dòng nào vi phạm schema sẽ bị ghi log cảnh báo và tăng biến `skipped` mà không làm chết replay.
#   + Trả về dict tóm tắt: `input`, `topic`, `target_rate_eps`, `start_index`, `sent`, `skipped`,
#     `completed`, `paused_seconds`, `elapsed_seconds`, `effective_rate_eps`, `next_index`.
# ==============================================================================
def replay(
    config,
    sink,
    *,
    input_path=None,
    control_file,
    checkpoint_file,
    checkpoint_every=100,
    from_start=False,
    limit=None,
    clock=time.monotonic,
    sleep=time.sleep,
    now=_utc_now,
    control_interval=0.2,
):
    """Replay a CSV to Kafka at a fixed rate (config.rate events/s).

    Delivery is at-least-once: the checkpoint is only advanced after a successful flush, so after a
    crash up to `checkpoint_every` rows may be sent again. Replayed rows keep the same
    transaction_id/key, so downstream writes must be idempotent.
    `sink` needs send(topic, key, value, on_error) and flush().
    """
    input_path = Path(input_path or config.input_path)
    start = 0
    if not from_start:
        cp = load_checkpoint(checkpoint_file, input_path, config.topic)
        if cp:
            start = int(cp["next_index"])
    interval = 1.0 / config.rate
    errors = []

    def on_error(exc):
        errors.append(exc)

    summary = {
        "input": str(input_path),
        "topic": config.topic,
        "target_rate_eps": config.rate,
        "start_index": start,
        "sent": 0,
        "skipped": 0,
        "completed": False,
        "paused_seconds": 0.0,
    }
    confirmed = start  # last position that is flushed and checkpointed
    position = start
    unflushed = 0
    t0 = clock()
    next_due = t0
    last_control_check = None
    paused = False
    reached_end = False

    def commit(completed=False):
        nonlocal confirmed, unflushed
        sink.flush()
        if errors:
            raise RuntimeError(f"{len(errors)} message(s) failed to deliver: {errors[0]!r}")
        confirmed = position
        unflushed = 0
        save_checkpoint(checkpoint_file, input_path, config.topic, confirmed, completed)

    try:
        with input_path.open(encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                if idx < start:
                    continue
                if limit is not None and summary["sent"] >= limit:
                    break

                t = clock()
                if last_control_check is None or t - last_control_check >= control_interval:
                    last_control_check = t
                    if read_control(control_file) == "pause":
                        if unflushed:
                            commit()
                        log.info("paused at index %d", position)
                        paused = True
                        while read_control(control_file) == "pause":
                            sleep(control_interval)
                            summary["paused_seconds"] += control_interval
                        log.info("resumed at index %d", position)
                        paused = False
                        next_due = clock()  # no burst after a pause
                        last_control_check = clock()

                wait = next_due - clock()
                if wait > 0:
                    sleep(wait)
                if clock() - next_due > 1.0:
                    next_due = clock()  # fell behind: do not burst to catch up
                next_due += interval

                position = idx + 1
                try:
                    message = row_to_message(row, now())
                except (ValueError, TypeError, KeyError) as exc:
                    log.warning("skipping row %d: %s", idx, exc)
                    summary["skipped"] += 1
                else:
                    sink.send(config.topic, to_key(message), to_value(message), on_error)
                    summary["sent"] += 1
                unflushed += 1
                if unflushed >= checkpoint_every:
                    commit()
            else:
                reached_end = True
            if limit is not None and summary["sent"] >= limit:
                reached_end = False
        commit(completed=reached_end)
        summary["completed"] = reached_end
    except BaseException:
        # Interrupted or failed: keep the last confirmed checkpoint, try not to lose acked work.
        try:
            if not errors:
                commit()
        except Exception:
            log.exception("could not save checkpoint on exit; last confirmed index=%d", confirmed)
        raise
    finally:
        elapsed = max(0.0, clock() - t0)
        summary["elapsed_seconds"] = round(elapsed, 4)
        summary["effective_rate_eps"] = round(summary["sent"] / elapsed, 2) if elapsed > 0 else None
        summary["next_index"] = confirmed
    return summary

