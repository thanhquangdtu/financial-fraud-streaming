# ==============================================================================
# Lớp: KafkaSink
# - Mục đích: Lớp bọc (wrapper) cho `KafkaProducer` của thư viện `kafka-python`.
# - Điều cần chú ý:
#   + Lazy Import: Chỉ `import KafkaProducer` bên trong `__init__` để khi chạy Unit Test độc lập
#     (không có broker Kafka), các module khác import `KafkaSink` không bị lỗi.
#   + Cấu hình gửi an toàn:
#     * `acks="all"`: Đảm bảo toàn bộ bản sao (ISR) của Kafka đã xác nhận ghi dữ liệu trước khi coi là thành công.
#     * `retries=5`: Tự động thử lại 5 lần nếu gặp lỗi mạng tạm thời.
#     * `linger_ms=5`: Gom các message gửi trong khoảng 5ms để tối ưu hóa throughput truyền tải.
#   + Phương thức:
#     * `send(topic, key, value, on_error)`: Gửi bất đồng bộ kèm callback `add_errback` bắt lỗi gửi.
#     * `flush()`: Đẩy toàn bộ message đang đợi trong bộ đệm ra Kafka (blocking chờ ACK).
#     * `close()`: Đóng kết nối producer an toàn khi hoàn thành.
# ==============================================================================
class KafkaSink:
    """Thin wrapper over kafka-python's KafkaProducer (imported lazily so tests need no broker)."""

    def __init__(self, bootstrap_servers):
        from kafka import KafkaProducer

        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            retries=5,
            linger_ms=5,
        )

    def send(self, topic, key, value, on_error):
        self._producer.send(topic, key=key, value=value).add_errback(on_error)

    def flush(self):
        self._producer.flush()

    def close(self):
        self._producer.close()

