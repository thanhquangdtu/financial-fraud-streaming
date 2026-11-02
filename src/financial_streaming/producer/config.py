import os
from dataclasses import dataclass

DEFAULT_RATE = 10.0


# ==============================================================================
# Lớp: ProducerConfig
# - Mục đích: Đối tượng cấu hình bất biến (immutable dataclass) cho Kafka Producer.
# - Điều cần chú ý:
#   + Chứa các tham số: `bootstrap_servers` (mặc định 'localhost:9092'), `topic` ('transactions.raw'),
#     `rate` (tốc độ phát lại, mặc định 10.0 events/giây), `input_path` ('data/processed/validation.csv').
#   + Hàm `__post_init__`: Kiểm tra ràng buộc hợp lệ: `rate` bắt buộc phải > 0, `bootstrap_servers`
#     và `topic` không được để trống. Ném ValueError nếu vi phạm.
# ==============================================================================
@dataclass(frozen=True)
class ProducerConfig:
    bootstrap_servers: str = "localhost:9092"
    topic: str = "transactions.raw"
    rate: float = DEFAULT_RATE
    input_path: str = "data/processed/validation.csv"

    def __post_init__(self):
        if self.rate <= 0:
            raise ValueError("REPLAY_RATE must be > 0")
        if not self.bootstrap_servers or not self.topic:
            raise ValueError("bootstrap servers and topic are required")


# ==============================================================================
# Hàm: load_config
# - Mục đích: Nạp cấu hình Producer từ các biến môi trường (Environment Variables).
# - Điều cần chú ý:
#   + Tránh hardcode thông tin kết nối và cấu hình trực tiếp trong code.
#   + Đọc các biến: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `REPLAY_RATE`, `REPLAY_INPUT`.
#   + Nếu không tìm thấy biến môi trường, tự động gán theo giá trị mặc định của `ProducerConfig`.
#   + Hỗ trợ truyền dict `env` tùy chỉnh (rất thuận tiện khi viết unit test).
# ==============================================================================
def load_config(env=None):
    """Build a ProducerConfig from environment variables (no hardcoded credentials)."""
    env = os.environ if env is None else env
    defaults = ProducerConfig()
    return ProducerConfig(
        bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", defaults.bootstrap_servers),
        topic=env.get("KAFKA_TOPIC", defaults.topic),
        rate=float(env.get("REPLAY_RATE", defaults.rate)),
        input_path=env.get("REPLAY_INPUT", defaults.input_path),
    )

