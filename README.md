# Financial Fraud Streaming

Hệ thống phát hiện gian lận tài chính và phân tích hành vi khách hàng theo thời gian thực trên bộ dữ liệu **BankSim** (594,643 giao dịch), sử dụng **Python Replay Producer → Apache Kafka → Apache Spark Structured Streaming → Machine Learning → PostgreSQL / Parquet → Streamlit Dashboard**.

---

## 1. Khởi Động Lại Dự Án Nhanh (Sau Khi Bật Máy)

Mở **PowerShell** tại thư mục gốc `E:\financial-fraud-streaming` và chạy theo thứ tự sau:

### Bước 1: Bật Docker Desktop & Khởi động Kafka
Đảm bảo ứng dụng **Docker Desktop** đang chạy (`Engine running`), sau đó bật container Kafka:

```powershell
docker compose up -d
docker compose ps -a
```
*(Kỳ vọng: `fraud-kafka` ở trạng thái `Up ... (healthy)`, `fraud-kafka-init` ở trạng thái `Exited (0)`).*

### Bước 2: Kích hoạt môi trường ảo Python
```powershell
.\.venv\Scripts\Activate.ps1
```
*(Nếu dùng thư mục `venv` của bạn thì chạy `.\venv\Scripts\Activate.ps1`, hoặc gọi trực tiếp `.\.venv\Scripts\python.exe`).*

### Bước 3: Kiểm tra trạng thái & Bắn dữ liệu vào Kafka
```powershell
# 1. Xem trạng thái checkpoint hiện tại của Producer
python -m financial_streaming.producer.cli status

# 2. Phát tiếp 100 giao dịch vào Kafka (tốc độ 50 giao dịch/giây)
python -m financial_streaming.producer.cli run --limit 100 --rate 50

# 3. Kiểm tra tổng số message đang có trong Kafka và xuất báo cáo
python -m financial_streaming.producer.cli verify
```

---

## 2. Cài Đặt Môi Trường Từ Đầu (Khi Chuyển Sang Máy Mới)

Yêu cầu: **Python >= 3.10** và **Docker Desktop**.

```powershell
# 1. Tạo môi trường ảo và cài đặt package ở chế độ editable kèm bộ test
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .[dev]

# 2. Đặt file dữ liệu gốc BankSim (bs140513_032310.csvvào thư mục data/raw/
```

---

## 3. Các Lệnh Chạy Theo Từng Giai Đoạn

### 3.1. Chạy Kiểm Thử Tự Động (Unit Tests)
```powershell
.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider -v
```
*(19 unit tests kiểm tra cả module `preprocessing` và `producer` mà không cần bật Kafka).*

### 3.2. Tuần 1 – Pipeline Tiền Xử Lý & Chia Tập Dữ Liệu
Chạy lần lượt 5 lệnh theo đúng thứ tự (chỉ cần chạy lại khi thay đổi dữ liệu thô hoặc tham số chia tập):

```powershell
# 1. Khảo sát dữ liệu thô -> xuất data/reports/data_profile.json
.\.venv\Scripts\python.exe -m financial_streaming.preprocessing.inspect

# 2. Làm sạch & chuẩn hóa schema -> xuất data/processed/banksim_clean.csv & data/reports/preprocessing.json
.\.venv\Scripts\python.exe -m financial_streaming.preprocessing.clean

# 3. Sinh dữ liệu kịch bản hành vi nhân tạo -> xuất data/synthetic/*.csv & data/reports/synthetic_report.json
.\.venv\Scripts\python.exe -m financial_streaming.preprocessing.synthetic

# 4. Chia tập Train/Validation/Test (70/15/15 theo step) -> xuất data/processed/{train,validation,test}.csv
.\.venv\Scripts\python.exe -m financial_streaming.preprocessing.split

# 5. Kiểm định chất lượng dữ liệu -> xuất data/reports/data_quality.json
.\.venv\Scripts\python.exe -m financial_streaming.preprocessing.validate
```

### 3.3. Tuần 2 – Quản Lý Kafka & Replay Producer

#### Các lệnh điều khiển Producer CLI:
```powershell
# Phát lại từ đầu file (bỏ qua checkpoint cũvới tốc độ 50 giao dịch/giây, giới hạn 100 dòng
python -m financial_streaming.producer.cli run --limit 100 --rate 50 --from-start

# Phát tiếp từ vị trí checkpoint đã lưu (không có --from-start)
python -m financial_streaming.producer.cli run --limit 50 --rate 50

# Phát từ một file CSV cụ thể (ví dụ tập test hoặc synthetic)
python -m financial_streaming.producer.cli run --input data/processed/test.csv --limit 100 --rate 10 --from-start
python -m financial_streaming.producer.cli run --input data/synthetic/transactions.csv --limit 50 --rate 10 --from-start

# Tạm dừng / Tiếp tục khi Producer đang chạy ở một cửa sổ terminal khác
python -m financial_streaming.producer.cli pause
python -m financial_streaming.producer.cli resume

# Xem trạng thái điều khiển và vị trí dòng trong checkpoint
python -m financial_streaming.producer.cli status

# Kiểm tra trực tiếp số message trên từng partition của Kafka và lấy 3 message mẫu
python -m financial_streaming.producer.cli verify --sample 3
```

#### Các lệnh quản lý Docker Kafka:
```powershell
# Bật Kafka
docker compose up -d

# Tắt Kafka (vẫn giữ nguyên dữ liệu đã gửi)
docker compose stop

# Xóa toàn bộ container và volume dữ liệu Kafka (reset sạch topic về 0 message)
docker compose down -v
```

---

## 4. Vị Trí Các File Báo Cáo Kết Quả (`data/reports/`)

Toàn bộ kết quả chạy thực tế được lưu tự động dưới dạng JSON trong thư mục `data/reports/`:

| File báo cáo | Nội dung |
|---|---|
| `data/reports/data_profile.json` | Thống kê dữ liệu gốc: 594,643 dòng, 4,112 khách hàng, 50 merchant, 7,200 giao dịch gian lận (1.2108%), steps 0–179 |
| `data/reports/preprocessing.json` | Kết quả làm sạch dữ liệu, mốc thời gian bắt đầu (`2020-01-01T00:00:00+00:00`), `step_hours = 24` |
| `data/reports/split_report.json` | Thống kê chia tập theo thời gian: Train (392,733 dòng, steps 0–124), Validation (101,793 dòng, steps 125–152), Test (100,117 dòng, steps 153–179|
| `data/reports/synthetic_report.json` | Thông tin 160 giao dịch nhân tạo của 4 khách hàng mẫu (`SYN_A` → `SYN_D`) |
| `data/reports/data_quality.json` | Kết quả kiểm định schema, trùng lặp ID, ranh giới thời gian (0 lỗi|
| `data/reports/producer_report.json` | Kết quả phiên chạy Kafka Producer gần nhất (số dòng đã gửi, thời gian chạy, tốc độ thực tế đạt được) |
| `data/reports/kafka_verification.json` | Kết quả truy vấn trực tiếp Kafka Broker: tổng số message hiện có, offset từng partition (0, 1, 2) và bản ghi mẫu |

---

## 5. Cấu Trúc Mã Nguồn Chính

```text
financial-fraud-streaming/
├── docker-compose.yml                  # Cấu hình Kafka 3.9.0 (KRaft& tạo topic transactions.raw
├── .env.example                        # Biến môi trường mẫu cho Producer
├── data/
│   ├── raw/                            # Dữ liệu gốc BankSim (bs140513_032310.csv)
│   ├── processed/                      # Dữ liệu sạch: banksim_clean.csv, train.csv, validation.csv, test.csv
│   ├── synthetic/                      # Dữ liệu nhân tạo: customer_profiles.csv, transactions.csv
│   ├── replay/                         # Trạng thái runtime của Producer: checkpoint.json, control
│   └── reports/                        # Các báo cáo JSON nghiệm thu Tuần 1 & Tuần 2
├── src/financial_streaming/
│   ├── preprocessing/                  # Module tiền xử lý dữ liệu (Tuần 1)
│   │   ├── common.py                   # Schema chuẩn & hàm tiện ích dùng chung
│   │   ├── inspect.py                  # Khảo sát dữ liệu thô
│   │   ├── clean.py                    # Làm sạch & chuẩn hóa dữ liệu
│   │   ├── synthetic.py                # Sinh kịch bản hành vi khách hàng nhân tạo
│   │   ├── split.py                    # Phân chia Train/Validation/Test theo step
│   │   └── validate.py                 # Kiểm định chất lượng dữ liệu
│   └── producer/                       # Module Kafka Replay Producer (Tuần 2)
│       ├── config.py                   # Cấu hình Producer từ biến môi trường
│       ├── message.py                  # Đóng gói sự kiện JSON, gán ingestion_time, Key = customer_id
│       ├── replay.py                   # Điều tiết tốc độ gửi, Pause/Resume & Checkpoint
│       ├── sink.py                     # Kết nối KafkaProducer (acks="all", retries=5)
│       └── cli.py                      # Giao diện dòng lệnh: run, pause, resume, status, verify
└── tests/unit/                         # 19 bài kiểm thử tự động (pytest)
```
