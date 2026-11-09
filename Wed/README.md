# Financial Fraud Detection Dashboard (Streamlit)

Dashboard demo theo 3 schema PostgreSQL: `transactions`, `daily_stats`, `customer_week`.
Hiện tại ứng dụng sinh mock data, chưa kết nối Kafka hoặc PostgreSQL.

## 1. Cài đặt
Mở Terminal tại thư mục `web` và chạy:

```bash
python -m venv .venv
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run app.py
```

Nếu PowerShell chặn kích hoạt môi trường, có thể chạy trực tiếp:

```powershell
python -m pip install -r requirements.txt
python -m streamlit run app.py
```

## 2. Các màn hình
- Tổng quan: KPI, biểu đồ giao dịch, trạng thái và cảnh báo mới.
- Giao dịch: lọc trạng thái/ngành hàng, tìm mã giao dịch/khách hàng, tải CSV.
- Cảnh báo gian lận: lọc theo ngưỡng `fraud_score`.
- Khách hàng 360: thống kê 7 ngày và lịch sử giao dịch theo khách hàng.

## 3. Cấu trúc
```text
web/
├── app.py
├── requirements.txt
├── README.md
└── data/
    └── mock_data.py
```

## 4. Schema
Các DataFrame được tạo theo đúng tên cột của 3 bảng đã thống nhất. Khi có dữ liệu thật, thay phần sinh mock data bằng hàm đọc PostgreSQL hoặc CSV; không cần viết lại giao diện.
