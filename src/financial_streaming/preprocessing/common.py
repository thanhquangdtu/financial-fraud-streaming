import csv
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
RAW_COLUMNS = ['step','customer','age','gender','zipcodeOri','merchant','zipMerchant','category','amount','fraud']
COLUMNS = ['transaction_id','customer_id','merchant_id','step','event_time','ingestion_time','amount','category','age_group','gender','zipcode_ori','zipcode_merchant','fraud_label','data_source']

# ==============================================================================
# Hàm: source_file
# - Mục đích: Xác định và trả về đường dẫn file CSV dữ liệu thô (raw BankSim).

def source_file(path=None):
    if path:
        p=Path(path)
        if not p.is_file(): raise FileNotFoundError(p)
        return p
    matches=sorted((ROOT/'data/raw').glob('*.csv'))
    if len(matches)!=1: raise ValueError(f'Expected exactly one CSV in data/raw; found {len(matches)}. Supply --input.')
    return matches[0]

# ==============================================================================
# Hàm: clean_str
# - Mục đích: Làm sạch chuỗi văn bản, loại bỏ khoảng trắng thừa và dấu nháy bao quanh.
# - Điều cần chú ý:
#   + Dữ liệu thô BankSim thường chứa các trường bị bọc trong dấu nháy đơn hoặc kép (ví dụ: "'C123'").
#   + Xử lý cả giá trị `None` chuyển thành chuỗi rỗng `''`.
#   + Thực hiện `.strip().strip("'\"").strip()` để làm sạch triệt để cả dấu nháy lẫn khoảng trắng ngoài cùng.
# ==============================================================================
def clean_str(value):
    return str(value or '').strip().strip("'\"").strip()

# ==============================================================================
# Hàm: write_json
# - Mục đích: Ghi dữ liệu dạng dictionary/object ra file JSON với format chuẩn.
# - Điều cần chú ý:
#   + Tự động tạo thư mục cha (`parents=True, exist_ok=True`) nếu chưa tồn tại.
#   + Ghi với định dạng UTF-8, `indent=2`, `ensure_ascii=False` để hiển thị rõ ràng ký tự Unicode.
# ==============================================================================
def write_json(path, obj):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(obj,ensure_ascii=False,indent=2),encoding='utf-8')

# ==============================================================================
# Hàm: write_csv
# - Mục đích: Ghi danh sách các bản ghi (list of dicts) ra file CSV.
# - Điều cần chú ý:
#   + Mặc định sử dụng danh mục cột chuẩn `COLUMNS` nếu không truyền tham số `columns`.
#   + Tự động tạo thư mục cha nếu chưa có.
#   + Thiết lập `extrasaction='ignore'` để bỏ qua các trường không nằm trong `columns` mà không gây lỗi.
#   + Mã hóa `utf-8`, mở file với `newline=''` để tránh dòng trống thừa trên Windows.
# ==============================================================================
def write_csv(path,rows,columns=COLUMNS):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w',newline='',encoding='utf-8') as f:
        writer=csv.DictWriter(f,fieldnames=columns,extrasaction='ignore');writer.writeheader();writer.writerows(rows)

# ==============================================================================
# Hàm: txid
# - Mục đích: Sinh mã định danh giao dịch duy nhất và tất định (`transaction_id`).
# - Điều cần chú ý:
#   + Kết hợp tên file nguồn (`source_name`) và số dòng trong file CSV (`row_number`),
#     sau đó băm SHA-256 và cắt lấy 24 ký tự hex đầu tiên.
#   + Tính tất định (idempotent / stable ID): cùng một dòng trong cùng một file khi chạy lại
#     sẽ luôn sinh ra đúng mã ID đó, phục vụ việc kiểm tra trùng lặp và replay stream đáng tin cậy.
# ==============================================================================
def txid(source_name,row_number):
    return hashlib.sha256(f'{source_name}:{row_number}'.encode()).hexdigest()[:24]
