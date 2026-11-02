from datetime import datetime,timezone
import pytest
from financial_streaming.preprocessing.clean import normalize
from financial_streaming.preprocessing.split import split
from financial_streaming.preprocessing.synthetic import generate

RAW={'step':'0','customer':"'C1'",'age':"'4'",'gender':"'M'",'zipcodeOri':"'28007'",'merchant':"'M1'",'zipMerchant':"'28007'",'category':"'es_transportation'",'amount':'4.55','fraud':'0'}

# ==============================================================================
# Test: test_normalize
# - Mục đích: Kiểm tra hàm `normalize` bóc tách nháy, ép kiểu dữ liệu và map schema đúng.
# - Điều cần chú ý: Kiểm tra 'customer_id' sạch nháy, 'amount' là float, 'ingestion_time' rỗng, 'fraud_label' là int.
# ==============================================================================
def test_normalize():
    result=normalize(RAW,'banksim.csv',2,datetime(2020,1,1,tzinfo=timezone.utc),24)
    assert result['customer_id']=='C1'
    assert result['amount']==4.55
    assert result['ingestion_time']==''
    assert result['fraud_label']==0

# ==============================================================================
# Test: test_stable_ids
# - Mục đích: Kiểm tra tính tất định (idempotence) của việc sinh mã `transaction_id`.
# - Điều cần chú ý: Cùng một file nguồn và số dòng khi chạy lại phải luôn trả về đúng mã ID cũ.
# ==============================================================================
def test_stable_ids():
    args=('banksim.csv',2,datetime(2020,1,1,tzinfo=timezone.utc),24)
    assert normalize(RAW,*args)['transaction_id']==normalize(RAW,*args)['transaction_id']

# ==============================================================================
# Test: test_reject_invalid
# - Mục đích: Kiểm tra hàm `normalize` bắt buộc phải ném lỗi ValueError khi gặp dữ liệu sai.
# - Điều cần chú ý: Trường hợp thử nghiệm với `amount = -1` (số tiền âm).
# ==============================================================================
def test_reject_invalid():
    with pytest.raises(ValueError):normalize({**RAW,'amount':'-1'},'b.csv',2,datetime.now(timezone.utc),24)

# ==============================================================================
# Test: test_split_step_boundary
# - Mục đích: Kiểm tra việc phân chia theo step thời gian không bị chồng lấn và bảo toàn số dòng.
# - Điều cần chú ý: Không có giao thoa giữa các tập step và tổng số phần tử bằng 30.
# ==============================================================================
def test_split_step_boundary():
    rows=[{'step':str(i//3),'fraud_label':'0'} for i in range(30)]
    parts=split(rows)
    steps=[set(int(r['step']) for r in p) for p in parts.values()]
    assert not (steps[0]&steps[1] or steps[1]&steps[2] or steps[0]&steps[2])
    assert sum(len(p) for p in parts.values())==30

# ==============================================================================
# Test: test_synthetic_uses_24h_steps
# - Mục đích: Kiểm tra dữ liệu synthetic tính toán `event_time` theo bước nhảy 24 giờ mỗi step.
# - Điều cần chú ý: Với start là '2020-01-01T00:00:00+00:00', step 2 phải tương ứng '2020-01-03T00:00:00+00:00'.
# ==============================================================================
def test_synthetic_uses_24h_steps():
    _,rows=generate()
    r=next(x for x in rows if x['step']==2)
    assert r['event_time']=='2020-01-03T00:00:00+00:00'

# ==============================================================================
# Test: test_synthetic_reproducible
# - Mục đích: Kiểm tra tính lặp lại (reproducible) và đặc điểm dữ liệu của hàm sinh synthetic.
# - Điều cần chú ý: Cùng seed sinh ra dữ liệu giống hệt; sinh đúng 4 khách hàng x 40 giao dịch = 160 dòng;
#   toàn bộ `data_source` là 'synthetic' và `fraud_label` là rỗng.
# ==============================================================================
def test_synthetic_reproducible():
    assert generate(seed=18)==generate(seed=18)
    profiles,rows=generate()
    assert len(profiles)==4 and len(rows)==160
    assert all(r['data_source']=='synthetic' and r['fraud_label']=='' for r in rows)
