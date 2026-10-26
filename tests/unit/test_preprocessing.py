from datetime import datetime,timezone
import pytest
from financial_streaming.preprocessing.clean import normalize
from financial_streaming.preprocessing.split import split
from financial_streaming.preprocessing.synthetic import generate

RAW={'step':'0','customer':"'C1'",'age':"'4'",'gender':"'M'",'zipcodeOri':"'28007'",'merchant':"'M1'",'zipMerchant':"'28007'",'category':"'es_transportation'",'amount':'4.55','fraud':'0'}

def test_normalize():
    result=normalize(RAW,'banksim.csv',2,datetime(2020,1,1,tzinfo=timezone.utc),24)
    assert result['customer_id']=='C1'
    assert result['amount']==4.55
    assert result['ingestion_time']==''
    assert result['fraud_label']==0

def test_stable_ids():
    args=('banksim.csv',2,datetime(2020,1,1,tzinfo=timezone.utc),24)
    assert normalize(RAW,*args)['transaction_id']==normalize(RAW,*args)['transaction_id']

def test_reject_invalid():
    with pytest.raises(ValueError):normalize({**RAW,'amount':'-1'},'b.csv',2,datetime.now(timezone.utc),24)

def test_split_step_boundary():
    rows=[{'step':str(i//3),'fraud_label':'0'} for i in range(30)]
    parts=split(rows)
    steps=[set(int(r['step']) for r in p) for p in parts.values()]
    assert not (steps[0]&steps[1] or steps[1]&steps[2] or steps[0]&steps[2])
    assert sum(len(p) for p in parts.values())==30

def test_synthetic_reproducible():
    assert generate(seed=18)==generate(seed=18)
    profiles,rows=generate()
    assert len(profiles)==4 and len(rows)==160
    assert all(r['data_source']=='synthetic' and r['fraud_label']=='' for r in rows)
