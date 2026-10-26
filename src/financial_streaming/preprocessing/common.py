import csv
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
RAW_COLUMNS = ['step','customer','age','gender','zipcodeOri','merchant','zipMerchant','category','amount','fraud']
COLUMNS = ['transaction_id','customer_id','merchant_id','step','event_time','ingestion_time','amount','category','age_group','gender','zipcode_ori','zipcode_merchant','fraud_label','data_source']

def source_file(path=None):
    if path:
        p=Path(path)
        if not p.is_file(): raise FileNotFoundError(p)
        return p
    matches=sorted((ROOT/'data/raw').glob('*.csv'))
    if len(matches)!=1: raise ValueError(f'Expected exactly one CSV in data/raw; found {len(matches)}. Supply --input.')
    return matches[0]

def clean_str(value):
    return str(value or '').strip().strip("'\"").strip()

def write_json(path, obj):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(obj,ensure_ascii=False,indent=2),encoding='utf-8')

def write_csv(path,rows,columns=COLUMNS):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w',newline='',encoding='utf-8') as f:
        writer=csv.DictWriter(f,fieldnames=columns,extrasaction='ignore');writer.writeheader();writer.writerows(rows)

def txid(source_name,row_number):
    return hashlib.sha256(f'{source_name}:{row_number}'.encode()).hexdigest()[:24]
