import argparse
import csv
from collections import Counter
from .common import RAW_COLUMNS, ROOT, clean_str, source_file, write_json

def inspect(path):
    count=0; customers=set(); merchants=set(); fraud=Counter(); missing=Counter(); invalid=0
    min_amount=None;max_amount=None;total_amount=0.;steps=Counter()
    with path.open(encoding='utf-8-sig',newline='') as f:
        reader=csv.DictReader(f)
        if not reader.fieldnames or any(c not in reader.fieldnames for c in RAW_COLUMNS):
            raise ValueError('Missing BankSim columns: '+str(sorted(set(RAW_COLUMNS)-set(reader.fieldnames or []))))
        for row in reader:
            count+=1
            for c in RAW_COLUMNS:
                if not clean_str(row.get(c)):missing[c]+=1
            customers.add(clean_str(row['customer']));merchants.add(clean_str(row['merchant']))
            try:
                amt=float(clean_str(row['amount'])); label=int(clean_str(row['fraud']));step=int(clean_str(row['step']))
                if label not in (0,1) or amt<0 or step<0:raise ValueError()
                total_amount+=amt;min_amount=amt if min_amount is None else min(min_amount,amt)
                max_amount=amt if max_amount is None else max(max_amount,amt)
                fraud[label]+=1;steps[step]+=1
            except (ValueError,TypeError):invalid+=1
    return {'source':path.name,'rows':count,'unique_customers':len(customers),'unique_merchants':len(merchants),
            'fraud_counts':dict(fraud),'fraud_rate_percent':round(fraud[1]/count*100,4) if count else 0,
            'invalid_rows':invalid,'missing_values':dict(missing),'amount_min':min_amount,'amount_max':max_amount,
            'amount_mean_valid':round(total_amount/(sum(fraud.values())),4) if sum(fraud.values()) else None,
            'step_min':min(steps) if steps else None,'step_max':max(steps) if steps else None,'unique_steps':len(steps)}

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--input');a=parser.parse_args()
    result=inspect(source_file(a.input));write_json(ROOT/'data/reports/data_profile.json',result)
    print(result)
if __name__=='__main__':main()
