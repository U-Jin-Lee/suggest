# 스코어링 + 카테고리 예측 모두 한 데이터 디비에 넣기
import json
import argparse
import pandas as pd
from utils.db import QueryDatabaseKo, QueryDatabaseJa

def make_insert_values(date,
                       df : pd.DataFrame):
    insert_values = []
    for cat in list(df['category'].unique()):
        kws_by_cat = list(df[df['category']==cat]['keyword'])
        job_id = date[:8]
        cnt = len(kws_by_cat)
        info = json.dumps({"keywords" : kws_by_cat},  ensure_ascii=False)
        insert_values.append([job_id, 
                              cat, 
                              cnt, 
                              info])
        
    return insert_values

def main(date,
         lang):
    df_path = f"/data/data2/yj.lee/suggest/src/data/result/{lang}/{date}/score_df.csv"
    df = pd.read_csv(df_path)
    insert_values = make_insert_values(date, df)
    
    if lang == 'ko':
        QueryDatabaseKo.upsert_google_suggest_trend(insert_values)
    elif lang == 'ja':
        QueryDatabaseJa.upsert_google_suggest_trend(insert_values)

def main_db(date,
         lang):
    df_path = f"/data/data2/yj.lee/suggest/src/data/result/{lang}/{date}/score_df.csv"
    df = pd.read_csv(df_path)
    insert_values = make_insert_values(date, df)
    
    if lang == 'ko':
        QueryDatabaseKo.upsert_google_suggest_trend(insert_values)
    elif lang == 'ja':
        QueryDatabaseJa.upsert_google_suggest_trend(insert_values)
          
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str)
    parser.add_argument("--lang", type=str)
    args = parser.parse_args()
    main(args.date, args.lang)