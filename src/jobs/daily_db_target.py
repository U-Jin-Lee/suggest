import os
import json
import argparse
import pandas as pd
from utils.db import QueryDatabaseKo, QueryDatabaseJa

def get_user_target_list(lang) -> pd.DataFrame:
    if lang=='ko':
        res = QueryDatabaseKo.get_target_keyword_by_user()
    if lang=='ja':
        res = QueryDatabaseJa.get_target_keyword_by_user()
    
    # except_user_id = ['hubble']
    res.loc[res['source']=='admin', 'user_id'] = "admin"
    # res = res[(~res['user_id'].isnull()) & ~(res['user_id'].isin(except_user_id))].reset_index(drop=True)
    return res

def get_today_trend_result_by_target(all_targets , lang, date):
    
    target_trend_kws = {}
    for target in all_targets:
        data_path = f"/data/data2/yj.lee/git/suggest/src/data/result/{lang}/{date}/score_{target}_df.csv"
        if not os.path.exists(data_path):
            continue
        target_score = pd.read_csv(f"/data/data2/yj.lee/git/suggest/src/data/result/{lang}/{date}/score_{target}_df.csv")
        if len(target_score) == 0 :
            continue
        target_score = target_score.sort_values('score', ascending=False).reset_index(drop=True)
        target_trend_kws[target] = list(target_score['keyword'])
    return target_trend_kws

def main(lang,
         date):
    # 디비에서 유저별 타겟 키워드 가져오기
    user_target_df = get_user_target_list(lang)
    print(user_target_df)
    
    # 타겟 키워드별 트렌드 키워드 정리
    all_targets = set(user_target_df['target_keyword'])
    target_trend_kws = get_today_trend_result_by_target(all_targets, lang, date)
    
    # 디비에 넣을 형식으로 변경
    insert_values = []
    for i in range(len(user_target_df)):
        user_id = user_target_df.loc[i, 'user_id']
        target = user_target_df.loc[i, 'target_keyword']
        job_id = date
        if target in target_trend_kws:
            cnt = len(target_trend_kws[target])
            info = json.dumps({'keywords' : target_trend_kws[target]},  ensure_ascii=False)
            insert_values.append([user_id, job_id, target.replace('_', ' '), cnt, info])
    
    print(insert_values)
    if lang == "ko":
        QueryDatabaseKo.upsert_google_suggest_trend_target(insert_values)
    if lang == "ja":
        QueryDatabaseJa.upsert_google_suggest_trend_target(insert_values)

def main_db_target(lang,
                   date):
    main(lang,
         date)
    
if __name__=="__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str)
    parser.add_argument("--lang", type=str)
    args = parser.parse_args()
    
    main(args.lang,
         args.date)
    