# 병렬처리로 얻은 스코어 합치기
import argparse
from tqdm import tqdm

from utils.file import PickleFileHandler
from utils.data import combine_dictionary
from score.trend_suggest_score import trend_scoring


def main(jobid,
         lang,
         ):
    # 하루에 한번 수집시
    data = PickleFileHandler(f"/data/data2/yj.lee/suggest/src/data/result/ko/{jobid}/{jobid}_score.pickle").read()
    # 하루에 두번 수집시
    # data = PickleFileHandler(f"/data/data2/yj.lee/suggest/src/data/result/ko/{jobid}/{jobid}_1_score.pickle").read()
    # data += PickleFileHandler(f"/data/data2/yj.lee/suggest/src/data/result/ko/{jobid}/{jobid}_2_score.pickle").read()
    all_scores_by_kw = combine_dictionary(data)
    max_data=[]
    for d in tqdm(data):
        max_data.append(trend_scoring.get_max_score_by_rank(d))
    max_combine_data = combine_dictionary(max_data)
    # 3. 스코어 합치기
    final_score_by_keywords = trend_scoring.sum_score(max_combine_data)
    
    # 4. 각 단계에서의 빈도수 저장
    cnt_results = trend_scoring.frequency_by_rank(all_scores_by_kw)
    
    # 데이터 프레임화
    score_df = trend_scoring.make_final_score_df(cnt_results, final_score_by_keywords)
    columns = list(score_df.columns)
    columns_to_sort = [columns[1]] + \
                       sorted(columns[2:-1]) # ['score', '0단계', 1단계', '2단계', '3단계', '4단계'] 
    print(f"columns_to_sort : ", columns_to_sort)          
    score_df = score_df.sort_values(columns_to_sort,
                                    ascending=False)
    score_df.to_csv(f"/data/data2/yj.lee/suggest/src/data/result/{lang}/{jobid[:8]}/score_df.csv", index=False, encoding='utf-8-sig')
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str)
    parser.add_argument("--lang", type=str)
    args = parser.parse_args()
    main(args.date, args.lang)