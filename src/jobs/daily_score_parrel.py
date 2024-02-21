# 병렬 처리를 위한 스크립트
import concurrent.futures
from tqdm import tqdm
from typing import List
 
from score.trend_suggest_score import trend_scoring


def get_all_trend_score_parallel(data, lang, chunk_size=10000) -> List[dict]:
    all_trend_kws = []
    chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = list(tqdm(executor.map(trend_scoring.get_all_scores_by_kw, chunks, [lang]*len(chunks)), 
                            total=len(chunks)))
        for result in results:
            all_trend_kws.append(result)
    return all_trend_kws

if __name__ == "__main__":
    from utils.file import JsonlFileHandler, PickleFileHandler
    from datetime import datetime
    print(f"데이터 읽기 시작 - {datetime.now()}")
    # data = JsonlFileHandler("/data1/share/notebooks/yj.lee-notebooks/서제스트 트렌드 키워드/20240203.jsonl").read() # 20240203
    # data = JsonlFileHandler("/data2/yj.lee/git/suggest/src/data/tmp/ko/20240204/20240204/2024020400/20240204.jsonl").read() # 20240204
    # data = JsonlFileHandler("/data2/yj.lee/git/suggest/src/data/tmp/ko/20240205/20240205/2024020500/20240205.jsonl").read() # 20240205
    # data = JsonlFileHandler("/data2/yj.lee/git/suggest/src/data/tmp/ko/20240206/20240206/2024020600/20240206.jsonl").read() # 20240206_1
    # data = JsonlFileHandler("/data2/yj.lee/git/suggest/src/data/tmp/ko/20240206/20240206/2024020618/20240206.jsonl").read() # 20240206_2
    # data = JsonlFileHandler("/data2/yj.lee/git/suggest/src/data/tmp/ko/20240207/20240207/2024020700/20240207.jsonl").read() # 20240207_1
    data = JsonlFileHandler("/data2/yj.lee/git/suggest/src/data/tmp/ko/20240207/20240207/2024020712/20240207.jsonl").read() # 20240207_2
    print(f"데이터 읽기 완료 ({len(data)}개) - {datetime.now()}")
    print(f"스코어링 시작 - {datetime.now()}")
    res = get_all_trend_score_parallel(data, 'ko')
    # PickleFileHandler("/data2/yj.lee/git/suggest/src/data/result/ko/20240203/20240203_score.pickle").write(res) # 20240203
    # PickleFileHandler("/data2/yj.lee/git/suggest/src/data/result/ko/20240204/20240204_score.pickle").write(res) # 20240204
    # PickleFileHandler("/data2/yj.lee/git/suggest/src/data/result/ko/20240205/20240205_score.pickle").write(res) # 20240205
    # PickleFileHandler("/data2/yj.lee/git/suggest/src/data/result/ko/20240206/20240206_1_score.pickle").write(res) # 20240206_1
    # PickleFileHandler("/data2/yj.lee/git/suggest/src/data/result/ko/20240206/20240206_2_score.pickle").write(res) # 20240206_2
    # PickleFileHandler("/data2/yj.lee/git/suggest/src/data/result/ko/20240207/20240207_1_score.pickle").write(res) # 20240207_1
    PickleFileHandler("/data2/yj.lee/git/suggest/src/data/result/ko/20240207/20240207_2_score.pickle").write(res) # 20240207_2
    print(f"스코어링 완료 - {datetime.now()}")
    print(len(res))
    print(len(res[0]))