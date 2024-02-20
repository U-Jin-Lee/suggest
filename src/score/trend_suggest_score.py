import pandas as pd
import numpy as np
from typing import Dict, List
from datetime import datetime
import concurrent.futures
from tqdm import tqdm

from lang import Ko, Ja
from suggest_validator import SuggestValidator
from utils.data import combine_dictionary

class TrendSuggestScoring:
    def __init__(self):
        self.ko = Ko()
        self.ja = Ja()
        self.rank_by_token_by_lang = {'ko':{0:self.ko.suggest_extension_texts_by_rank(0),
                                           1:self.ko.suggest_extension_texts_by_rank(1),
                                           2:self.ko.suggest_extension_texts_by_rank(2),
                                           3:self.ko.suggest_extension_texts_by_rank(3),
                                           4:self.ko.suggest_extension_texts_by_rank(4)},
                                    'ja':{0:self.ja.suggest_extension_texts_by_rank(0),
                                           1:self.ja.suggest_extension_texts_by_rank(1),
                                           2:self.ja.suggest_extension_texts_by_rank(2),
                                           3:self.ja.suggest_extension_texts_by_rank(3)}}
        self.base_score_by_rank = {0 : 5,
                                    1 : 4,
                                    2: 3,
                                    3: 2,
                                    4: 1}
        
        self.score_by_position = {i : round(0.1**i, i) for i in range(1, 21)}
    
    def get_rank(self,
                 kw,
                 lang,
                 target_kw:str = None):
        rank_by_token = self.rank_by_token_by_lang[lang]
        if (target_kw == None) or (kw == ""):
            expantion_token = kw
        else:
            expantion_token = kw.replace(target_kw, "")
            expantion_token = expantion_token.strip()
        for k, v in rank_by_token.items():
            if expantion_token in v:
                return k

        print(f"No Rank : {lang} | {kw} | {target_kw}")
        return "No Rank"

    def get_all_scores_by_kw(self, data, lang, target_kw = None) -> dict:
        all_scores_by_kw : Dict[str, list] = {}
        cnt = 0
        for d in data:
            cnt += 1
            if cnt % 10000 == 0:
                print(f"{cnt}/{len(data)}개 스코어 완료 - {datetime.now()}")
            valid_suggest= []
            trend_suggest = []
            if 'keyword' not in d:
                kw = ""
                rank = 0
            else:
                kw = d['keyword']
                rank = self.get_rank(kw, lang, target_kw)

            suggestions = d['suggestions']

            for suggestion in suggestions:
                text= suggestion['text']
                _s_t = suggestion['suggest_type']
                _s_s_t = suggestion['suggest_subtypes']
                if SuggestValidator.is_valid_suggest(_s_t, _s_s_t):
                    valid_suggest.append(text)
                    if SuggestValidator.is_suffix_trend_suggest(_s_t, _s_s_t):
                        if target_kw != None: # 타겟 키워드 있으면
                            if text.startswith(target_kw + " "): # <타겟 키워드 + " "> 로 시작하는 경우만 추출
                                trend_suggest.append(text)
                        else:
                            trend_suggest.append(text)


             # 트렌드 키워드 스코어링
            for tk in trend_suggest:
                if tk not in all_scores_by_kw:
                    all_scores_by_kw[tk] = []
                position = valid_suggest.index(tk)+1
                score = self.base_score_by_rank[rank] + self.score_by_position[position]
                all_scores_by_kw[tk].append(score)
        return all_scores_by_kw
    
    def get_all_scores_by_kw_parallel(self, data, lang, chunk_size=10000) -> List[dict]:
        all_trend_kws : List[dict]= []
        chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
        with concurrent.futures.ProcessPoolExecutor() as executor:
            results = list(tqdm(executor.map(self.get_all_scores_by_kw, chunks, [lang]*len(chunks)), 
                                total=len(chunks)))
            for result in results:
                all_trend_kws.append(result)
        all_trend_kws = combine_dictionary(all_trend_kws)
        return all_trend_kws

    def max_values_by_starting_digit(self, numbers) -> dict:
        max_values = {}

        for num in numbers:
            start_digit = int(num)

            if start_digit not in max_values or num > max_values[start_digit]:
                max_values[start_digit] = num

        return max_values
    
    def count_values_by_starting_digit(self, numbers) -> dict:
        counts = {}

        for num in numbers:
            start_digit = int(num)

            if start_digit in counts:
                counts[start_digit] += 1
            else:
                counts[start_digit] = 1

        return counts

    def get_max_score_by_rank(self, all_scores_by_kw):
        max_scores_by_rank_by_kw = {}
        for kw, score in all_scores_by_kw.items():
            max_scores_by_rank_by_kw[kw] = list(self.max_values_by_starting_digit(score).values())
        return max_scores_by_rank_by_kw
    
    def sum_score(self, max_scores_by_rank_by_kw : dict):
        return {k : sum(v) for k, v in max_scores_by_rank_by_kw.items()}
    
    def frequency_by_rank(self, all_scores_by_kw : dict):
        cnt_results = {}
        for kw, score in all_scores_by_kw.items():
            cnt_results[kw] = self.count_values_by_starting_digit(score)
        return cnt_results
    
    def make_final_score_df(self, cnt_results, final_score_by_keywords):
        # 데이터 프레임화
        cnt_df = pd.DataFrame(cnt_results).T.reset_index(names='keyword').fillna(0).rename({5:"0단계",
                                                                                            4:"1단계",
                                                                                            3:"2단계",
                                                                                            2:"3단계",
                                                                                            1:"4단계"},
                                                                                            axis=1)
        score_df = pd.DataFrame(np.array([list(final_score_by_keywords.keys()), 
                                          list(final_score_by_keywords.values())]).T, 
                                columns = ['keyword', 'score'])
        
        score_df = score_df.merge(cnt_df, on = 'keyword', how='left')
        
        # 정렬
        columns = list(score_df.columns)
        columns_to_sort = [columns[1]] + \
                          sorted(columns[2:]) # ['score', '0단계', 1단계', '2단계', '3단계', '4단계']
        print(f"columns_to_sort : {columns_to_sort}")
        score_df = score_df.sort_values(columns_to_sort, ascending=False).reset_index(drop=True)
        return score_df
    
    def total_score(self, data, lang, target_kw = None):
        # 1. 트렌드 키워드의 모든 단계의 스코어 저장
        if target_kw != None : # 타겟키워드 있으면 병렬처리 x
            all_scores_by_kw = self.get_all_scores_by_kw(data, lang, target_kw)
        else : # 타겟키워드 없으면 병렬처리 0
            all_scores_by_kw = self.get_all_scores_by_kw_parallel(data, lang)
        
        # 2. 모든 단계에서 가장 큰 스코어만 저장
        max_scores_by_rank_by_kw = self.get_max_score_by_rank(all_scores_by_kw)
        
        # 3. 스코어 합치기
        final_score_by_keywords = self.sum_score(max_scores_by_rank_by_kw)
        
        # 4. 각 단계에서의 빈도수 저장
        cnt_results = self.frequency_by_rank(all_scores_by_kw)
        
        # 데이터 프레임화
        score_df = self.make_final_score_df(cnt_results, final_score_by_keywords)
        
        # 정렬 (일단 건너뜀, 단계가 다 달라서)
        # score_df = score_df.sort_values(['score', '0단계', '1단계', '2단계'], ascending=False).reset_index(drop=True)
        
        return score_df

trend_scoring = TrendSuggestScoring()

if __name__ == "__main__":
    from utils.file import JsonlFileHandler
    data = JsonlFileHandler("/data/data1/share/notebooks/yj.lee-notebooks/서제스트 트렌드 키워드/이강인.jsonl").read()
    res = trend_scoring.get_all_scores_by_kw(data, 'ko', '이강인')
    print(res)