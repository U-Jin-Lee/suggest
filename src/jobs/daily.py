import os
from datetime import datetime
from typing import List

from utils.file import JsonlFileHandler, gzip, remove_folder
from utils.slack import ds_daily_dbgout, ds_daily_dbgout_error, test_dbgout
from utils.hdfs import HdfsFileHandler
from utils.db import QueryDatabaseKo
from suggest_collector import Suggest
from config import prefix_message
from lang import Ko
from jobs.daily_score import main_score
from jobs.daily_category import main_category
from jobs.daily_db import main_db
from jobs.daily_score_target import main_score_target
from jobs.daily_db_target import main_db_target
from jobs.daily_serp import main_serp

prefix_message += f"-ko\n"

class DailySuggestCollector:
    def __init__(self,
                 date # yyyymmdd
                 ):
        self.date = date
        self.now = datetime.now().strftime("%Y%m%d%H")
        self.jobid = self.now
        self.hdfs = HdfsFileHandler()
        self.ko = Ko()
        self.local_result_path = f"./data/result/ko/{date}/{self.now}"
        if not os.path.exists(self.local_result_path):
            os.makedirs(self.local_result_path)
    
    def get_suggest(self,
                    target_keyword = None) -> str: # 저장 경로 반환
        suggest = Suggest()
        if target_keyword == None: # 기본 서제스트
            targets = self.ko.suggest_extension_texts(contain_none=False)
            result_file_path = f"{self.local_result_path}/{self.date}.jsonl"
        else: # 타겟 키워드 있을 경우 서제스트 (2단계 까지)
            targets = self.ko.get_none() # 타겟키워드 + " "
            targets += self.ko.suggest_extension_texts_by_rank(rank=1) # 1단계
            targets += self.ko.suggest_extension_texts_by_rank(rank=2) # 2단계
            result_file_path = f"{self.local_result_path}/{target_keyword.replace(' ', '_')}.jsonl"
            targets = [target_keyword + " " + t for t in targets]
        batch_size = 10000
        print(f"수집할 개수 : {len(targets)} | batch_size : {batch_size}")
        for i in range(0, len(targets), batch_size):
            start = datetime.now()
            result = suggest._requests(targets[i : i+batch_size], 'ko')
            print(f"   ㄴ batch {i+1} finish : {datetime.now()-start}")
            JsonlFileHandler(result_file_path).write(result)
            
        return result_file_path
    
    def target_suggest_job(self):
        target_keywords = QueryDatabaseKo.get_suggest_target_keywords() # 타겟키워드 수집
        hdfs_result_path = f"/user/ds/wordpopcorn/ko/daily/google_suggest_for_trend_target/{self.date[:4]}/{self.date[:6]}/{self.date}/{self.now}"
        for kw in target_keywords:
            result_file_path = self.get_suggest(target_keyword=kw)
            gzip_file_path = gzip(result_file_path)
            try:
                self.hdfs.upload(source=gzip_file_path, dest=hdfs_result_path, overwrite=True)
                remove_folder(gzip_file_path)
            except Exception as e:
                print(f"error from upload to hdfs : {e}")
        ds_daily_dbgout(prefix_message + f"jobid : {self.jobid}\n" + f"타겟 키워드 {len(target_keywords)}개 서제스트 수집 완료 (hdfs dest : {hdfs_result_path})")
        
    def basic_suggest_job(self):
        # 타겟키워드 수집
        hdfs_result_path = f"/user/ds/wordpopcorn/ko/daily/google_suggest_for_trend/{self.date[:4]}/{self.date[:6]}/{self.date}/{self.now}"
        result_file_path = self.get_suggest()
        gzip_file_path = gzip(result_file_path)
        try:
            self.hdfs.upload(source=gzip_file_path, dest=hdfs_result_path, overwrite=True)
            remove_folder(gzip_file_path)
        except Exception as e:
                print(f"error from upload to hdfs : {e}")
        ds_daily_dbgout(prefix_message + f"jobid : {self.jobid}\n" + f"기본 서제스트 수집 완료 (hdfs dest : {hdfs_result_path})")
               
    def main(self):
        ds_daily_dbgout(prefix_message + f"jobid : {self.jobid}\n" + "데일리 수집 시작")
        try:            
            # 타겟키워드 서제스트 수집
            self.target_suggest_job()
            # basic 서제스트 수집
            self.basic_suggest_job()
        except Exception as e:
            print(e)
            ds_daily_dbgout_error(prefix_message + f"jobid : {self.jobid}\n" + f"데일리 수집 실패 (error msg : {e})")
        else:
            ds_daily_dbgout(prefix_message + f"jobid : {self.jobid}\n" + "데일리 수집 완료")
    
if __name__ == "__main__":    
    today = datetime.now().strftime("%Y%m%d")
    daily_suggest_collector = DailySuggestCollector(today)
    daily_suggest_collector.main()
    lang = 'ko'
    
    # 타겟 서제스트 스코어링 및 디비 저장
    main_score_target(today, lang, 'google_suggest_for_trend_target')
    test_dbgout(f"{lang} - 타겟 서제스트 스코어링 완료")
    main_db_target(lang, today)
    test_dbgout(f"{lang} - 타겟 서제스트 디비 업데이트 완료")
    
    # 기본 서제스트 스코어링 및 카테고리 및 디비 저장
    main_score(today, lang, "google_suggest_for_trend")
    test_dbgout(f"{lang} - 기본 서제스트 스코어링 완료")
    main_serp(today, lang)
    test_dbgout(f"{lang} - 카프카에 서프 수집 요청 완료")
    main_category(today, lang)
    test_dbgout(f"{lang} - 기본 서제스트 카테고링 완료")
    main_db(today, lang)
    test_dbgout(f"{lang} - 기본 서제스트 디비 업데이트 완료")
    
    # 모든 결과 삭제
    # remove_path = f"/data/data2/yj.lee/git/suggest/src/data/result/{lang}/{today}"
    # remove_folder(remove_path)
    # test_dbgout(f"{lang} - 데일리 결과 로컬 삭제 완료 ({remove_path})")
    