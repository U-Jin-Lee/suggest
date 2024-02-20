import os
from datetime import datetime

from utils.file import JsonlFileHandler, gzip, remove_folder
from utils.slack import ds_daily_dbgout, ds_daily_dbgout_error, test_dbgout
from utils.hdfs import HdfsFileHandler
from utils.db import QueryDatabaseJa
from suggest_collector import Suggest
from config import prefix_message
from lang import Ja
from jobs.daily_score_target import main_score_target
from jobs.daily_db_target import main_db_target
from jobs.daily_score import main_score

prefix_message += f"-ja\n"

class DailySuggestCollectorJa:
    def __init__(self,
                 date # yyyymmdd
                 ):
        self.date = date
        self.now = datetime.now().strftime("%Y%m%d%H")
        self.jobid = self.now
        self.hdfs = HdfsFileHandler()
        self.ja = Ja()
        self.local_result_path = f"./data/result/ja/{date}/{self.now}"
        if not os.path.exists(self.local_result_path):
            os.makedirs(self.local_result_path)
    
    def get_suggest(self,
                    target_keyword = None) -> str: # 저장 경로 반환
        suggest = Suggest()
        targets = []
        total_start = datetime.now()
        print(f"start : {total_start}")
        if target_keyword == None:
            for rank in [1, 2, 3]:  # 3단계까지
                targets += self.ja.suggest_extension_texts_by_rank(rank)
            result_file_path = f"{self.local_result_path}/{self.date}.jsonl"
        else:
            print(f"target_keyword : {target_keyword}")
            for rank in [0, 1, 2]: # 타겟 키워드 + 2단계까지
                targets += self.ja.suggest_extension_texts_by_rank(rank)
            result_file_path = f"{self.local_result_path}/{target_keyword.replace(' ', '_')}.jsonl"
            targets = [target_keyword + " " + t for t in targets]
        batch_size = 10000
        print(f"수집할 개수 : {len(targets)} | batch_size : {batch_size}")
        for i in range(0, len(targets), batch_size):
            start = datetime.now()
            result = suggest._requests(targets[i : i+batch_size], 'ja')
            print(f"   ㄴ batch {i+1} finish : {datetime.now()-start}")
            JsonlFileHandler(result_file_path).write(result)
        
        print(f"end : {datetime.now()} ({datetime.now()-total_start})")
        return result_file_path
    
    def target_suggest_job(self):
        # 타겟키워드 수집
        target_keywords = QueryDatabaseJa.get_suggest_target_keywords()
        hdfs_result_path = f"/user/ds/wordpopcorn/ja/daily/google_suggest_for_trend_target/{self.date[:4]}/{self.date[:6]}/{self.date}/{self.now}"
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
        hdfs_result_path = f"/user/ds/wordpopcorn/ja/daily/google_suggest_for_trend/{self.date[:4]}/{self.date[:6]}/{self.date}/{self.now}"
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
    daily_suggest_collector = DailySuggestCollectorJa(today)
    daily_suggest_collector.main()
    lang = 'ja'
    
    # 타겟 키워드 스코어링 및 디비 저장
    main_score_target(today, lang, 'google_suggest_for_trend_target')
    test_dbgout(f"{lang} - 타겟 서제스트 스코어링 완료")
    main_db_target(lang, today)
    test_dbgout(f"{lang} - 타겟 서제스트 디비 업데이트 완료")
    
    # 기본 서제스트는 카테고리 분류 못함 스코어링만 해놓기
    # main_score(today, lang, 'google_suggest_for_trend')
    # test_dbgout(f"{lang} - 기본 서제스트 스코어링 완료")
    
    # 모든 결과 삭제
    # remove_path = f"/data/data2/yj.lee/suggest/src/data/result/{lang}/{today}"
    # remove_folder(remove_path)
    # test_dbgout(f"{lang} - 데일리 결과 로컬 삭제 완료 ({remove_path})")
