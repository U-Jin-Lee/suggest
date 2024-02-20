import os
from datetime import datetime
from typing import List

from utils.file import JsonlFileHandler, gzip, remove_folder
from utils.slack import ds_daily_dbgout, ds_daily_dbgout_error, test_dbgout
from utils.hdfs import HdfsFileHandler
from utils.db import QueryDatabaseKo, QueryDatabaseJa
from suggest_collector import Suggest
from config import prefix_message
from lang import Ko, Ja
from jobs.daily_score import main_score
from jobs.daily_category import main_category
from jobs.daily_db import main_db
from jobs.daily_score_target import main_score_target
from jobs.daily_db_target import main_db_target

class DailySuggestCollector:
    def __init__(self,
                 date : str, # yyyymmdd
                 lang : str,
                 service : str # ['google', 'youtube']
                 ):
        self.date = date
        self.now = datetime.now().strftime("%Y%m%d%H")
        self.jobid = self.now
        self.hdfs = HdfsFileHandler()
        self.lang = self.set_language(lang)
        self.db = self.set_database(lang)
        self.local_result_path = f"./data/result/{service}/{lang}/{date}/{self.now}"
        self.make_local_result_folder()
        self.hdfs_result_path = f"/user/ds/wordpopcorn/{lang}/daily/{service}_suggest_for_trend/{self.date[:4]}/{self.date[:6]}/{self.date}/{self.now}"
        self.hdfs_result_path_target = f"/user/ds/wordpopcorn/{lang}/daily/{service}_suggest_for_trend_target/{self.date[:4]}/{self.date[:6]}/{self.date}"
    
    def set_language(self, lang):
        if lang == 'ko':
            return Ko()
        if lang == 'ja':
            return Ja()
    
    def set_database(self, lang):
        if lang == 'ko':
            return QueryDatabaseKo()
        if lang == 'ja':
            return QueryDatabaseJa()
            
    def make_local_result_folder(self):
        if not os.path.exists(self.local_result_path):
            os.makedirs(self.local_result_path)
        
    def get_suggest(self,
                    targets : List[str],
                    result_file_path : str) -> str: # 저장 경로 반환
        '''
        서제스트 수집 요청 및 로컬에 저장
        '''
        suggest = Suggest()
        batch_size = 10000
        print(f"수집할 개수 : {len(targets)} | batch_size : {batch_size}")
        for i in range(0, len(targets), batch_size):
            start = datetime.now()
            result = suggest._requests(targets[i : i+batch_size], 'ko')
            print(f"   ㄴ batch {i+1} finish : {datetime.now()-start}")
            JsonlFileHandler(result_file_path).write(result)
            
        return result_file_path
    
    def get_target_keywords(self) -> List[str]:
        return self.db.get_suggest_target_keywords() # 타겟키워드 수집
    
    def combine_target_keyword_and_extention_texts(self, 
                                                   target_keyword : str, 
                                                   extention_texts : List[str]):
        return [target_keyword + ' ' + extention_text for extention_text in extention_texts]
    
    def get_target_suggest_extension_texts(self) -> List[str]:
        '''
        타겟 키워드의 서제스트 extention texts 가져오기
        '''
        ...
        
    def target_suggest_job(self):
        '''
        타겟 키워드의 서제스트 수집 프로세스 
        '''
        target_keywords = self.get_target_keywords() # 디비에서 타겟 키워드 가져오기
        extension_texts = self.get_target_suggest_extension_texts() # 타겟 키워드 확장 텍스트 가져오기 (언어별로 다름)
        for target_keyword in target_keywords:
            local_result_path = f"{self.local_result_path}/{target_keyword}.jsonl"
            targets = self.combine_target_keyword_and_extention_texts(target_keyword,
                                                                      extension_texts)
            local_result_path = self.get_suggest(targets, local_result_path)
            local_result_path = gzip(local_result_path)
    
    def get_basic_suggest_extention_texts(self) -> List[str]:
        '''
        기본 키워드의 서제스트 extension texts 가져오기
        '''
        ...
    
    def basic_suggest_job(self):
        '''
        기본 키워드의 서제스트 수집 프로세스
        '''
        ...
          
    def main(self):
        ...