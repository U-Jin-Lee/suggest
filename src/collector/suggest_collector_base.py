import os
from datetime import datetime
from typing import List
from abc import ABC, abstractmethod

from utils.file import JsonlFileHandler, gzip, remove_folder
from utils.hdfs import HdfsFileHandler
from utils.db import QueryDatabaseKo, QueryDatabaseJa
from utils.slack import ds_daily_dbgout, ds_daily_dbgout_error
from suggest_collector import Suggest
from config import prefix_message
from lang import Ko, Ja

class DailySuggestCollector(ABC):
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
        self.service = service
        self.local_result_path = f"./data/result/{service}/{lang}/{date}/{self.now}"
        self.make_local_result_folder()
        self.hdfs_result_path_basic = f"/user/ds/wordpopcorn/{lang}/daily/{service}_suggest_for_trend/{self.date[:4]}/{self.date[:6]}/{self.date}/{self.now}" # 기본 서제스트 hdfs 업로드 경로
        self.hdfs_result_path_target = f"/user/ds/wordpopcorn/{lang}/daily/{service}_suggest_for_trend_target/{self.date[:4]}/{self.date[:6]}/{self.date}" # 타겟 서제스트 hdfs 업로드 경로
        self.prefix_msg = f"{prefix_message}-{service}-{lang}"
        
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
                    result_file_path : str # "*.jsonl"
                    ) -> str: # 저장 경로 반환
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
    
    def combine_target_keyword_and_extension_texts(self, 
                                                   target_keyword : str, 
                                                   extension_texts : List[str]):
        return [target_keyword + ' ' + extension_text for extension_text in extension_texts]
    
    @abstractmethod
    def get_target_suggest_extension_texts(self) -> List[str]:
        '''
        타겟 키워드의 서제스트 extension texts 가져오기
        '''
        ...
        
    def target_suggest_job(self) -> int: # 타겟 키워드 개수 반환
        '''
        타겟 키워드의 서제스트 수집 프로세스 
        '''
        target_keywords = self.get_target_keywords() # 디비에서 타겟 키워드 가져오기
        extension_texts = self.get_target_suggest_extension_texts() # 타겟 키워드 확장 텍스트 가져오기 (언어별로 다름)
        for target_keyword in target_keywords:
            local_result_path = f"{self.local_result_path}/{target_keyword}.jsonl"
            targets = self.combine_target_keyword_and_extension_texts(target_keyword,
                                                                      extension_texts)
            local_result_path = self.get_suggest(targets, local_result_path)
            local_result_path = gzip(local_result_path)
        
        return len(target_keywords)
    
    @abstractmethod
    def get_basic_suggest_extension_texts(self) -> List[str]:
        '''
        기본 키워드의 서제스트 extension texts 가져오기
        '''
        ...
    
    def basic_suggest_job(self) -> str : # 저장 경로 반환
        '''
        기본 키워드의 서제스트 수집 프로세스
        '''
        local_result_path = f"{self.local_result_path}/{self.date}.jsonl"
        targets = self.get_basic_suggest_extension_texts(contain_none=False)
        local_result_path = self.get_suggest(targets, local_result_path)
        local_result_path = gzip(local_result_path)
        
        return local_result_path
          
    def main(self):
        ds_daily_dbgout("\n".join([self.prefix_msg,
                                   f"jobid : {self.jobid}",
                                   f"데일리 수집 시작"]))
        # 타겟 키워드 서제스트 수집
        target_keywords_cnt = self.target_suggest_job()
        ds_daily_dbgout("\n".join([self.prefix_msg,
                                   f"jobid : {self.jobid}",
                                   f"타겟 키워드 {target_keywords_cnt}개 서제스트 수집 완료"]))
        # 타겟 키워드 서제스트 hdfs 업로드 (폴더 전체를 업로드)
        try:
            self.hdfs.upload(self.local_result_path, self.hdfs_result_path_target)
        except Exception as e:
            print(f"error from upload to hdfs (target) : {e}")
            ds_daily_dbgout_error("\n".join([self.prefix_msg,
                                             f"jobid : {self.jobid}",
                                             f"타겟 키워드 서제스트 hdfs 업로드 실패",
                                             f"error msg : {e}"]))
        else:
            ds_daily_dbgout("\n".join([self.prefix_msg,
                                       f"jobid : {self.jobid}",
                                       f"타겟 키워드 서제스트 hdfs 업로드 완료 (hdfs dest : {self.hdfs_result_path_target})"]))
        
        # 기본 서제스트 수집
        local_result_path = self.basic_suggest_job()
        ds_daily_dbgout("\n".join([self.prefix_msg,
                                   f"jobid : {self.jobid}",
                                   f"기본 서제스트 수집 완료"]))
        # 기본 서제스트 hdfs 업로드 (파일 하나를 업로드)
        try:
            self.hdfs.upload(local_result_path, self.hdfs_result_path_basic)
        except Exception as e:
            print(f"error from upload to hdfs (target) : {e}")
            ds_daily_dbgout_error("\n".join([self.prefix_msg,
                                             f"jobid : {self.jobid}",
                                             f"기본 서제스트 hdfs 업로드 실패",
                                             f"error msg : {e}"]))
        else:
            ds_daily_dbgout("\n".join([self.prefix_msg,
                                       f"jobid : {self.jobid}",
                                       f"기본 서제스트 hdfs 업로드 완료 (hdfs dest : {self.hdfs_result_path_target})"]))