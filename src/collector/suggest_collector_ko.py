from typing import List
from collector.suggest_collector_base import DailySuggestCollector


class DailySuggestCollectorKo(DailySuggestCollector):
    def __init__(self,
                 date : str, # yyyymmdd
                 lang : str,
                 service : str # ['google', 'youtube']
                 ):
        super().__init__(date, lang, service)
    
    def get_target_suggest_extension_texts(self) -> List[str]:
        '''
        타겟 키워드의 서제스트 extension texts 가져오기
        '''
        extension_texts = self.lang.get_none() # 타겟키워드 + " "
        extension_texts += self.lang.suggest_extension_texts_by_rank(rank=1) # 1단계
        extension_texts += self.lang.suggest_extension_texts_by_rank(rank=2) # 2단계
        return extension_texts
    
    def get_basic_suggest_extension_texts(self) -> List[str]:
        '''
        기본 키워드의 서제스트 extension texts 가져오기
        '''
        extension_texts = self.lang.suggest_extension_texts(contain_none=False)
        return extension_texts
        
if __name__ == "__main__":
    daily_suggest_collector = DailySuggestCollectorKo("20240221", 'ko', "youtube")
    print(f"lang", daily_suggest_collector.lang)
    print(f"language", daily_suggest_collector.lang.language())
    print(f"local_result_path", daily_suggest_collector.local_result_path)
    print(f"hdfs_result_path", daily_suggest_collector.hdfs_result_path)
    print(f"hdfs_result_path_target", daily_suggest_collector.hdfs_result_path_target)
    target_suggest_extension_texts = daily_suggest_collector.get_target_suggest_extension_texts()
    print(f"target_suggest_extension_texts", 
          len(target_suggest_extension_texts), 
          f"{target_suggest_extension_texts[0]} to {target_suggest_extension_texts[-1]}")
    basic_suggest_extension_texts = daily_suggest_collector.get_basic_suggest_extension_texts()
    print(f"basic_suggest_extension_texts", 
          len(basic_suggest_extension_texts),
          f"{basic_suggest_extension_texts[0]} to {basic_suggest_extension_texts[-1]}")