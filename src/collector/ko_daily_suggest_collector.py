from collector.collector_base import DailySuggestCollector

class DailySuggestCollectorKo(DailySuggestCollector):
    def __init__(self,
                 date : str, # yyyymmdd
                 lang : str,
                 service : str # ['google', 'youtube']
                 ):
        super().__init__(date, lang, service)
        
if __name__ == "__main__":
    daily_suggest_collector = DailySuggestCollectorKo("20240221", 'ko', "youtube")
    print(f"lang", daily_suggest_collector.lang)
    print(f"language", daily_suggest_collector.lang.language())
    print(f"local_result_path", daily_suggest_collector.local_result_path)
    print(f"hdfs_result_path", daily_suggest_collector.hdfs_result_path)
    print(f"hdfs_result_path_target", daily_suggest_collector.hdfs_result_path_target)