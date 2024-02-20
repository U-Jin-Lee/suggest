import os
import argparse
import gzip
import shutil
from typing import List
from datetime import datetime
from utils.hdfs import HdfsFileHandler 
from utils.file import JsonlFileHandler, find_files_by_format, remove_folder
from score.trend_suggest_score import trend_scoring
from jobs.daily_db import main_db
from jobs.daily_category import main_category

# 디비에 입력하는 프로세스

# 1. 결과 값 다운로드
# 2. 카테고리 예측
# 3. 스코어링
# 4. 저장


def download(date,
             lang,
             project_name: str # ['google_suggest_for_trend_target', 'google_suggest_for_trend']
                  ):
    hdfs = HdfsFileHandler()
    hdfs_path = f"/user/ds/wordpopcorn/{lang}/daily/{project_name}/{date[:4]}/{date[:6]}/{date[:8]}"
    local_path = f"/data/data2/yj.lee/suggest/src/data/tmp/{lang}/{date}"
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    
    hdfs.download(hdfs_path, local_path)
    
    return local_path

def read_all_jsonl_file(folder_path) -> List[dict]:
    data = []
    jsonl_files = find_files_by_format(folder_path, "jsonl") # folder_path에 있는 모든 jsonl 파일 읽기
    print(f"jsonl_files: ", jsonl_files)
    for f in jsonl_files:
        data += JsonlFileHandler(f).read()
    return data

def extract_gzip_files(folder_path):
    # 폴더 내의 모든 파일 및 하위 폴더 리스트 가져오기
    all_files = []
    for root, dirs, files in os.walk(folder_path):
        all_files.extend([os.path.join(root, file) for file in files])

    # 'jsonl.gz'로 끝나는 파일 필터링 및 압축 해제
    for file_path in filter(lambda x: x.endswith('.jsonl.gz'), all_files):
        # 압축 해제된 파일의 경로 설정 (확장자 .jsonl.gz를 제외)
        dest_path = os.path.splitext(file_path)[0]

        # gzip 파일 열기
        print("file_path:", file_path)
        with gzip.open(file_path, 'rb') as gz_file:
            # 압축 해제된 파일로 복사
            with open(dest_path, 'wb') as dest_file:
                shutil.copyfileobj(gz_file, dest_file)

        print(f'압축 해제 완료: {file_path} -> {dest_path}')
        
def main(jobid,
         lang,
         project_name):
    print(f"데이터 다운로드 - {datetime.now()}")
    local_path = download(jobid, lang, project_name)
    
    print(f"압축풀기 - {datetime.now()}")
    extract_gzip_files(local_path)
    
    data = read_all_jsonl_file(local_path)
    
    print(f"스코어링 시작 - {datetime.now()}")
    score_df = trend_scoring.total_score(data, lang)
    
    if not os.path.exists(f"/data/data2/yj.lee/suggest/src/data/result/{lang}/{jobid[:8]}"):
        os.makedirs(f"/data/data2/yj.lee/suggest/src/data/result/{lang}/{jobid[:8]}")
    score_df.to_csv(f"/data/data2/yj.lee/suggest/src/data/result/{lang}/{jobid[:8]}/score_df.csv", index=False, encoding='utf-8-sig')
    
    print(f"모든 작업 종료 - {datetime.now()}")
    remove_folder(local_path)

def main_score(jobid,
         lang,
         project_name):
    print(f"데이터 다운로드 - {datetime.now()}")
    local_path = download(jobid, lang, project_name)
    
    print(f"압축풀기 - {datetime.now()}")
    extract_gzip_files(local_path)
    
    data = read_all_jsonl_file(local_path)
    
    print(f"스코어링 시작 - {datetime.now()}")
    score_df = trend_scoring.total_score(data, lang)
    
    if not os.path.exists(f"/data/data2/yj.lee/suggest/src/data/result/{lang}/{jobid[:8]}"):
        os.makedirs(f"/data/data2/yj.lee/suggest/src/data/result/{lang}/{jobid[:8]}")
    score_df.to_csv(f"/data/data2/yj.lee/suggest/src/data/result/{lang}/{jobid[:8]}/score_df.csv", index=False, encoding='utf-8-sig')
    
    print(f"모든 작업 종료 - {datetime.now()}")
    remove_folder(local_path)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str)
    parser.add_argument("--lang", type=str)
    parser.add_argument("--project", type=str)
    args = parser.parse_args()
    
    main(args.date, args.lang, args.project) # 스코어링
    main_category(args.date, args.lang)
    main_db(args.date, args.lang) # 디비 저장