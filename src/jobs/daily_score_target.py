import os
import argparse
import gzip
import shutil
from typing import List
from datetime import datetime
from utils.hdfs import HdfsFileHandler 
from utils.file import JsonlFileHandler, find_files_by_format, remove_folder
from score.trend_suggest_score import trend_scoring

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
    if project_name == "google_suggest_for_trend_target":
        local_path = f"/data/data2/yj.lee/suggest/src/data/tmp/{lang}/{date}/target"
        
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    
    hdfs.download(hdfs_path, local_path)
    
    return local_path

def target_list(folder_path):
    # 폴더 내의 모든 파일 및 하위 폴더 리스트 가져오기
    all_targets = []
    all_files = []
    for root, dirs, files in os.walk(folder_path):
        all_files.extend([os.path.join(root, file) for file in files])
        
    # 'jsonl.gz'로 끝나는 파일 필터링 및 압축 해제
    for file_path in filter(lambda x: x.endswith('.jsonl.gz'), all_files):
        all_targets.append(file_path.split('/')[-1].split('.')[0])
    
    return list(set(all_targets))

def find_target_files_by_format(folder_path,
                                format,
                                target):
    # 폴더 내의 모든 파일 및 하위 폴더 리스트 가져오기
    all_files = []
    for root, dirs, files in os.walk(folder_path):
        all_files.extend([os.path.join(root, file) for file in files])

    # 'format'로 끝나는 파일 필터링
    format_files = [file for file in all_files if (file.split('/')[-1] == f"{target}.{format}")]

    return format_files

def read_target_jsonl_file(folder_path,
                           target) -> List[dict]:
    data = []
    jsonl_files = find_target_files_by_format(folder_path, "jsonl", target) # folder_path에 있는 모든 jsonl 파일 읽기
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
    targets = target_list(local_path)
    
    print(f"압축풀기 - {datetime.now()}")
    extract_gzip_files(local_path)
    for target in targets:
        print(f"target : {target}")
        data = read_target_jsonl_file(local_path, target)
        print(f"  데이터 개수 : {len(data)}")
        print(f"  스코어링 시작 - {datetime.now()}")
        score_df = trend_scoring.total_score(data, lang, target.replace('_', ' '))
        
        if not os.path.exists(f"/data/data2/yj.lee/suggest/src/data/result/{lang}/{jobid[:8]}"):
            os.makedirs(f"/data/data2/yj.lee/suggest/src/data/result/{lang}/{jobid[:8]}")
        score_df.to_csv(f"/data/data2/yj.lee/suggest/src/data/result/{lang}/{jobid[:8]}/score_{target}_df.csv", index=False, encoding='utf-8-sig')
        print(f"  스코어링 완료 - {datetime.now()}")
        
    print(f"모든 작업 종료 - {datetime.now()}")
    remove_folder(local_path)

def main_score_target(jobid,
                      lang,
                      project_name):
    main(jobid,
         lang,
         project_name)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str)
    parser.add_argument("--lang", type=str)
    parser.add_argument("--project", type=str)
    args = parser.parse_args()
    
    main(args.date, args.lang, args.project)
   