import os
import json
import pickle
import jsonlines
import shutil
from typing import Union, List

class PickleFileHandler:
    def __init__(self, path):
        self.path = path

    def read(self):
        with open(self.path, 'rb') as f:
            return pickle.load(f)

    def write(self, obj):
        with open(self.path, 'wb') as f:
            pickle.dump(obj, f)

class JsonlFileHandler:
    def __init__(self, path):
        self.path = path
    
    def read(self, line_len : int = None):
        data = []
        cnt = 0
        try:
            with jsonlines.open(self.path) as f:
                for line in f:
                    cnt += 1
                    data.append(line)
                    if (line_len != None) & (line_len == cnt):
                        break
        except Exception as e:
            print(self.path, str(e))
        finally:
            return data
    
    def write(self, data : Union[dict, List[dict]]):
        try:
            if type(data) == dict: # 하나의 데이터 입력
                with open(self.path, "a", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False) # ensure_ascii로 한글이 깨지지 않게 저장
                    f.write("\n") # json을 쓰는 것과 같지만, 여러 줄을 써주는 것이므로 "\n"을 붙여준다.
            elif type(data) == list: # 여러 데이터 입력
                with open(self.path, "a", encoding="utf-8") as f:
                    for d in data:
                        json.dump(d, f, ensure_ascii=False) # ensure_ascii로 한글이 깨지지 않게 저장
                        f.write("\n") # json을 쓰는 것과 같지만, 여러 줄을 써주는 것이므로 "\n"을 붙여준다.
        except Exception as e:
            print(self.path, str(e))

def gzip(file : str):
    try:
        cmd = f"gzip {file}"
        os.system(cmd)
    except Exception as e:
        print(f"fail gzip [{file}] file", e)
        return None
    else:
        print(f"success gzip [{file}] file")
        return file + '.gz'

def ungzip(file : str):
    if file.endswith(".gz"):
        cmd = f"gzip -d {file}"
        os.system(cmd)
        return file[:-3] # 뒤에 ".gz" 빼고 반환
    else:
        print(f"not .gz file : {file}")
            
def zip_folder(folder):
    cmd = f"zip -r {folder}.zip {folder}"
    os.system(cmd)
    return f"{folder}.zip"

def unzip_folder(zip_folder, dest_folder):
    '''
    zip_folder안에 있는 파일 혹은 폴더들을 dest_folder에 압축 풀기
    '''
    cmd = f"unzip -j {zip_folder} -d {dest_folder}"
    os.system(cmd)
    return dest_folder

def remove_folder(folder):
    cmd = f"rm -rf {folder}"
    os.system(cmd)

def find_files_by_format(folder_path,
                        format):
    # 폴더 내의 모든 파일 및 하위 폴더 리스트 가져오기
    all_files = []
    for root, dirs, files in os.walk(folder_path):
        all_files.extend([os.path.join(root, file) for file in files])

    # 'format'로 끝나는 파일 필터링
    format_files = [file for file in all_files if file.endswith(format)]

    return format_files

if __name__ == "__main__":
    extract_gzip_files("/data/data2/yj.lee/git/suggest/src/data/tmp/ko/20240207")