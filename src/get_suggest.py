import os
import argparse
from datetime import datetime

from utils.file import PickleFileHandler
from suggest_collector import Suggest
            
def main(date : str, # yyyymmdd
         target_keyword : str):
    suggest = Suggest()
    if target_keyword == None:
        result_folder_path = f"/data/data2/yj.lee/suggest/src/data/result/{date}/basic"
    else:
        result_folder_path = f"/data/data2/yj.lee/suggest/src/data/result/{date}/{target_keyword}"
    if not os.path.exists(result_folder_path):
        # os.mkdir(result_folder_path)
        os.makedirs(result_folder_path)
    for rank in range(1, 5):
        result_file_path = f"rank{rank}.pickle"
        start = datetime.now()
        print(f"rank{rank} start - {start}")
        result = suggest.get_suggest(rank=rank, target_keyword=target_keyword)
        print(f"   ㄴ finish : {datetime.now()-start}")
        PickleFileHandler(f"{result_folder_path}/{result_file_path}").write(result)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target_keyword", type=str, default=None)
    args = parser.parse_args()
    
    today = datetime.now().strftime("%Y%m%d")
    main(today,
         args.target_keyword)