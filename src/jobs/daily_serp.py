import pandas as pd
from utils.kafka import request_collect_serp_to_kafka

def main_serp(jobid,
              lang):
    data = pd.read_csv(f"/data/data2/yj.lee/git/suggest/src/data/result/{lang}/{jobid[:8]}/score_df.csv")
    keywords = list(data['keyword'])
    for k in keywords:
        request_collect_serp_to_kafka(k)