prefix_message = f"🌟suggest_for_trend_keyword msg"

import requests

# hdfs 접속 확인
params = {'op': 'GETFILESTATUS'}
response_1 =requests.head('http://master001.hadoop.prod.ascentlab.io:50070/webhdfs/v1/user/ds', params=params)
response_2 =requests.head('http://master002.hadoop.prod.ascentlab.io:50070/webhdfs/v1/user/ds', params=params)

if (response_1.status_code == 200) & (response_2.status_code == 403):
    hdfs_host = 'http://master001.hadoop.prod.ascentlab.io:50070'
    print(f"\nhdfs_host : {hdfs_host}\n")
elif (response_1.status_code == 403) & (response_2.status_code == 200):
    hdfs_host = 'http://master002.hadoop.prod.ascentlab.io:50070'
    print(f"\nhdfs_host : {hdfs_host}\n")
else:
    print("\nhdfs host 확인 필요\n")
    raise Exception('hdfs host 확인 필요')


config = {
    'project_name' : "suggest_for_trend_keyword",
    'hdfs_host' : hdfs_host,
    'hdfs_user' : "ds"
}