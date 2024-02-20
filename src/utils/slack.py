import json
import requests
from datetime import datetime
from dataclasses import dataclass

@dataclass
class DailyConfig:
    token:str='xoxb-15302442678-3538976572647-btpgiPrgP2gKnSJOrnUBBFxw' #Daily Bot
    channel:str='#ds-daily'
    
@dataclass
class DailyErrorConfig:
    token:str='xoxb-15302442678-3924017195808-ktK9jtVjYzfDxPGap9YkpMwG' #Daily Error Bot
    channel:str='#ds-daily-error'
    
def post_message(token, channel, text):
    response = requests.post("https://slack.com/api/chat.postMessage",
        headers={"Authorization": "Bearer "+token},
        data={"channel": channel,"text": text}
    )
    if not json.loads(response.text)["ok"]:
        raise Exception(response.text)

def ds_daily_dbgout(message):
    try:
        myToken = 'xoxb-15302442678-3538976572647-btpgiPrgP2gKnSJOrnUBBFxw' #Daily Bot
        strbuf = datetime.now().strftime('[%m/%d %H:%M:%S] ') + message
        post_message(myToken,"#ds-daily", strbuf)
    except Exception as e:
        print(e)

def ds_daily_dbgout_error(message):
    myToken = 'xoxb-15302442678-3924017195808-ktK9jtVjYzfDxPGap9YkpMwG' #Daily Error Bot
    strbuf = datetime.now().strftime('[%m/%d %H:%M:%S] ') + message
    post_message(myToken,"#ds-daily-error", strbuf)

def test_dbgout(message):
    # 테스트용
    myToken = 'xoxb-15302442678-4288918455155-kKiVzb8hJ2FQYNYuYwKW7TWv'
    strbuf = datetime.now().strftime('[%m/%d %H:%M:%S] ') + message
    post_message(myToken,'#ds-test-2', strbuf)
    
# if __name__ == '__main__':
    # test_dbgout('test')