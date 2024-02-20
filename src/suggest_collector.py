from dataclasses import dataclass
import json
import requests
from http import HTTPStatus
from multiprocessing import Pool
from tenacity import retry, stop_after_attempt
from typing import TYPE_CHECKING

from lang import Ko, Ja

if TYPE_CHECKING:
    from lang import LanguageBase

@dataclass(frozen=True)
class SuggestApiParams:
    query: str
    hl: str 
    gl: str  
    expand_mode: str
    pre_expand_keyword: str = None
    ds:str = 'google'

@retry #(stop=(stop_after_attempt(10))) # 10번 시도했는데 계속 retry조건에 맞으면 에러를 발생함 -> 어떻게 대처?
def get_suggestions(suggest_api_params : SuggestApiParams):
    suggestions = []
    url = "http://google-suggest-api.ascentlab.io/api/suggest/v2/suggestions"
    _payload = {
            "q" : f"{suggest_api_params.query}",
            "hl": f"{suggest_api_params.hl}",
            "gl": f"{suggest_api_params.gl}",
        }
    if suggest_api_params.pre_expand_keyword:
        _payload['q'] = suggest_api_params.query
        _payload['pre_expand_keyword'] = suggest_api_params.pre_expand_keyword

    payload = json.dumps(_payload)       
    headers = {"Content-Type": "application/json"}

    response = requests.post(url, headers=headers, data=payload, timeout=180.0)
    status_code = response.status_code
    
    if status_code == HTTPStatus.OK:
        suggestions = json.loads(response.text)
        return suggestions
    else:
        print(f"fail to get suggest - retry : {_payload}")
        raise Exception("error!")

def get_target_suggest(suggest_api_params : SuggestApiParams):
    return get_suggestions(suggest_api_params)

class Suggest:
    def __init__(self):
        self.lang_dict = {"ko":Ko(),
                          "ja":Ja()}    
    
    def _requests(self,
                  targets,
                  lang,
                  num_processes : int = 35) -> dict:
        lang : LanguageBase = self.lang_dict[lang]
        targets = [SuggestApiParams(query=t, 
                                    hl=lang.hl,
                                    gl=lang.gl,
                                    expand_mode='exact') \
                                    for t in targets]
        with Pool(num_processes) as pool:
            result = pool.map(get_suggestions, targets)
            # result = pool.map(get_target_suggest, targets)
        return result
    
if __name__ == "__main__":
    from utils.file import JsonlFileHandler
    suggest = Suggest()
    res = suggest._requests([""], 'ko')
    print(res)
    JsonlFileHandler("/data/data1/share/notebooks/yj.lee-notebooks/서제스트 트렌드 키워드/non_test_ko.jsonl").write(res)
    res = suggest._requests([""], 'ja')
    print(res)
    JsonlFileHandler("/data/data1/share/notebooks/yj.lee-notebooks/서제스트 트렌드 키워드/non_test_ja.jsonl").write(res)