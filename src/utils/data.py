from typing import List

def combine_dictionary(dict_list : List[dict]) -> dict:
    # 결과를 저장할 빈 딕셔너리
    result_dict = {}

    # 딕셔너리 리스트를 순회하면서 합치기
    for d in dict_list:
        for key, value in d.items():
            if key in result_dict:
                result_dict[key].extend(value)
            else:
                result_dict[key] = value

    return result_dict