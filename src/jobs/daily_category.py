# scoring한 결과 받아서 카테고리 지정
import argparse
import pandas as pd

from category.knn import category_knn

def main(date, lang):
    df_path = f"/data/data2/yj.lee/git/suggest/src/data/result/{lang}/{date}/score_df.csv"
    
    score_df = pd.read_csv(df_path)
    score_df['category'] = category_knn.predict(list(score_df['keyword']))
    score_df.to_csv(df_path, index=False, encoding='utf-8-sig')

def main_category(date, lang):
    df_path = f"/data/data2/yj.lee/git/suggest/src/data/result/{lang}/{date}/score_df.csv"
    
    score_df = pd.read_csv(df_path)
    score_df['category'] = category_knn.predict(list(score_df['keyword']))
    score_df.to_csv(df_path, index=False, encoding='utf-8-sig')
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str)
    parser.add_argument("--lang", type=str)
    args = parser.parse_args()
    main(args.date, args.lang)