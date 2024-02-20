import numpy as np
import pandas as pd
import faiss
import fasttext
from typing import Union

class CategoryKNN:
    def __init__(self):
        # 임베딩 모델
        embedding_model_path = "/data2/wordpopcorn/wordpopcorn/ko/model/fasttext/20230427000000/model.bin"
        self.embedding_model = fasttext.load_model(embedding_model_path)
        self.knn_data = pd.read_csv("/data/data2/yj.lee/suggest/src/category/data/ko/knn_data.csv")
        vectors = []
        for i in range(len(self.knn_data)):
            keyword = self.knn_data.loc[i, 'keyword']
            vectors.append(self.embedding_model.get_sentence_vector(keyword))
                
        self.index = faiss.IndexFlatIP(100) # 코사인 거리
        self.index.add(np.array(vectors))
    
    def search_index(self,
                     query_embeddings, 
                     k: int = 1):
        D, I = self.index.search(query_embeddings, k=k) 
    
        # 유사도 기준으로 결과 정렬
        sorted_indices = np.argsort(-D, axis=1)  # 유사도가 큰 것부터 정렬
        sorted_distances = -np.sort(-D, axis=1)    # 유사도를 다시 원래대로 되돌림
        sorted_neighbors = np.take_along_axis(I, sorted_indices, axis=1)

        return sorted_distances, sorted_neighbors
    
    def predict(self, 
                keywords : Union[list,str],
                k : int = 1,
                distance : bool = False):
        if type(keywords) == list:
            words_embedding = [self.embedding_model.get_sentence_vector(w) for w in keywords]
        else:
            words_embedding = [self.embedding_model.get_sentence_vector(keywords)]
        
        D, I = self.search_index(np.array(words_embedding), k)

        if distance == True:
            return [(D[i][0], 
                     self.knn_data.loc[I[i][0], 'keyword'], 
                     self.knn_data.loc[I[i][0], 'category']) 
                    for i in range(len(D))]
        else:
            return [self.knn_data.loc[I[i][0], 'category']
                    for i in range(len(D))]

category_knn = CategoryKNN() 

if __name__ =="__main__":
    print(category_knn.predict(["아이유",
                                '이종석' ,
                                '간호학과', 
                                '입시', 
                                '컴퓨터']))