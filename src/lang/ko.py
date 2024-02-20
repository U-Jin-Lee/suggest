import string

from lang.lang_base import LanguageBase
from utils.file import PickleFileHandler

class Ko(LanguageBase):
    def __init__(self) -> None:
        super().__init__()
        self.complete_hanguls = PickleFileHandler("/data/data2/yj.lee/suggest/src/lang/ko/data/characters.pickle").read()

        self.alphabets = self.get_alphabets()
        self.numbers = self.get_numbers()
        self.characters = self.get_characters()
        self.letters = self.get_letters()
    
    def language(self) -> str: 
        return 'ko'

    @property
    def hl(self) -> str:
        return 'ko'

    @property
    def gl(self) -> str:
        return 'kr'
    
    def get_none(self) -> list:
        return [""]
    
    def get_characters(self) -> list:
        return list(self.complete_hanguls)

    def get_letters(self) -> list:
        return ["ㄱ","ㄲ","ㄴ","ㄷ","ㄸ","ㄹ","ㅁ","ㅂ","ㅃ","ㅅ","ㅆ","ㅇ","ㅈ","ㅉ","ㅊ","ㅋ","ㅌ","ㅍ","ㅎ"]

    def get_alphabets(self) -> list:
        return list(string.ascii_lowercase)
        #return frozenset(list(string.ascii_lowercase))

    def get_numbers(self) -> list:
        return [str(n) for n in list(range(0,10))]
        #return frozenset(list(range(0,10)))
    
    def suggest_extension_texts_by_rank(self, rank) -> list:
        if rank == 0:
            return self.get_none()
        elif rank == 1:
            return self.get_letters() + self.get_alphabets() + self.get_numbers()
        elif rank == 2:
            return self.get_characters()
        elif rank == 3:
            return [x + y \
                    for x in self.get_characters() \
                    for y in (self.get_letters() + self.get_alphabets() + self.get_numbers())
                    ]
        elif rank == 4:
            return [x + y \
                    for x in self.get_characters() \
                    for y in self.get_characters()
                    ]
     
    def suggest_extension_texts(self, 
                                stratgy : str = "all",
                                contain_none : bool = False) -> list:
        if stratgy == "all":
            if contain_none == True:
                return self.get_none() + \
                       self.suggest_extension_texts_by_rank(1) + \
                       self.suggest_extension_texts_by_rank(2) + \
                       self.suggest_extension_texts_by_rank(3) + \
                       self.suggest_extension_texts_by_rank(4)
            else:
                return self.suggest_extension_texts_by_rank(1) + \
                       self.suggest_extension_texts_by_rank(2) + \
                       self.suggest_extension_texts_by_rank(3) + \
                       self.suggest_extension_texts_by_rank(4)


    
if __name__ == "__main__":
    ko = Ko()
    print(ko.hl , ko.gl)
    