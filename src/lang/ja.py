import string
from lang.lang_base import LanguageBase

class Ja(LanguageBase):
    def __init__(self) -> None:
        super().__init__()
        self.alphabets = self.get_alphabets()
        self.numbers = self.get_numbers()
        self.letters = self.get_letters()
    
    def language(self) -> str: 
        return 'ja'

    @property
    def hl(self) -> str:
        return 'ja'

    @property
    def gl(self) -> str:
        return 'jp'
    
    def get_none(self) -> list:
        return [""]
    
    def get_characters(self) -> list:
        pass
    
    def get_letters(self) -> list:
        return ['あ', 'い', 'う', 'え', 'お',
                'か', 'き', 'く', 'け', 'こ',
                'さ', 'し', 'す', 'せ', 'そ',
                'た', 'ち', 'つ', 'て', 'と',
                'な', 'に', 'ぬ', 'ね', 'の',
                'は', 'ひ', 'ふ', 'へ', 'ほ',
                'ま', 'み', 'む', 'め', 'も',
                'や',       'ゆ',       'よ',
                'ら', 'り', 'る', 'れ', 'ろ',
                'わ',                   'を',
                            'ん'
                ]

    def get_alphabets(self) -> list:
        return list(string.ascii_lowercase)

    def get_numbers(self) -> list:
        return [str(n) for n in list(range(0,10))]
    
    def suggest_extension_texts_by_rank(self, rank) -> list:
        if rank == 0:
            return self.get_none()
        elif rank == 1:
            return self.get_letters() + self.get_alphabets() + self.get_numbers()
        elif rank == 2:
            targets = self.get_letters() + self.get_alphabets() + self.get_numbers()
            return [x + y \
                    for x in targets \
                    for y in targets
                    ]
        elif rank == 3:
            targets = self.get_letters() + self.get_alphabets() + self.get_numbers()
            return [x + y + z \
                    for x in targets \
                    for y in targets \
                    for z in targets
                    ]
            
            
    def suggest_extension_texts(self, 
                                stratgy : str = "all",
                                contain_none : bool = False) -> list:
        if stratgy == "all":
            if contain_none:
                return self.get_none() + \
                       self.get_letters() + \
                       self.get_alphabets() + \
                       self.get_numbers()
            else:
                return self.get_letters() + \
                       self.get_alphabets() + \
                       self.get_numbers()

    
if __name__ == "__main__":
    ja = Ja()
    print(ja.hl, ja.gl)
    print(len(ja.suggest_extension_texts_by_rank(1)),
          ja.suggest_extension_texts_by_rank(1))
    