import requests
from bs4 import BeautifulSoup

class WikiScraper:
    def __init__(self):
        self.baseUrl = 'https://ms.wiktionary.org/wiki/'
        self.word = None
        self.meanings = None

    def findWordMetaData(self, word):
        try:
            urlReq = self.baseUrl + word
            response = requests.get(urlReq)
            response.raise_for_status()  
            
            soup = BeautifulSoup(response.content, 'html.parser')
            self.word = word
            self.meanings = self.getWordMeanings(soup)
            
            if self.meanings:
                return {self.word: {"WikiDefi": self.meanings}}
            return None
        except requests.exceptions.RequestException as e:
            print(f"Error fetching word '{word}': {e}")
            return None

    def getWordMeanings(self, soup):
        meanings = []  
        try:
            bahasa_melayu_section = None
            for h2 in soup.find_all('h2'):
                if 'Bahasa Melayu' in h2.text:
                    bahasa_melayu_section = h2
                    break
            
            if bahasa_melayu_section:
                meaning_list = bahasa_melayu_section.find_next('ol')
                if meaning_list:
                    meanings = [li.text.strip() for li in meaning_list.find_all('li')]
        except Exception as e:
            print(f"Error extracting meanings: {e}")
        return meanings

    
