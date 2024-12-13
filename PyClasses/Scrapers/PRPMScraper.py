import requests
from bs4 import BeautifulSoup
import re

# PRPM web scraping
class PRPMScraper:
    def __init__(self):
        self.baseUrl = 'https://prpm.dbp.gov.my/Cari1?keyword='
        self.word = None
        self.meanings = None
        self.synonym = None

    def findWordMetaData(self, word):
        urlReq = self.baseUrl + word
        responsedHTML = requests.get(urlReq)
        soup = BeautifulSoup(responsedHTML.content, 'html.parser')
        self.word = word
        self.meanings = self.getWordMeanings(soup)
        self.synonym = self.getSynonym(soup)
        if self.meanings:
            return {self.word: {"meanings": self.meanings, "synonym": self.synonym}}
        return None

    def getWordMeanings(self, soup):
        meanings = []
        tabs = soup.find_all('div', class_="tab-pane")

        for tab in tabs:
            text = tab.get_text()
            index = 1  # Start with detecting 1

            segments = re.split(r'\b(\d+)\b\.\s*', text)
            current_number = None

            findOne = re.search(r'\b1\b', text)
            start_index = text.find("Definisi :")
            
            if not findOne:
                if start_index != -1: 
                    definition = text[start_index + len("Definisi :"):].strip()
                    first_meaning = re.split(r'[;:]', definition)[0].strip()
                    meanings.append(first_meaning)
                    break
           
            if findOne:    
                for i in range(1, len(segments), 2):
                    number = int(segments[i])  
                    content = segments[i + 1] 
    
                    if current_number is None:
                        current_number = number
    
                    if number == 1 and current_number != 1:
                        break
    
                    if number == index:
                        meaning = re.split(r'[:;]', content)[0].strip()
                        meanings.append(meaning)
                        index += 1  
                    else:
                        break  
        return meanings


    def getSynonym(self, soup):
        synonymList = []
        contents = soup.find_all('b', string='Bersinonim dengan ')
        for content in contents:
            all_synonyms = content.find_next_siblings('a')
            for synonym in all_synonyms:
                synonymList.append(synonym.text.strip())
        return synonymList
