import requests
from bs4 import BeautifulSoup
import re
from .prpm_cleanner import PRPMCleaner

# PRPM web scraping
class PRPMScraper:
    def __init__(self):
        self.baseUrl = 'https://prpm.dbp.gov.my/Cari1?keyword='
        self.word = None
        self.meanings = None
        self.synonym = None
        self.antonym = None
        self.cleaner = PRPMCleaner()

    def findWordMetaData(self, word):
        urlReq = self.baseUrl + word
        responsedHTML = requests.get(urlReq)
        soup = BeautifulSoup(responsedHTML.content, 'html.parser')
        self.word = word
        self.meanings = self.getWordMeanings(soup)
        self.synonym = self.getSynonym(soup)
        self.antonym = self.getAntonym(soup)
        if self.meanings:
            self.meanings = self.cleaner.clean_all_meanings(self.meanings)
            return {"word":self.word, "meanings": self.meanings, "synonym": self.synonym, "antonym": self.antonym}
        return None

    def getWordMeanings(self, soup):
        meanings = []
        tabs = soup.find_all('div', class_="tab-pane")

        for tab in tabs:
            text = tab.get_text()
            index = 1  # Start with detecting 1
            current_number = None # To prevent a 2nd "1" 
            segments = re.split(r'\b(\d+)\b\.\s*', text) 
            start_index = text.find("Definisi :")
            findOne = re.search(r'\b1\b', text)
            
            if not findOne:
                if start_index != -1: 
                    definition = text[start_index + len("Definisi :"):].strip()
                    first_meaning = re.split(r'[;:]', definition)[0].strip()
                    meanings.append(first_meaning)
                    continue
            else: 
                for i in range(1, len(segments), 2):
                    number = int(segments[i])  
                    content = segments[i + 1] 
    
                    if current_number is None:
                        current_number = number
    
                    if number == 1 and current_number != 1:
                        continue
    
                    if number == index:
                        meaning = re.split(r'[:;]', content)[0].strip()
                        meanings.append(meaning)
                        index += 1  
                    else:
                        continue 
                
        return(meanings)


    def getSynonym(self, soup):
        synonymList = []
        contents = soup.find_all('b', string='Bersinonim dengan')
        for content in contents:
            for sibling in content.find_all_next(string=False):
                if sibling.name == 'a': 
                    synonymList.append(sibling.text.strip())
                elif sibling.name == 'b':
                    break
        return synonymList

    
    def getAntonym(self, soup):
        antonyms = []
        contents = soup.find_all('b', string='Berantonim dengan')
        for content in contents:
            for sibling in content.find_all_next(string=False):
                if sibling.name == 'a':  
                    antonyms.append(sibling.text.strip())
                elif sibling.name == 'b':  
                    break
    
        return antonyms







        