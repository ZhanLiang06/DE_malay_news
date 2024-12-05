import requests
from bs4 import BeautifulSoup


##PRPM web scraping
class PRPMScraper:
    def __init__(self):
        self.baseUrl = 'https://prpm.dbp.gov.my/Cari1?keyword='
        self.word = None
        self.meanings = None
        self.sinonim = None

    def findWordMetaData(self, word):
        urlReq = self.baseUrl + word
        responsedHTML = requests.get(urlReq)
        soup = BeautifulSoup(responsedHTML.content,'html.parser')
        self.word = word
        self.meanings = self.getWordMeanings(soup)
        self.sinonim = self.getSinonim(soup)
        if self.meanings:
            return {self.word: {"meanings": self.meanings, "sinonim": self.sinonim}}
        return None
    
    def getWordMeanings(self, soup):
        meanings = []
        contents = soup.find_all('div', class_='tab-pane')
        for content in contents:
            defiTab = content.find('b', string='Definisi : ')
            meanings.append(defiTab.next_sibling.strip())
        return(meanings)
    
    def getSinonim(self, soup):
        sinonimList = []
        contents = soup.find_all('b', string='Bersinonim dengan ')
        print(contents)
        for content in contents:
            sinonim = content.find_next_sibling('a').text
            sinonimList.append(sinonim)
        return sinonimList
    
    # def resultExits(soup):
    #     content = soup.find('table', class_='info')
    #     result = content.find('b').text
    #     print(result)
    #     if('Tiada maklumat' in result):
    #         return False
    #     return True

    # def getPRPMSearchResult(record):
    #     return dict([self.word, dict(meanings = self.meanings,sinonim=self.sinonim)])