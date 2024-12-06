from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta

from bs4 import BeautifulSoup

import os
os.environ['DISPLAY'] = ':0'

# BeritaHarina max page = 20 only (as of 5 Dec 2024)
class BHarianScraper:
    __month_mapping = {
        "Jan": 1, "Feb": 2, "Mac": 3, "Apr": 4, "Mei": 5, "Jun": 6,
        "Jul": 7, "Ogs": 8, "Sep": 9, "Okt": 10, "Nov": 11, "Dis": 12
    }
    
    def __init__(self,category = None):
        self.baseUrl = 'https://www.bharian.com.my'
        self.category = category
        self.options = Options()
        self.options.add_argument("--headless")  # Run in headless mode
        self.options.add_argument("--disable-gpu")  # Disable GPU acceleration
        self.options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
        self.options.add_argument("--incognito") 
        self.options.add_argument('user-agent=Mozilla/5.1 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        self.service = Service(ChromeDriverManager().install())
        self.driver = None
        self.relLinkToArticle_all = []
        self.fromDate = None
        self.toDateTime = None
    
    def scrapArticleLinks(self):
        # #if category not specified raise exceptions
        # if category == None and self.category == None:
        #     raise CategoryError(category,f"Category is invalid. Category=\'{self.category}\'. Please specify category in the form of url appending string.")

        # #Assign new cat if new
        # self.category = category if category is not None else self.category
        # self.fromDate = fromDate
        # self.toDateTime = datetime.now()
        self.relLinkToArticle_all = []
        article_teasers=[]
        #Web Crawl Begins
        for i in range (0,21):
            try:
                self.driver = webdriver.Chrome(service=self.service, options=self.options)
                self.driver.get(f"{self.baseUrl}{self.category}?page={i}")
                self.driver.implicitly_wait(10)

                try:
                    # Wait for the div with class article-teaser to appear
                    WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'article-teaser'))
                    )
                    print(f"Article teaser found on page {i}.")
                except Exception as e:
                    print(f"Error: {e}")
                    return None
                # Get the page HTML Text 
                page_source = self.driver.page_source
                soup = BeautifulSoup(page_source, "html.parser")
                article_teasers = soup.find_all(
                    lambda tag: tag.name == 'div' 
                    and 'article-teaser' in tag.get('class', []) 
                    and 'd-block' not in tag.get('class', [])
                )
                # for content in article_teasers:
                #     print(content.prettify())
                # print(len(article_teasers))
            finally:
                self.driver.quit()
    
            # scrap all info
            # Due to every page last teaser is the next page first teaser
            # We have to remove the 1st teasers when we at page 1
            if i !=0:
                article_teasers.pop(0)
            for teaser in article_teasers:
                # get article create time
                span_PublishTimeInfo = teaser.find('span', class_='created-ago')
                timePublish_text = span_PublishTimeInfo.text.strip()
    
                # get article link
                aTag = teaser.find('a',class_='d-flex article listing mb-3 pb-3')
                relLinkToArticle = aTag['href']
                
                # check time within range
                timePublish_datetime = self.convertTimeStringToDateTime(timePublish_text,toDateTime=self.toDateTime)
                if(timePublish_datetime > self.fromDate):
                    self.relLinkToArticle_all.append(relLinkToArticle)
                else:
                    return self.relLinkToArticle_all
        


    def convertTimeStringToDateTime(self, timePublish_text,toDateTime):
        if '@' in timePublish_text:
            pratitonedTimeTxt = timePublish_text.split()
            month = BHarianScraper.__month_mapping[pratitonedTimeTxt[0]]  # Convert "Dis" to 12
            day = int(pratitonedTimeTxt[1].strip(','))
            year = int(pratitonedTimeTxt[2])
            time_str = pratitonedTimeTxt[4]
            time_format = "%I:%M%p"
            return (datetime.strptime(f"{year}-{month}-{day} {time_str}", "%Y-%m-%d %I:%M%p"))
        elif 'sejam' in timePublish_text:
            return (toDateTime - timedelta(hours=1))
        elif 'jam' in timePublish_text:
            hoursAgoInt = int(timePublish_text.split()[0])
            return (toDateTime - timedelta(hours=hoursAgoInt))
        # minit before
        else:
            minutesAgoInt = int(timePublish_text.split()[0])
            return (toDateTime - timedelta(minutes=minutesAgoInt))
            
    def scrapArticles(self,fromDate, category = None):
        #if category not specified raise exceptions
        if category == None and self.category == None:
            raise CategoryError(category,f"Category is invalid. Category=\'{self.category}\'. Please specify category in the form of url appending string.")

        #Assign new cat if new
        self.category = category if category is not None else self.category
        self.fromDate = fromDate
        self.toDateTime = datetime.now()
        self.relLinkToArticle_all = self.scrapArticleLinks()
        scrappedResults = [] 
        count = 1
        print(f'Scrapping {len(self.relLinkToArticle_all)} article links ...')
        for link in self.relLinkToArticle_all:
            try:
                scrappedOneResult = self.scrapOneArticle(link)
            except Exception as e: 
                print('exception occur')
                print(f'{count}. fail to scrap on {link}')
            if scrappedOneResult:
                scrappedResults.append(scrappedOneResult)
                print(f"{len(scrappedResults)}. Scrap success on url: \'{link}\'")
            count += 1

        return scrappedResults

    def scrapOneArticle(self,url):
        soup = None
        try:
            self.driver = webdriver.Chrome(service=self.service, options=self.options)
            destUrl = self.baseUrl + url
            self.driver.get(destUrl)
            self.driver.implicitly_wait(10)
            # try:
            #     # Wait for the div with class article-teaser to appear
            #     WebDriverWait(self.driver, 20).until(
            #         EC.presence_of_element_located((By.CLASS_NAME, 'article-teaser'))
            #     )
            #     print("Article teaser found.")
            # except Exception as e:
            #     print(f"Error: {e}")
            #     return None
            # Get the page HTML Text 
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, "html.parser")
            
        finally:
            self.driver.quit()

        #get Article Title
        title = ''
        titleEle = soup.find('span', class_='d-inline-block mr-1')
        try:
            title = titleEle.get_text()
        except Exception as e:
            #do nothing cuz ady initialize title
            pass

        author = ''
        authorRelativeLink = ''
        author_email = ''
        authorDict = {}
        
        try:
            #get authName
            author_n_dateTimeEle = soup.find('div',class_='article-meta mb-2 mb-lg-0 d-flex align-items-center')
            author_aTag = author_n_dateTimeEle.find('a')
            author = author_aTag.get_text()
            try:
                #get author link in BH
                authorRelativeLink = author_aTag['href']
                author_aTag.decompose()
                try:
                    email_aTag = author_n_dateTimeEle.find('a')
                    author_email = email_aTag.get_text()
                    email_aTag.decompose()
                except Exception as e:
                    #do nothing cuz ady initialize auth_email
                    pass
            except Exception as e:
                #do nothing cuz ady initialize author_aTag
                pass
        except Exception as e:
            #do nothing cuz ady initialize authorDict
            pass

        authorDict = {"authName":author,"relLink":authorRelativeLink,"email":author_email}
        
        # get date time
        haveSpanIfHaveAuthor = author_n_dateTimeEle.find('span')
        if haveSpanIfHaveAuthor is not None:
            author_n_dateTimeEle.find('span').decompose()
        publishTime_txt = author_n_dateTimeEle.get_text().strip()
        
        # get article brief
        articleBrief = ''
        articleBrief_ele = soup.find('figcaption',class_='p-2')
        try:
            articleBrief = articleBrief_ele.get_text()
        except Exception as e:
            #do nothing cuz ady initialize articleBrief
            pass
        

        # get article content
        articleContent_soup = soup.find('div',class_='dable-content-wrapper')
        articleContents = self.getArticleContents(articleContent_soup)

        scrappedResults = {"Title":title,"authDetail":authorDict,
                           "publishTime":publishTime_txt,"articleBrief":articleBrief,
                           "contents":articleContents,"url":url}

        return scrappedResults
        
    def getArticleContents(self,soup):
        #remove uncessary div if any
        contents = []
        divToRm = soup.find_all('div',class_='google-anno-skip google-anno-sc')
        for div in divToRm:
            div.decompose()
        all_para = soup.find_all('p')
        filtered_para_all = [p for p in all_para if p.get_text()]
        for p in filtered_para_all:
            p.string = p.get_text().replace('\xa0', ' ')
            contents.append(p.get_text())
        return contents
        
        
class CategoryError(Exception):
    def __init__(self, category, message="Category is invalid."):
        self.category = category
        self.message = message
        super().__init__(self.message)
