from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
import traceback
from pyspark.sql import Row

from bs4 import BeautifulSoup

import os
os.environ['DISPLAY'] = ':0'

# BeritaHarina max page = 20 only (as of 5 Dec 2024)
class BHarianScraper:
    __month_mapping = {
        "Jan": 1, "Feb": 2, "Mac": 3, "Apr": 4, "Mei": 5, "Jun": 6,
        "Jul": 7, "Ogs": 8, "Sep": 9, "Okt": 10, "Nov": 11, "Dis": 12
    }

    linksScrapAttempts = 0
    
    def __init__(self):
        self.baseUrl = 'https://www.bharian.com.my'
        self.options = Options()
        self.options.add_argument("--headless")  # Run in headless mode
        self.options.add_argument("--disable-gpu")  # Disable GPU acceleration
        self.options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
        self.options.add_argument("--incognito") 
        self.options.add_argument("--no-proxy-server")
        self.options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        self.service = Service(ChromeDriverManager().install())
        self.driver = None
        self.relLinkToArticle_all = []
        self.toDateTime = None

    @classmethod
    def scrapOneArticle(cls,url):
        try:
            return BHarianScraper.scrapOneArticleNoTryCatch(url)
        except:
            traceback.print_exc()
            print(f"Fail to scrap \'{url}\' due to function error")
            return None
            
    
    @classmethod
    def scrapOneArticleNoTryCatch(cls,url):
        scraperInstance = BHarianScraper()
        soup = None
        for i in range(1,4):
            try:
                scraperInstance.driver = webdriver.Chrome(service=scraperInstance.service, options=scraperInstance.options)
                destUrl = scraperInstance.baseUrl + url
                scraperInstance.driver.get(destUrl)
                scraperInstance.driver.implicitly_wait(3)
                # try:
                #     # Wait for the div with class article-teaser to appear
                #     WebDriverWait(scraperInstance.driver, 20).until(
                #         EC.presence_of_element_located((By.CLASS_NAME, 'article-meta mb-2 mb-lg-0 d-flex align-items-center'))
                #     )
                # except Exception as e:
                #     print(f"Error: {e}")
                #     return None
                page_source = scraperInstance.driver.page_source
                soup = BeautifulSoup(page_source, "html.parser")
                break;
            except:
                print(f'Timeout error on \'{url}\' {i} time(s)')
                if i == 3:
                    print(f'Fail to scrap on \'{url}\' {i} due to timeout')
                    return None
            finally:
                scraperInstance.driver.quit()
        

        #timeStart = datetime.now()
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

        ## There are scenario of having no HTML elements to scrap, thus it need to be handle, so far try catch will be used for more fault-tolerent application
        ## Future improvement includes change from try-catch to if-else for the purpose of reducing computation resources
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
        if not author_n_dateTimeEle: 
            print(f'No author_n_dateTimeEle \'{url}\' fail.')
            return None
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
        articleContents = scraperInstance.getArticleContents(articleContent_soup)

        # scrappedResults = {"title":title,"authDetail":authorDict,
        #                    "publishTime":publishTime_txt,"articleBrief":articleBrief,
        #                    "contents":articleContents,"url":url}
        # #print(f'Time Taken to process retrived HTML {datetime.now()-timeStart}')
        # if scrappedResults:
        #     print(f"Scrap success on url: \'{url}\'")
        # else:
        #     print(f'Fail to scrap on {url}')
        #     return Row(title='',
        #           authorDetails = {"authName":'',"relLink":'',"email":''},
        #           publishTime = '',
        #           articleBrief = '',
        #           contents = '',
        #           url = '')
        
        #print(f"Scrap success on url: \'{url}\'")
        return Row(title=title,
                  authorDetails = authorDict,
                  publishTime = publishTime_txt,
                  articleBrief = articleBrief,
                  contents = articleContents,
                  url = url)
        
    
    def scrapArticleLinks(self,fromDate, category):
        if not isinstance(fromDate,datetime):
            raise DataTypeError(fromDate)
        if not isinstance(category,str) or not category or category is None:
            raise CategoryError(category,f"Category is missing/invalid data type. Category=\'{self.category}\'. Please specify category in the form of url appending string.")
        self.fromDate = fromDate
        self.category = category
        # #if category not specified raise exceptions
        # if category == None and self.category == None:
        #     raise CategoryError(category,f"Category is invalid. Category=\'{self.category}\'. Please specify category in the form of url appending string.")

        # #Assign new cat if new
        # self.category = category if category is not None else self.category
        # self.fromDate = fromDate
        # self.toDateTime = datetime.now()
        self.relLinkToArticle_all = []
        article_teasers=[]
        self.toDateTime = datetime.now()
        #Web Crawl Begins
        self.driver = webdriver.Chrome(service=self.service, options=self.options)
        for i in range (0,21):
        #try:
            #self.driver = webdriver.Chrome(service=self.service, options=self.options)
            # Try for 3 times to load the data
            url = f"{self.baseUrl}{self.category}?page={i}"
            for tryAttempt in range(1,4):
                try:
                    self.driver.get(url)
                    self.driver.implicitly_wait(10)
                    # Wait for the div with class article-teaser to appear
                    WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'article-teaser'))
                    )
                    print(f"Article teaser found on page {i}.")

                    # Get the page HTML Text 
                    page_source = self.driver.page_source
                    soup = BeautifulSoup(page_source, "html.parser")
                    article_teasers = soup.find_all(
                        lambda tag: tag.name == 'div' 
                        and 'article-teaser' in tag.get('class', []) 
                        and 'd-block' not in tag.get('class', [])
                    )
                    break;
                except Exception as e:
                    print(f'Timeout error on \'{url}\' {tryAttempt} time(s)')
                    if i == 3:
                        print(f'Fail to load \'{url}\' {tryAttempt} due to timeout')
                        self.driver.quit()
                        return None
            # for content in article_teasers:
            #     print(content.prettify())
            # print(len(article_teasers))
        #finally:
            #self.driver.quit()

        # scrap all info
        # Due to every page last teaser is the next page first teaser
        # We have to remove the 1st teasers when we at page 1
            if i > 0 and i < 20:
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
                    print(f'Current no. of link: {len(self.relLinkToArticle_all)}')
                    self.driver.quit()
                    return self.relLinkToArticle_all
            print(f'Current no. of link: {len(self.relLinkToArticle_all)}')
        self.driver.quit()
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
        elif 'seminit' in timePublish_text:
            return (toDateTime - timedelta(minutes=1))
        # minits before
        else:
            minutesAgoInt = int(timePublish_text.split()[0])
            return (toDateTime - timedelta(minutes=minutesAgoInt))

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

class DataTypeError(Exception):
    def __init__(self, dataPassed, message="Datatype is invalid. Date must be in datetime format"):
        self.dataPassed = dataPassed
        self.message = message
        super().__init__(self.message)
        
class CategoryError(Exception):
    def __init__(self, category, message="Category is invalid."):
        self.category = category
        self.message = message
        super().__init__(self.message)

    # @classmethod
    # def scrapBatchArticles(cls, urls):
    #     scraperInstance = BHarianScraper()
    #     scrappedResults = [] 
    #     #Start chrome
    #     scraperInstance.driver = webdriver.Chrome(service=scraperInstance.service, options=scraperInstance.options)
    #     cls.linksScrapAttempts = 0
    #     for link in urls:
    #         scrappedOneResult = None
    #         try:
    #             cls.linksScrapAttempts += 1   
    #             print(link)
    #             scrappedOneResult = scraperInstance.scrapOneArticle(link)
    #         except Exception as e: 
    #             print('Fail to scrap on url: \'{link}\'')
    #             traceback.print_exc()
    #         if scrappedOneResult:
    #             scrappedResults.append(scrappedOneResult)            
    #             print('Scrap success on url: \'{link}\'')
    #         else:
    #             print('Fail to scrap on url: \'{link}\'')
                
        
    #     scraperInstance.driver.quit()
    #     return scrappedResults

            
    # def scrapArticles(self,fromDate = None, category = None):
    #     #if category not specified raise exceptions
    #     if category == None and self.category == None:
    #         raise CategoryError(category,f"Category is missing. Category=\'{self.category}\'. Please specify category in the form of url appending string.")
    #     if not isinstance(fromDate,datetime):
    #         raise DataTypeError(fromDate)
    #     #Assign new cat if new
    #     self.category = category if category is not None else self.category
    #     self.fromDate = fromDate
    #     self.toDateTime = datetime.now()
    #     self.relLinkToArticle_all = self.scrapArticleLinks()
    #     scrappedResults = [] 
    #     count = 1
    #     print(f'Scrapping {len(self.relLinkToArticle_all)} article links ...')
    #     #Start chrome
    #     self.driver = webdriver.Chrome(service=self.service, options=self.options)
    #     for link in self.relLinkToArticle_all:
    #         scrappedOneResult = None
    #         try:
    #             scrappedOneResult = self.scrapOneArticle(link)
    #         except Exception as e: 
    #             print(f'{count}. fail to scrap on {link}')
    #             traceback.print_exc()
    #         if scrappedOneResult:
    #             scrappedResults.append(scrappedOneResult)
    #             print(f"{len(scrappedResults)}. Scrap success on url: \'{link}\'")
    #         count += 1
        
    #     self.driver.quit()
    #     return scrappedResults

    
    # def scrapOneArticle(self,url):
    #     soup = None
    #     try:
    #         self.driver = webdriver.Chrome(service=self.service, options=self.options)
    #         destUrl = self.baseUrl + url
    #         self.driver.get(destUrl)
    #         self.driver.implicitly_wait(10)
    #         try:
    #             # Wait for the div with class article-teaser to appear
    #             WebDriverWait(self.driver, 20).until(
    #                 EC.presence_of_element_located((By.CLASS_NAME, 'article-meta mb-2 mb-lg-0 d-flex align-items-center'))
    #             )
    #             print(f"Article teaser found on page {i}.")
    #         except Exception as e:
    #             print(f"Error: {e}")
    #             return None
    #         page_source = self.driver.page_source
    #         soup = BeautifulSoup(page_source, "html.parser")
            
    #     finally:
    #         self.driver.quit()

    #     #timeStart = datetime.now()
    #     #get Article Title
    #     title = ''
    #     titleEle = soup.find('span', class_='d-inline-block mr-1')
    #     try:
    #         title = titleEle.get_text()
    #     except Exception as e:
    #         #do nothing cuz ady initialize title
    #         pass

    #     author = ''
    #     authorRelativeLink = ''
    #     author_email = ''
    #     authorDict = {}

    #     ## There are scenario of having no HTML elements to scrap, thus it need to be handle, so far try catch will be used for more fault-tolerent application
    #     ## Future improvement includes change from try-catch to if-else for the purpose of reducing computation resources
    #     try:
    #         #get authName
    #         author_n_dateTimeEle = soup.find('div',class_='article-meta mb-2 mb-lg-0 d-flex align-items-center')
    #         author_aTag = author_n_dateTimeEle.find('a')
    #         author = author_aTag.get_text()
    #         try:
    #             #get author link in BH
    #             authorRelativeLink = author_aTag['href']
    #             author_aTag.decompose()
    #             try:
    #                 email_aTag = author_n_dateTimeEle.find('a')
    #                 author_email = email_aTag.get_text()
    #                 email_aTag.decompose()
    #             except Exception as e:
    #                 #do nothing cuz ady initialize auth_email
    #                 pass
    #         except Exception as e:
    #             #do nothing cuz ady initialize author_aTag
    #             pass
    #     except Exception as e:
    #         #do nothing cuz ady initialize authorDict
    #         pass

    #     authorDict = {"authName":author,"relLink":authorRelativeLink,"email":author_email}
        
    #     # get date time
    #     haveSpanIfHaveAuthor = author_n_dateTimeEle.find('span')
    #     if haveSpanIfHaveAuthor is not None:
    #         author_n_dateTimeEle.find('span').decompose()
    #     publishTime_txt = author_n_dateTimeEle.get_text().strip()
        
    #     # get article brief
    #     articleBrief = ''
    #     articleBrief_ele = soup.find('figcaption',class_='p-2')
    #     try:
    #         articleBrief = articleBrief_ele.get_text()
    #     except Exception as e:
    #         #do nothing cuz ady initialize articleBrief
    #         pass
        

    #     # get article content
    #     articleContent_soup = soup.find('div',class_='dable-content-wrapper')
    #     articleContents = self.getArticleContents(articleContent_soup)

    #     scrappedResults = {"title":title,"authDetail":authorDict,
    #                        "publishTime":publishTime_txt,"articleBrief":articleBrief,
    #                        "contents":articleContents,"url":url}
    #     #print(f'Time Taken to process retrived HTML {datetime.now()-timeStart}')
    #     return scrappedResults
        