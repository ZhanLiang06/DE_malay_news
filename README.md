# Developer-Manual
# 1.0 Data Collection Web Scraping and Data Crawling
The main data source will be [Berita Harian](https://www.bharian.com.my/). While [PRPM](https://prpm.dbp.gov.my/) will be used to find the meaning of words found in Berita Harian
## 1.1 Crawling Berita Harian
User-defined `BHarianScraper` python class will be used for crawling the data.

`scrapArticles` method will mainly be used, which take in two arguments (`fromDate`,`category`)
* fromDate is datetime datatype (to use datetime, `from datetime import datetime`)
* category is string type which specify the news category (e.g. 'sukan/bola')
*   the category value are value that will append to base url (https://www.bharian.com.my/)
*   e.g. https://www.bharian.com.my/ + 'sukan/bola' will direct to page sukan/bola category

This `scrapArticles` will returns you list of dictionary where dictionary format as followed:
| Key    | Data Type | Desc |
| -------- | ------- | - |
| title  | string    | Article Title |
| authDetail | dictionary    | Author Details  |
| publishTime    | string    | Article Publish Time |
| articleBrief    | string    | A brief description followed by article image |
| contents | list of string    | Article contents seperated by paragraphs |
| url | list of string    | Article contents seperated by paragraphs |


** the authDetail dictonary consist of
| Key    | Data Type | Desc |
| -------- | ------- | -------------------------------- |
| authName  | string    | Author Name |
| relLink | string    | relative link to author page on BH  |
| email    | string    | author email |

### Setup required to use this `BHarianScraper` library
```
pip install selenium webdriver_manager bs4
sudo apt update
sudo apt upgrade
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb
```
This setup will install selenium, webdriver_manager and bs4 library for you as well as install google chrome in your linux.

Lastly, you should be able to use the `BHarianScraper` library 
NOTE: There is more method and properties that might help you when buildling the DE project

## 1.2 Web Scraping on PRPM
User-defined `PRPMScraper` python class will be used to scrap data from PRPM

The method `findWordMetaData` method will be our method of interest for this project.
* This method takes in one argument (`word`). This is a string value, specifying the string we wish to search up on the PRPM website.
* This method will return data in dictionary data type:
  Where this dictionary will have one __key__, which is the `word` value you pass to the method as argument.
  The __value__ will be dictionory with two key value pairs
  * | Key | Value |
  * | --- | ----- |
  * | 'meaning' | list of string, each string is the meaning of word |
  * | 'synonym' | list of synonym |
 
### Setup required to use `PRPMScraper` library
You can ignore this if you already have bs4 library installed in your python environment
```
pip install bs4
```



