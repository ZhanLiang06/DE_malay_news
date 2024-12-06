# Developer-Manual
# 1.0 Data Collection Web Scraping and Data Crawling
The main data source will be [Berita Harian](https://www.bharian.com.my/). While [PRPM](https://prpm.dbp.gov.my/) will be used to find the meaning of words found in Berita Harian
## 1.1 Crawling Berita Harian
User-defined `BHarianScraper` python class will be used for crawling the data.

`scrapArticles` method will mainly be used, which take in two arguments (`fromDate`,`category`)
* fromDate is datetime datatype (to use datetime, `from datetime import datetime`)
* category is string type which specify the news category (e.g. 'sukan/bola')
*   the category value are value that will append to base url (https://www.bharian.com.my/)
*   e.g. https://www.bharian.com.my/ + 'sukan/bola' will direct to page sukan/bola category (the page url: https://www.bharian.com.my/sukan/bola)

This `scrapArticles` will return you list of dictionary where dictionary format as followed:
| Key    | Data Type | Desc |
| -------- | ------- | - |
| title  | string    | Article Title |
| authDetail | dictionary    | Author Details  |
| publishTime    | string    | Article Publish Time |
| articleBrief    | string    | A brief description followed by article image |
| contents | list of string    | Article contents seperated by paragraphs |
| url | list of string    | Article contents seperated by paragraphs |


** The authDetail dictonary consist of
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
NOTE THAT when using the `scrapOneArticle` method, you have to **manually start the driver first** and **quit it** after executing.

## 1.2 Web Scraping on PRPM
User-defined `PRPMScraper` python class will be used to scrap data from PRPM

The method `findWordMetaData` method will be our method of interest for this project.
* This method takes in one argument (`word`). This is a string value, specifying the string we wish to search up on the PRPM website.
* This method will return data in dictionary data type:
  Where this dictionary will have one __key__, which is the `word` value you pass to the method as argument.
  The __value__ will be dictionory with two key value pairs


| Key | Value |
| -------- | ---------- |
| 'meaning' | list of string, each string is the meaning of word |
| 'synonym' | list of synonym |
 
### Setup required to use `PRPMScraper` library
You can ignore this if you already have bs4 library installed in your python environment
```
pip install bs4
```


# GitHub Linux Handbook
## 1.0 Git Installation on Linux
currently, you at linux cli as student 
### 1. install git
- `(type -p wget >/dev/null || (sudo apt update && sudo apt-get install wget -y)) \
	&& sudo mkdir -p -m 755 /etc/apt/keyrings \
	&& wget -qO- https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo tee /etc/apt/keyrings/githubcli-archive-keyring.gpg > /dev/null \
	&& sudo chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg \
	&& echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
	&& sudo apt update \
	&& sudo apt install gh -y`

### 2. Login git
- `gh auth login`

### 3. Clone repo
- `gh repo clone ZhanLiang06/DE_malay_news`

### 4. create branch
- `git checkout -b {insert any name here}`
- `git push -u origin {branch_name}`

### 5. coding
here you make changes on the files like jupyter notebook

### 6. Now you wish to update the contents in your branch (which is commit changes)
- `git add {files that you have made changes}`
- `git commit`

Tips: you can execute git commit first to see what files have you changed

### 7. Go to github
### 8. Navigate to your branch
### 9. Make pull request (this is to update the main repository)

### 10. then wait for review and merge

## 2.0 To get updated version into your main and branch
### 1. switch to the main branch first
- `git switch main`
### 2. fetch and pull all the commit to update your local main branch
- `git fetch`

- `git pull`
### 3. switch back to your branch
- `git switch {your branch name}`

### 4. merge your branch with origin/main, and update the remote branch
- `git merge origin/main`

- `git push`

### Please do note that we have local and remote repository
In general sense, we can say that:
- local repo (the folders inside your computer linux)
- remote repo (the folders you see on github website)
Your linux main and branch in your PC will not automatically update to the most recent commit from this website (if you no perform fetch and pull at your linux environment)




