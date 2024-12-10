from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import requests
from bs4 import BeautifulSoup
import string
import re

class PeriScraper:
    def __init__(self):
        self.AM_Url = 'https://ms.wikipedia.org/wiki/Senarai_peribahasa_(A–M)'
        self.NZ_Url = 'https://ms.wikipedia.org/wiki/Senarai_peribahasa_(N–Z)'
        self.spark = SparkSession.builder.appName("PeriScraper").getOrCreate()
        self.proverbs_df_AM = None
        self.proverbs_df_NZ = None

    def findPeriMetaData(self, url):
        proverbs_data = self.scrape_page(url)
        if not proverbs_data:
            return None

        schema = ["Proverb", "Meaning"]
        return self.spark.createDataFrame(proverbs_data, schema)

    def scrape_page(self, url):
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to fetch URL: {url}")
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        tables = soup.find_all('table')  
        proverbs_data = []

        for index, table in enumerate(tables):
            rows = table.find_all('tr')[1:]  # Skip the headerrrr
            print(f"Processing Table {index + 1}...")  # If PC is potato, can show progress...
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2: 
                    # Did a bit of cleaning here
                    proverb = ' '.join(cells[0].stripped_strings)
                    proverb_cleaned = self.clean_proverb(proverb) 
                    meaning = cells[1].get_text(strip=True)
                    proverbs_data.append((proverb_cleaned, meaning))
    
        return proverbs_data

    def clean_proverb(self, proverb):
        proverb = re.sub(r'[^\w\s]', '', proverb)
        proverb = re.sub(r'\s+', ' ', proverb)
        proverb = proverb.strip()
        return proverb.lower()

    def findPeri(self, peri):
        first_letter = peri[0].lower()

        if first_letter in string.ascii_lowercase[:13]:  # A to M 
            if self.proverbs_df_AM is None:
                self.proverbs_df_AM = self.findPeriMetaData(self.AM_Url)

            df = self.proverbs_df_AM
        elif first_letter in string.ascii_lowercase[13:]:  # N–Z
            if self.proverbs_df_NZ is None:
                self.proverbs_df_NZ = self.findPeriMetaData(self.NZ_Url)

            df = self.proverbs_df_NZ
        else:
            return "Invalid first letter of the proverb."

        if df is not None:
            result = df.filter(col("Proverb") == peri).collect()
            if result:
                return result[0]["Meaning"]

        return "Proverb not found."