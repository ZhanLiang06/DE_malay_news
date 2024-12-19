from kafka import KafkaConsumer
from pyspark.sql import SparkSession
import pandas as pd
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Row

from lexicraft.scrapers.bharian_scraper import BHarianScraper
from lexicraft.util.kafka import KafkaStreamProducer
from lexicraft.util.kafka import get_kafka_topic_latest_message

class ArticleDataCollection:
    def __init__(self, spark):
        self.spark = spark

    # @staticmethod
    # def safe_json_deserializer(value):
    #     try:
    #         return json.loads(value.decode('utf-8'))
    #     except Exception as e:
    #         print(f"Error deserializing JSON: {e}")
    #         return None
    
    def start_collect_to_kafka(self, fromDate, categoryLink, kafka_topic, kafka_broker):
        bharianScraper = BHarianScraper()
        kafka_producer = KafkaStreamProducer(topic_name=kafka_topic)
    
        # print("Scraping article links...")
        
        crawlledUrls = bharianScraper.scrapArticleLinks(fromDate,categoryLink)
        if len(crawlledUrls) == 0:
            return 'No Article Found'
        url_RDD = self.spark.sparkContext.parallelize(crawlledUrls)
        if crawlledUrls is None:
            print('failed to get article links')
            return 'fail'

        url_RDD = url_RDD.repartition(3)
        scrappedData = url_RDD.mapPartitions(BHarianScraper.scrapBatchArticle).collect()
        scrappedData = [data for data in scrappedData if data is not None]

        scrapped_dict_list = []
        for data in scrappedData:
            if isinstance(data, Row):  
                dict_data = data.asDict(recursive = True)
                scrapped_dict_list.append(dict_data)

        kafka_producer.send_data(scrapped_dict_list)
        
        # for data in scrappedData:
        #      if isinstance(data, Row):  
        #          dict_data = data.asDict(recursive = True)
        #          json_data = json.dumps(dict_data, ensure_ascii=False, indent=4)
        #          kafka_producer.send_data(json_data) 
        #          print(json_data)
    
        print(f"Number of articles data sent {len(scrapped_dict_list)}")
        return 'success in sending collected data to kafka'

    def collect_data_from_kafka(self,kafka_topic,kafka_broker,partition):
        # TOPIC = "beritaH"  
        # KAFKA_BROKER = "localhost:9092"
        
        latest_message = get_kafka_topic_latest_message(kafka_topic,kafka_broker,partition)

        # retrive all data
        data_list = latest_message.value

        json_file_path = 'data_output.json'
        with open(json_file_path, 'w') as json_file:
            json.dump(data_list, json_file, indent=4)
        
        with open(json_file_path, 'r', encoding='utf-8') as file:
            bharian_data = json.load(file)
        
        df = pd.DataFrame(bharian_data)
        spark_df = self.spark.createDataFrame(df)
        
        output_path = "DE-prj/RawData"
        spark_df.write.format("parquet").mode("overwrite").save(output_path)
        print(len(data_list))
        
        print("Data has been saved successfully to:", output_path)
        return 'success in collect data from kafka'

      