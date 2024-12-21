import re
from datetime import datetime, timedelta
import traceback
import time
import sys
from pyspark.sql import SparkSession
import os
import json

from lexicraft import LexiconBuilder
from lexicraft import Transformation
from lexicraft import ArticleDataCollection
from lexicraft import Analysis
from lexicraft.util.neo4j import LexiconNodeManager

def start_lexicraft(config_data, demo_num_of_article=-1):
    start_time = datetime.now()
    collect_from_time = datetime.fromisoformat(config_data.get("next_scrap_from"))
    uri = config_data.get("db_uri")
    auth = (config_data.get("db_username"), config_data.get("db_password"))
    # try for 5 times
    for i in range (0,5):
        try:
            print(config_data.get("peri_to_reg"),config_data.get("peri_registered"))
            ## data collection 
            spark = SparkSession.builder.appName("DE-prj").getOrCreate()
            data_collection = ArticleDataCollection(spark)
            print(f"Performing Data Collection from time {collect_from_time}...")
            result, scrapToDate = data_collection.start_collect_to_kafka(collect_from_time,'/berita/nasional',"beritaH","localhost:9092", demo_num_of_article)
            if result == 'No Article Found':
               print("No articles left can be scrapped.")
               print("Exiting...")
               try:
                   spark.stop()
               except:
                   pass
               return

            ## data transformation
            #spark = SparkSession.builder.appName("DE-prj").getOrCreate()
            print("Performing Data Transformation...")
            transformation = Transformation(spark,"beritaH","localhost:9092",0)
            status = transformation.start_transform_rawdata_from_kafka()
            if status == "no article to be processed":
                print("No Article Links can scrap.")
                print("Exiting...")
                try:
                    spark.stop()
                except:
                    pass
                return
            # spark.stop()
            result = Transformation.wordCountMapReduce()
            if result == "fail":
                print("Fail to run Hadoop Map Reduce")
                print("Retrying...")
                continue
            #else
            status = transformation.save_temp_data_to_permanent()
            if status == "fail":
                print("Fail to save temp_data_to_permanent")
                print("Retrying...")
                continue
            
            ## Lexicon Creation
            print("Performing Lexicon Creation...")
            spark = SparkSession.builder.appName("DE-prj").getOrCreate()
            lexiconBuilder = LexiconBuilder(spark,uri,auth)
            #lnm = LexiconNodeManager(uri, auth)
            #lnm.clear_db()
            lexiconBuilder.build_lexicon()
            # spark.stop()
            # spark = SparkSession.builder.appName("DE-prj").getOrCreate()
            # lexiconBuilder.spark = spark
            lexiconBuilder.build_peribahasa(config_data.get("peri_to_reg"),config_data.get("peri_registered"))
            spark.stop()

            ## Perform analysis and send result to kafka
            print("Performing Lexicon Analysis...")
            spark = SparkSession.builder.appName("DE-prj").getOrCreate()
            analysis_instance = Analysis(spark,uri,auth)
            analysis_instance.word_length_analysis()
            analysis_instance.lemma_length_analysis()
            analysis_instance.morphological_analysis() 
            analysis_instance.POSDistribution()
            analysis_instance.sentiment_dist()
            analysis_instance.word_freq_with_stopwords()
            analysis_instance.word_freq_without_stopwords()
            print("Analysis Done. Result sent to kafka.")
            analysis_instance.lexicon_analysis()
            spark.stop()
            
            return scrapToDate
        except KeyboardInterrupt:
            print("\nProcess interrupted by user. Stopping execution...")
            try:
                spark.stop()
            except:
                pass 
                
            sys.exit(0)
            
        except Exception as ex:
            traceback.print_exc()
            print("Exception has occured. Retrying the process in 45 seconds - ",i)
            time.sleep(45)
            continue

        finally:
            try:
                spark.stop()
            except:
                pass

    print("Errors occur even after 5 times of retry")
    return None

def read_config(file_path):
    if not os.path.exists(file_path):
        print(f"Error: The config file '{file_path}' was not found.")
        exit() 
    config_data = ""
    try:
        with open(file_path, 'r') as file:
            config_data = json.load(file)
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON from the config file.")
        exit()

    #Check all key has value
    required_keys = ["db_uri", "db_username", "db_password", "next_scrap_from", "peri_to_reg", "peri_registered"]
    for key in required_keys:
        if key not in config_data or config_data[key] is None:
            print(f"Error: '{key}' is missing or has no value in the config file.")
            exit()

    if not isinstance(config_data["peri_to_reg"], int):
        print(f"Error: 'peri_to_reg' must be an integer.")
        exit()

    if not isinstance(config_data["peri_registered"], int):
        print(f"Error: 'peri_registered' must be an integer.")
        exit()
    
    next_scrap_from_str = config_data.get("next_scrap_from", None)

    if next_scrap_from_str:
        try:
            next_scrap_from_datetime = datetime.fromisoformat(next_scrap_from_str)
            return config_data
        except (ValueError, TypeError):
            print(f"Error: '{next_scrap_from_str}' is not a valid ISO datetime string.")
            exit() 
    else:
        print("'next_scrap_from' at config file is missing or None.")
        exit() 


def modify_config(file_path, config_data):
    try:
        with open(file_path, 'w') as file:
            json.dump(config_data, file, indent=4)
        print("Config file updated successfully.")
        
        # Verify if the file was modified successfully
        with open(file_path, 'r') as file:
            updated_data = json.load(file)
            if updated_data == config_data:
                print("Modification verified: The config file was updated successfully.")
            else:
                print("Error: The config file was not modified correctly.")
    except (IOError, json.JSONDecodeError) as e:
        print(f"Error: Failed to write or verify the config file. {e}")
        exit()

        
if __name__ == "__main__":
    start_time = datetime.now()
    config_file_path = 'config.json'
    config_data = read_config(config_file_path)
    if len(sys.argv) < 2:
        start_lexicraft(config_data)
    else:
        demo_num_of_article_str = sys.argv[1]
        demo_num_of_article = -1 # declare and assign random value first (just in case)
        try:
            demo_num_of_article = int(demo_num_of_article_str)
            if demo_num_of_article <= 0:
                print("Error: The argument must be an integer greater than 0.")
                sys.exit(1)
        except ValueError:
            print("Error: The argument must be an integer.")
            sys.exit(1)

    #start the process
    next_scrap_from_datetime = start_lexicraft(config_data, demo_num_of_article)
    if next_scrap_from_datetime is None: # meaning fail to perform task 
        sys.exit(1)
        
    # next_scrap_from_datetime = datetime.fromisoformat(config_data.get("next_scrap_from"))
    # print(next_scrap_from_datetime)
    config_data["next_scrap_from"] = next_scrap_from_datetime.isoformat()
    config_data["peri_registered"] += config_data["peri_to_reg"]
    modify_config(config_file_path, config_data)
    # print("modified data", config_data)
    time_end = datetime.now()
    print("Done Creating Lexicon")
    print("Time Start : ", start_time)
    print("Time End   : ", time_end)
    print("Time Taken : ", time_end - start_time)
    







