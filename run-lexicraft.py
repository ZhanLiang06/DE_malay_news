import re
from datetime import datetime, timedelta
import traceback
import time
import sys
from pyspark.sql import SparkSession

from lexicraft import LexiconBuilder
from lexicraft import Transformation
from lexicraft import ArticleDataCollection
from lexicraft import Analysis
from lexicraft.util.neo4j import LexiconNodeManager

def start_lexicraft(db_uri, db_uname, db_auth,interval):
    start_time = datetime.now()
    collect_from_time = start_time - timedelta(hours=interval)
    uri = db_uri
    auth = (db_uname, db_auth)
    # try for 5 times
    for i in range (0,5):
        try:
            spark = SparkSession.builder.appName("DE-prj").getOrCreate()
            data_collection = ArticleDataCollection(spark)
            ## data collection 
            print("Performing Data Collection...")
            result = data_collection.start_collect_to_kafka(collect_from_time,'/berita/nasional',"beritaH","localhost:9092")
            if result == 'No Article Found':
                print("No Article Links can scrap.")
                print("Exiting...")
                try:
                    spark.stop()
                except:
                    pass
                return
            print("Collecting Data From Kafka...")
            data_collection.collect_data_from_kafka("beritaH","localhost:9092",0)
            spark.stop()

            ## data transformation
            spark = SparkSession.builder.appName("DE-prj").getOrCreate()
            print("Performing Data Transformation...")
            transformation = Transformation(spark)
            transformation.start_transform_rawdata()
            spark.stop() # must stop or else gg !!!!! IMPORTANT
            Transformation.wordCountMapReduce()
        
            ## Lexicon Creation
            print("Performing Lexicon Creation...")
            spark = SparkSession.builder.appName("DE-prj").getOrCreate()
            lexiconBuilder = LexiconBuilder(spark,uri,auth)
            lnm = LexiconNodeManager(uri, auth)
            lnm.clear_db()
            lexiconBuilder.build_lexicon()
            lexiconBuilder.build_peribahasa()

            ## Perform analysis and send result to kafka
            print("Performing Lexicon Analysis...")
            spark = SparkSession.builder.appName("DE-prj").getOrCreate()
            analysis_instance = Analysis(spark,uri,auth)
            analysis_instance.word_length_analysis()
            analysis_instance.lemma_length_analysis()
            analysis_instance.lexicon_analysis()
            analysis_instance.morphological_analysis() 
            analysis_instance.POSDistribution()
            analysis_instance.sentiment_dist()
            analysis_instance.word_freq_with_stopwords()
            analysis_instance.word_freq_without_stopwords()
            print("Analysis Done. Result sent to kafka.")

            spark.stop()
            print("Done Creating Lexicon")
            print("Time Start :", start_time)
            print("Time End : ", datetime.now())
            
            return
        except KeyboardInterrupt:
            print("\nProcess interrupted by user. Stopping execution...")
            try:
                spark.stop()
            except:
                pass 
                
            return  
            
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


if __name__ == "__main__":
    if sys.argv[1] is None and sys.argv[2] is None and sys.argv[3] is None:
        print("Invalid input for database credentials, input must not be null Example of arugment format \"uri db_username db_password\".")
    try:
        forth_input = sys.argv[4]  
        if forth_input.endswith("H") and forth_input[:-1].isdigit():
            interval = int(forth_input[:-1])
            start_lexicraft(sys.argv[1], sys.argv[2], sys.argv[3], interval)
        else:
            print("Invalid input at argument 4! The format must be a number followed by 'H'.")
    except IndexError:
        print("Please provide an argument specifying the interval (e.g., '3H' for 3 hours).")
    except ValueError:
        print("Invalid argument format. Please provide a valid integer followed by 'H'.")








