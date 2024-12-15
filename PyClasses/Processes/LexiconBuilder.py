from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from PyClasses.Scrapers.PRPMScraper import PRPMScraper
from PyClasses.Preprocessing.PRPMCleaner import PRPMCleaner
from datetime import datetime, timedelta
from malaya.pos import huggingface as POSModel
from malaya.pos import describe as pos_dict
import malaya.stem
from malaya.sentiment import available_huggingface
from malaya.torch_model.huggingface import Classification
from malaya.supervised.huggingface import load
from PyClasses.Neo4j.neo4j_connection import Neo4jConnection
from PyClasses.Neo4j.lexicon_nodes import LexiconNodeManager
from PyClasses.Neo4j.lexicon_relationships import LexiconRelManager
import traceback

class LexiconBuilder:
    def __init__(self, spark):
        self.spark = spark
        
    def get_reduced_words(self):
        try:
            file_path = "DE-prj/MR_WC_Result/part-00000"
            df = self.spark.read.text(file_path)
            df_split = df.select(split(df['value'], '\t').alias('word_count'))
            df_final = df_split.select(
                df_split['word_count'].getItem(0).alias('word'),  # First item is the word
                df_split['word_count'].getItem(1).cast('int').alias('count')  # Second item is the count, cast it to integer
            )
            return df_final
        except:
            return None


    def get_words_properties(self):
        # get map reduced words
        df = self.get_reduced_words()

        # Load the sentiment model
        model_names = [
            'mesolitica/sentiment-analysis-nanot5-tiny-malaysian-cased',
            'mesolitica/sentiment-analysis-nanot5-small-malaysian-cased'
        ]
        sentiment_models = [load(
            model=model_name,
            class_model=Classification,
            available_huggingface=available_huggingface,
            force_check=True
        ) for model_name in model_names]
        
        # Broadcast variables (including model)
        modelBC = self.spark.sparkContext.broadcast(malaya.stem.huggingface())
        pos_modelBC = self.spark.sparkContext.broadcast(POSModel())
        formatted_dict = {item['Tag']: item['Description'].split(',')[0].strip() for item in pos_dict}
        formatted_pos_dict_BC = self.spark.sparkContext.broadcast(formatted_dict)
        sentiment_model_BC = self.spark.sparkContext.broadcast(sentiment_models)
        
        def build_lexicon(rows,uri,auth):
            PRPMscrap = PRPMScraper()
            model = modelBC.value
            pos_model = pos_modelBC.value
            pos_dict = formatted_pos_dict_BC.value
            sentiment_model = sentiment_model_BC.value

            #Establish Connection 
            lnm = LexiconNodeManager(uri, auth)
            lrm = LexiconRelManager(uri, auth)
            for row in rows:
                singleResult = PRPMscrap.findWordMetaData(row.word)
                if singleResult is not None:
                    singleResult["base"] = model.stem(row.word)
                    singleResult["count"] = row["count"]
                    singleResult["POS"] = pos_dict[pos_model.predict(row.word)[0][1]]
                    singleResult["SentimentLabel"] = WordLabelling.label_word(sentiment_models, row.word)
                    ## Create Nodes
                    lnm.create_word_node(singleResult)

                    ## Create Base_Word nodes
                    if singleResult["base"] != row.word:
                        base_word_meta = PRPMscrap.findWordMetaData(singleResult["base"])
                        if base_word_meta is not None:
                            base_word_meta["base"] = model.stem(singleResult["base"])
                            base_word_meta["POS"] = pos_dict[pos_model.predict(singleResult["base"])[0][1]]
                            base_word_meta["SentimentLabel"] = WordLabelling.label_word(sentiment_models, singleResult["base"])
                            lnm.create_word_node(base_word_meta)
                            ## Establish Relationships "LEMMATIZED" 
                            lrm.create_node_relationship("WORD", "word", row.word, "WORD", "word", singleResult["base"], "LEMMATIZED")
                        
                    ## Create Synonym Nodes
                    synonymList = list(set(singleResult["synonym"]))
                    if synonymList:
                        for word in synonymList:
                            word_meta = PRPMscrap.findWordMetaData(word)
                            if word_meta is not None:
                                word_meta["base"] = model.stem(word)
                                word_meta["POS"] = pos_dict[pos_model.predict(word)[0][1]]
                                word_meta["SentimentLabel"] = WordLabelling.label_word(sentiment_models, word)
                                lnm.create_word_node(word_meta)
                                ## Establish Relationships "SYNONYM_OF"
                                lrm.create_node_relationship("WORD", "word", row.word, "WORD", "word", word, "SYNONYM_OF")
                    
                    ## Create Anotnym Nodes
                    antonymList = list(set(singleResult["antonym"]))
                    if antonymList:
                        for word in antonymList:
                            word_meta = PRPMscrap.findWordMetaData(word)
                            if word_meta is not None:
                                word_meta["base"] = model.stem(word)
                                word_meta["POS"] = pos_dict[pos_model.predict(word)[0][1]]
                                word_meta["SentimentLabel"] = WordLabelling.label_word(sentiment_models, word)
                                lnm.create_word_node(word_meta)
                                ## Establish Relationships "ANTONYM_OF"
                                lrm.create_node_relationship("WORD", "word", row.word, "WORD", "word", word, "ANTONYM_OF")

        wordRows_rdd = df.rdd.repartition(4)
        timeNow = datetime.now() 
        wordRows_rdd.foreachPartition(get_word_metadata)
        timeEnd =  datetime.now() 
        timeTaken = timeEnd - timeNow
        print(f'Processing Start Time: {timeNow}')
        print(f'Processing End Time: {timeEnd}')
        print(f'Total Time Taken: {timeTaken}')
        return 'success'



class WordLabelling:
    @staticmethod
    def label_word(sentiment_models, input_string):
        model_label_mapping = {
            "positive": 1,
            "neutral": 0,
            "negative": -1
        }

        model_predictions = []
        
        # Get predictions from each model
        for sentiment_model in sentiment_models:
            result = sentiment_model.predict(input_string)
            if isinstance(result, list):
                result = result[0]
            if isinstance(result, str):
                sentiment_label = model_label_mapping.get(result.lower(), "unknown")
            else:
                sentiment_label = model_label_mapping.get(result.get("label", "").lower(), "unknown")
            model_predictions.append(sentiment_label)

        # Aggregate the predictions
        sentiment_labels = model_predictions

        # Majority voting for labels
        if len(set(sentiment_labels)) == 1:
            # If both models agree, take the common label
            final_label = sentiment_labels[0]
        else:
            # Tie case: use scores to break the tie
            positive_score = sum(1 for label in sentiment_labels if label == "positive")
            negative_score = sum(1 for label in sentiment_labels if label == "negative")

            # Compare aggregated scores
            if positive_score > negative_score:
                final_label = "positive"
            elif negative_score > positive_score:
                final_label = "negative"
            else:
                # If scores are also tied, use a priority order
                priority = ["positive", "neutral", "negative"]
                final_label = max(sentiment_labels, key=priority.index)

        return final_label
        