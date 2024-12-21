# from malaya.tokenizer import SentenceTokenizer
# from malaya.stem import sastrawi
from pyspark.sql import functions
from pyspark.sql.functions import array, col, concat_ws
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkContext
from itertools import chain
import re
from py4j.java_gateway import java_import
import os
import subprocess
import json
import pandas as pd
from pyspark.sql.utils import AnalysisException


from lexicraft.util.kafka import get_kafka_topic_latest_message
from lexicraft.util.redis import RedisManager


class Transformation:

    def __init__(self, spark, kafka_topic, kafka_broker, topic_partition):
        self.spark = spark
        self.redis_mgr = RedisManager()
        self.kafka_topic = kafka_topic
        self.kafka_broker = kafka_broker
        self.topic_partition = topic_partition

    def start_transform_rawdata_from_kafka(self):
        # self.spark = SparkSession.builder.appName("DataTransformationWithMalaya").getOrCreate()
        print("Collecting Data From Kafka...")
        pd_df = Transformation.collect_data_from_kafka(self.kafka_topic,self.kafka_broker,self.topic_partition)
        spark_df = self.spark.createDataFrame(pd_df)
        #drop duplicated
        spark_df = spark_df.drop_duplicates(['url']).select(['title','articleBrief','contents','url'])
        #drop duplicated where the article already exist b4 in our RawData (i.e. already srapped b4 on previous session)
        newData_df = spark_df.select(['title','articleBrief','contents','url'])
        try:
            existed_data_df = self.spark.read.parquet("DE-prj/RawData").select(['url'])
            # if DE-prj/RawData exist drop any data duplicte with records in DE-prj/RawData
            newData_df = newData_df.join(existed_data_df, existed_data_df["url"] == newData_df["url"], "left_anti").select(['title','articleBrief','contents'])
            print(f"After dropped duplicated articles which exist in database(DE-prj/RawData),")
            print(f"Number of articles left to be processed: {newData_df.count()}")
            if(newData_df.count() == 0):
                return "no article to be processed"
        except AnalysisException as e:
            # Handle case where the path is invalid or the file is missing (i.e. when run for the first time)
            newData_df = spark_df.select(['title','articleBrief','contents'])
            print(f"Is this your first time running the system?")
            print(f"If not please contact support as there is problem in accessing the DE-prj/RawData file in your hadoop file system.")
        
        # Remove strings starting with '-' in the 'articleBrief' column as it convey no meanings, after - the sentences is about the name who take the picture for the articles 
        # e.g. (- Foto fail NSTP, - Foto NSTP Amir Mamat, - Foto NSTP Rohanis Shukri, - Foto fail BERNAMA)
        newData_df = newData_df.withColumn(
            "articleBrief", 
            functions.regexp_replace(functions.col("articleBrief"), r"\s?-.*", "")
        )
        # Preprocesss the contents columns
        newData_df = newData_df.withColumn("Flattened_Contents", concat_ws(" ", col("contents")))
        newData_df = newData_df.select(['title','articleBrief','Flattened_Contents'])
        # Merge DF
        merged_df = newData_df.withColumn("Merged", array(*[col(c) for c in newData_df.columns])).select(['Merged'])
        lists = merged_df.collect()
        
        # Flatten the 'Merged' column
        flattened_list = list(chain.from_iterable(row.Merged for row in lists))
        #--------------Split into sentences and remove special characters-------------------------------------------------------------------------------------------------------------------------

        # spark_app_name = self.spark.sparkContext.appName
        # self.spark.stop() # this has to be stopped, else error
        # self.spark = SparkSession.builder.appName(spark_app_name).getOrCreate()
        #s_tokenizer = SentenceTokenizer()
        rddInput = self.spark.sparkContext.parallelize(flattened_list)
        
        def tokenize_sentences(text):
            # Regular expression to split text by periods, exclamation marks, and question marks
            result = []
            for data in text:
                sentences = re.split(r'(?<=[.!?]) +', data)
                
                result.append(sentences)
            return result
        
        rdd_sentences = rddInput.mapPartitions(tokenize_sentences)
        
        # remove none result
        rdd_sentences = rdd_sentences.filter(lambda x: len(x) > 0)
        def remove_special_characters(sentences):
            # Remove all characters except alphanumeric, comma, and period
            result = []
            for sentence in sentences:
                removedSpecialChar = re.sub(r'[^a-zA-Z-]', ' ', sentence)
                stripped = removedSpecialChar.strip()
                cleaned_text = re.sub(r" \.", ".", stripped)
                result.append(re.sub(r'\s+', ' ', cleaned_text))
            return result
        
        rdd_sentences_clean = rdd_sentences.map(remove_special_characters)
        print("Breaking raw data into list of sentences...")
        # rdd_result = rdd_sentences_clean.coalesce(1) 
        # result_list = rdd_result.collect()
        result_list = rdd_sentences_clean.collect()

        flattenned_sentences = [item for sublist in result_list for item in sublist if item != ""]
        # save flattenned_sentences on redis
        flattenned_sentences_json = json.dumps(flattenned_sentences)
        self.redis_mgr.redis_client.set("temp_sentences",flattenned_sentences_json)
        
        # save sentences to text file 
        # df = self.spark.createDataFrame([(x,) for x in flattenned_sentences],["Sentence"])
        # df.write.mode("overwrite").text('DE-prj/Sentences')

        #----------------Tokenization--------------------------------------------------------------------------------------------------------------------------------------------------------------------
        def tokenize(sentences):
            result = []
            splited_list = sentences.split()
            for ele in splited_list:
                cleanned = re.sub(r'(^-|-$)', '', ele).lower()
                if cleanned != '' and len(cleanned) > 1:
                    result.append(cleanned)
            return result
        senteces_rdd = self.spark.sparkContext.parallelize(flattenned_sentences)
        tokenize_map_output = senteces_rdd.map(tokenize)
        print("Breaking sentences into words...")
        result_list = tokenize_map_output.collect()
        flattened_tokenized_words = [item for sublist in result_list for item in sublist]
        flattened_tokenized_words_json = json.dumps(flattened_tokenized_words)
        self.redis_mgr.redis_client.set("temp_words",flattened_tokenized_words_json)
        # store in hdfs label as temp to do hadoop map reduce
        df = self.spark.createDataFrame([(x,) for x in flattened_tokenized_words],["Word"])
        df.write.mode("overwrite").text('DE-prj/TempWords')
        
        # # Stem
        # stemmer = sastrawi()
        # words_rdd = self.spark.sparkContext.parallelize(flattened_tokenized_words)
        # stemmed_output = words_rdd.map(stemmer.stem)
        # result_list = stemmed_output.collect()
        # flattened_stemmed_words = [item for sublist in result_list for item in sublist]
        # df = self.spark.createDataFrame([(x,) for x in flattened_tokenized_words],["StemmedWord"])
        # df.write.mode("overwrite").text('DE-prj/StemmedWords')

        
        return "success"
        
    @staticmethod
    def collect_data_from_kafka(kafka_topic,kafka_broker,partition):
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
        return df

    
    def save_temp_data_to_permanent(self):
        ## append new raw data
        try:
            raw_data_df = Transformation.collect_data_from_kafka(self.kafka_topic, self.kafka_broker, self.topic_partition)
            spark_app_name = self.spark.sparkContext.appName
            self.spark.stop() # this has to be stopped, else error
            self.spark = SparkSession.builder.appName(spark_app_name).getOrCreate()
            spark_raw_data_df = self.spark.createDataFrame(raw_data_df)
            try:
                existed_data_df = self.spark.read.parquet("DE-prj/RawData").select(['url'])
                # if DE-prj/RawData exist drop any data duplicte with records in DE-prj/RawData
                spark_raw_data_df = spark_raw_data_df.join(existed_data_df, existed_data_df["url"] == spark_raw_data_df["url"], "left_anti")
                print("Number of raw data to be appeneded: ",spark_raw_data_df.count()) 
            except AnalysisException as e:
                pass
            
            output_path = "DE-prj/RawData"
            spark_raw_data_df.write.format("parquet").mode("append").save(output_path)
            print("Raw Data has been successfully appended to HDFS: ", output_path)
    
            
            ## append new sentences data
            sentences_json = self.redis_mgr.redis_client.get("temp_sentences")
            output_path = 'DE-prj/Sentences'
            if sentences_json:
                self.spark.stop() # this has to be stopped, else error
                self.spark = SparkSession.builder.appName(spark_app_name).getOrCreate()
                sentences = json.loads(sentences_json)
                df = self.spark.createDataFrame([(x,) for x in sentences],["Sentence"])
                df.write.mode("append").text(output_path)
                print("Sentence data has been successfully appended to HDFS: ", output_path)
                self.redis_mgr.redis_client.delete("temp_sentences")
            else:
                print("Fail to append to ",output_path)
                print("No data found in Redis.")
    
            ## append new word data
            words_json = self.redis_mgr.redis_client.get("temp_words")
            output_path = 'DE-prj/Words'
            if words_json:
                self.spark.stop() # this has to be stopped, else error
                self.spark = SparkSession.builder.appName(spark_app_name).getOrCreate()
                words = json.loads(words_json)
                df = self.spark.createDataFrame([(x,) for x in words],["Word"])
                df.write.mode("append").text(output_path)
                print("Word data has been successfully appended to HDFS: ", output_path)
                self.redis_mgr.redis_client.delete("temp_words")
            else:
                print("Fail to append to ", output_path)
                print("No data found in Redis.")
            self.spark.stop()
            return "success"
        except:
            return "fail"
        
        

    @classmethod
    def wordCountMapReduce(cls):
        outputDir = "hdfs://localhost:9000/user/student/DE-prj/MR_WC_Result"
        inputFiles = cls.get_processed_words_file_path("/user/student/DE-prj/TempWords/")
        try:
            result = subprocess.run(f"hdfs dfs -rm -r {outputDir}", shell=True, check=True)
        except:
            pass
        # Set the HADOOP_HOME environment variable if not already set
        hadoop_home = os.getenv('HADOOP_HOME', '/home/hduser/hadoop3')
        
        # Check if HADOOP_HOME is set correctly
        if not hadoop_home:
            raise ValueError("HADOOP_HOME environment variable is not set!")
    
        # Define the Hadoop streaming command
        hadoop_streaming_command = [
            f"{hadoop_home}/bin/hadoop", "jar",  # hadoop executable and jar
            f"{hadoop_home}/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar",  # Path to streaming JAR
            "-input", ",".join(inputFiles),
            "-output", outputDir,
            "-mapper", "lexicraft/MapReduce/WordCount/mapper.py",
            "-reducer", "lexicraft/MapReduce/WordCount/reducer.py",
            "-file", "lexicraft/MapReduce/WordCount/mapper.py",
            "-file", "lexicraft/MapReduce/WordCount/reducer.py"
        ]
        print(hadoop_streaming_command)
        # Run the command using subprocess
        try:
            print("Running Hadoop MapReduce for word count...")
            result = subprocess.run(hadoop_streaming_command, check=True, capture_output=True, text=True)
            # Print the output from the Hadoop job
            print("Hadoop Streaming Command Output:")
            print(result.stdout)
            return "success"
        except subprocess.CalledProcessError as e:
            print("Error running the Hadoop streaming job:")
            print(e.stderr)
            return "fail"
            
    @classmethod
    def get_processed_words_file_path(cls,directory):
        # Initialize SparkContext
        sc = SparkContext.getOrCreate()
        
        # Import Hadoop's FileSystem
        java_import(sc._jvm, 'org.apache.hadoop.fs.FileSystem')
        java_import(sc._jvm, 'org.apache.hadoop.fs.Path')
        
        # Get the Hadoop Configuration and FileSystem object
        hadoop_conf = sc._jsc.hadoopConfiguration()
        fs = sc._jvm.FileSystem.get(hadoop_conf)
        
        # Define the HDFS path (replace with your directory path)
        hdfs_directory = f'hdfs://localhost:9000{directory}'
        
        # Create a Path object
        path = sc._jvm.Path(hdfs_directory)
        
        # List the files in the directory
        file_status = fs.listStatus(path)
        
        # Print the file paths
        result = []
        for status in file_status:
            file_path = status.getPath().toString()
            if file_path.startswith(f'hdfs://localhost:9000{directory}part'):
                result.append(file_path)
        return result


#==========================================OLD CODE====================================================
# Text Preprocessing with Malaya
# Broadcast Variables
# Stopwords List, https://github.com/stopwords-iso/stopwords-ms
# stopwords = set([
#     'abdul', 'abdullah', 'acara', 'ada', 'adalah', 'ahmad', 'air', 'akan', 'akhbar', 
#     'akhir', 'aktiviti', 'alam', 'amat', 'amerika', 'anak', 'anggota', 'antara', 
#     'antarabangsa', 'apa', 'apabila', 'april', 'as', 'asas', 'asean', 'asia', 'asing', 
#     'atas', 'atau', 'australia', 'awal', 'awam', 'bagaimanapun', 'bagi', 'bahagian', 
#     'bahan', 'baharu', 'bahawa', 'baik', 'bandar', 'bank', 'banyak', 'barangan', 
#     'baru', 'baru-baru', 'bawah', 'beberapa', 'bekas', 'beliau', 'belum', 'berada', 
#     'berakhir', 'berbanding', 'berdasarkan', 'berharap', 'berikutan', 'berjaya', 
#     'berjumlah', 'berkaitan', 'berkata', 'berkenaan', 'berlaku', 'bermula', 'bernama', 
#     'bernilai', 'bersama', 'berubah', 'besar', 'bhd', 'bidang', 'bilion', 'bn', 'boleh', 
#     'bukan', 'bulan', 'bursa', 'cadangan', 'china', 'dagangan', 'dalam', 'dan', 'dana', 
#     'dapat', 'dari', 'daripada', 'dasar', 'datang', 'datuk', 'demikian', 'dengan', 'depan', 
#     'derivatives', 'dewan', 'di', 'diadakan', 'dibuka', 'dicatatkan', 'dijangka', 
#     'diniagakan', 'dis', 'disember', 'ditutup', 'dolar', 'dr', 'dua', 'dunia', 'ekonomi', 
#     'eksekutif', 'eksport', 'empat', 'enam', 'faedah', 'feb', 'global', 'hadapan', 'hanya', 
#     'harga', 'hari', 'hasil', 'hingga', 'hubungan', 'ia', 'iaitu', 'ialah', 'indeks', 'india', 
#     'indonesia', 'industri', 'ini', 'islam', 'isnin', 'isu', 'itu', 'jabatan', 'jalan', 'jan', 
#     'jawatan', 'jawatankuasa', 'jepun', 'jika', 'jualan', 'juga', 'julai', 'jumaat', 'jumlah', 
#     'jun', 'juta', 'kadar', 'kalangan', 'kali', 'kami', 'kata', 'katanya', 'kaunter', 'kawasan', 
#     'ke', 'keadaan', 'kecil', 'kedua', 'kedua-dua', 'kedudukan', 'kekal', 'kementerian', 'kemudahan', 
#     'kenaikan', 'kenyataan', 'kepada', 'kepentingan', 'keputusan', 'kerajaan', 'kerana', 'kereta', 
#     'kerja', 'kerjasama', 'kes', 'keselamatan', 'keseluruhan', 'kesihatan', 'ketika', 'ketua', 
#     'keuntungan', 'kewangan', 'khamis', 'kini', 'kira-kira', 'kita', 'klci', 'klibor', 'komposit', 
#     'kontrak', 'kos', 'kuala', 'kuasa', 'kukuh', 'kumpulan', 'lagi', 'lain', 'langkah', 'laporan', 
#     'lebih', 'lepas', 'lima', 'lot', 'luar', 'lumpur', 'mac', 'mahkamah', 'mahu', 'majlis', 'makanan', 
#     'maklumat', 'malam', 'malaysia', 'mana', 'manakala', 'masa', 'masalah', 'masih', 'masing-masing', 
#     'masyarakat', 'mata', 'media', 'mei', 'melalui', 'melihat', 'memandangkan', 'memastikan', 'membantu', 
#     'membawa', 'memberi', 'memberikan', 'membolehkan', 'membuat', 'mempunyai', 'menambah', 'menarik', 
#     'menawarkan', 'mencapai', 'mencatatkan', 'mendapat', 'mendapatkan', 'menerima', 'menerusi', 
#     'mengadakan', 'mengambil', 'mengenai', 'menggalakkan', 'menggunakan', 'mengikut', 'mengumumkan', 
#     'mengurangkan', 'meningkat', 'meningkatkan', 'menjadi', 'menjelang', 'menokok', 'menteri', 
#     'menunjukkan', 'menurut', 'menyaksikan', 'menyediakan', 'mereka', 'merosot', 'merupakan', 
#     'mesyuarat', 'minat', 'minggu', 'minyak', 'modal', 'mohd', 'mudah', 'mungkin', 'naik', 'najib', 
#     'nasional', 'negara', 'negara-negara', 'negeri', 'niaga', 'nilai', 'nov', 'ogos', 'okt', 'oleh', 
#     'operasi', 'orang', 'pada', 'pagi', 'paling', 'pameran', 'papan', 'para', 'paras', 'parlimen', 
#     'parti', 'pasaran', 'pasukan', 'pegawai', 'pejabat', 'pekerja', 'pelabur', 'pelaburan', 'pelancongan', 
#     'pelanggan', 'pelbagai', 'peluang', 'pembangunan', 'pemberita', 'pembinaan', 'pemimpin', 
#     'pendapatan', 'pendidikan', 'penduduk', 'penerbangan', 'pengarah', 'pengeluaran', 'pengerusi', 
#     'pengguna', 'pengurusan', 'peniaga', 'peningkatan', 'penting', 'peratus', 'perdagangan', 'perdana', 
#     'peringkat', 'perjanjian', 'perkara', 'perkhidmatan', 'perladangan', 'perlu', 'permintaan', 
#     'perniagaan', 'persekutuan', 'persidangan', 'pertama', 'pertubuhan', 'pertumbuhan', 'perusahaan', 
#     'peserta', 'petang', 'pihak', 'pilihan', 'pinjaman', 'polis', 'politik', 'presiden', 'prestasi', 
#     'produk', 'program', 'projek', 'proses', 'proton', 'pukul', 'pula', 'pusat', 'rabu', 'rakan', 'rakyat', 
#     'ramai', 'rantau', 'raya', 'rendah', 'ringgit', 'rumah', 'sabah', 'sahaja', 'saham', 'sama', 'sarawak', 
#     'satu', 'sawit', 'saya', 'sdn', 'sebagai', 'sebahagian', 'sebanyak', 'sebarang', 'sebelum', 'sebelumnya', 
#     'sebuah', 'secara', 'sedang', 'segi', 'sehingga', 'sejak', 'sekarang', 'sektor', 'sekuriti', 'selain', 
#     'selama', 'selasa', 'selatan', 'selepas', 'seluruh', 'semakin', 'semalam', 'semasa', 'sementara', 
#     'semua', 'semula', 'sen', 'sendiri', 'seorang', 'sepanjang', 'seperti', 'sept', 'september', 
#     'serantau', 'seri', 'serta', 'sesi', 'setiap', 'setiausaha', 'sidang', 'singapura', 'sini', 'sistem', 
#     'sokongan', 'sri', 'sudah', 'sukan', 'suku', 'sumber', 'supaya', 'susut', 'syarikat', 'syed', 'tahap', 
#     'tahun', 'tan', 'tanah', 'tanpa', 'tawaran', 'teknologi', 'telah', 'tempat', 'tempatan', 'tempoh', 
#     'tenaga', 'tengah', 'tentang', 'terbaik', 'terbang', 'terbesar', 'terbuka', 'terdapat', 'terhadap', 
#     'termasuk', 'tersebut', 'terus', 'tetapi', 'thailand', 'tiada', 'tidak', 'tiga', 'timbalan', 'timur', 
#     'tindakan', 'tinggi', 'tun', 'tunai', 'turun', 'turut', 'umno', 'unit', 'untuk', 'untung', 'urus', 'usaha', 
#     'utama', 'walaupun', 'wang', 'wanita', 'wilayah', 'yang'
# ])

# stemmer = malaya.stem.sastrawi()
# broadcast_stopwords = spark.sparkContext.broadcast(stopwords)
# broadcast_stemmer = spark.sparkContext.broadcast(stemmer)

    # def transform_data(self, df):
    #     # Step 1: Combine the text columns into one text column
    #     df = df.withColumn("text_column", concat_ws(" ", col("title"), col("contents"), col("articleBrief")))
    #     # Step 1: Noise Removal
    #     df = df.withColumn("cleaned_text", regexp_replace("text_column", "[^a-zA-Z\\s]", ""))
    #     df = df.withColumn("cleaned_text", lower(col("cleaned_text")))
    
    #     # Step 2: Tokenization with Malaya
    #     # Sentence Segmentation
    #     from malaya.text.function import split_into_sentences
        
    #     def malaya_split_sentences(text):
    #         if isinstance(text, str):
    #             return split_into_sentences(text)
    #         return []
        
    #     segment_udf = udf(malaya_split_sentences, ArrayType(StringType()))
    #     df = df.withColumn("sentences", segment_udf(col("cleaned_text")))
    
    #     def split_into_words(sentences):
    #         if isinstance(sentences, list):
    #             # Flatten the list of sentences into words
    #             return [word for sentence in sentences for word in sentence.split()]
    #         return []
        
    #     tokenize_udf = udf(split_into_words, ArrayType(StringType()))
    #     df = df.withColumn("tokens", tokenize_udf(col("sentences")))
    
    #     stopwords = broadcast_stopwords.value
    #     # Step 3: Stopword Removal using Broadcasted Stopwords
    #     def remove_stopwords(tokens):
    #         return [word for word in tokens if word not in stopwords]
    
    #     remove_stopwords_udf = udf(remove_stopwords, ArrayType(StringType()))
    #     df = df.withColumn("filtered_tokens", remove_stopwords_udf(col("tokens")))
    
    #     stemmer = broadcast_stemmer.value
    #     # Step 4: Stemming with Malaya
    #     def malaya_stem(tokens):
    #         return [stemmer.stem(token) for token in tokens]
    
    #     stem_udf = udf(malaya_stem, ArrayType(StringType()))
    #     df = df.withColumn("stemmed_tokens", stem_udf(col("filtered_tokens")))

    #     df = df.select("cleaned_text", "sentences", "tokens", "filtered_tokens", "stemmed_tokens")

    #     return df

# # Read raw data
# df_raw = data_transformer.readRawData()

# # Perform data transformation
# df_transformed = data_transformer.transform_data(df_raw)

# # Show the transformed DataFrame with full content
# df_transformed.show(truncate=False)  # Disable truncation for full results
# # Convert PySpark DataFrame to Pandas DataFrame
# pandas_df = df_transformed.toPandas()

# # Save as CSV
# pandas_df.to_csv('out.csv', index=False)



# # Optionally, save the processed DataFrame as a Parquet file
# output_path = "DE-prj/ProcessedData"
# df_transformed.write.parquet(output_path, mode="overwrite")
# #write_to_csv(df, output_local_path, is_hdfs=False) 

# # Stop SparkSession