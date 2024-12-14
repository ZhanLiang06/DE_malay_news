from malaya.tokenizer import SentenceTokenizer
from malaya.stem import sastrawi
from pyspark.sql import functions
from pyspark.sql.functions import array, col, concat_ws
from pyspark.sql import Row
from pyspark import SparkContext
from itertools import chain
import re
from py4j.java_gateway import java_import
import os
import subprocess

class DE_Transformation:

    def __init__(self, spark):
        self.spark = spark

    def start_transform_rawdata(self):
        # self.spark = SparkSession.builder.appName("DataTransformationWithMalaya").getOrCreate()
        df = self.spark.read.parquet("DE-prj/RawData")
        #drop duplicated
        df = df.drop_duplicates(['url']).select(['title','articleBrief','contents'])
        
        # Remove strings starting with '-' in the 'articleBrief' column as it convey no meanings, after - the sentences is about the name who take the picture for the articles 
        # e.g. (- Foto fail NSTP, - Foto NSTP Amir Mamat, - Foto NSTP Rohanis Shukri, - Foto fail BERNAMA)
        df = df.withColumn(
            "articleBrief", 
            functions.regexp_replace(functions.col("articleBrief"), r"\s?-.*", "")
        )
        # Preprocesss the contents columns
        df = df.withColumn("Flattened_Contents", concat_ws(" ", col("contents")))
        df = df.select(['title','articleBrief','Flattened_Contents'])
        # Merge DF
        merged_df = df.withColumn("Merged", array(*[col(c) for c in df.columns])).select(['Merged'])
        lists = merged_df.collect()
        
        # Flatten the 'Merged' column
        flattened_list = list(chain.from_iterable(row.Merged for row in lists))
        #--------------Split into sentences and remove special characters-------------------------------------------------------------------------------------------------------------------------
        s_tokenizer = SentenceTokenizer()
        rddInput = self.spark.sparkContext.parallelize(flattened_list)
        rdd_sentences = rddInput.map(s_tokenizer.tokenize)
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
        
        result_list = rdd_sentences_clean.collect()

        flattenned_sentences = [item for sublist in result_list for item in sublist]
        # save sentences to text file 
        df = self.spark.createDataFrame([(x,) for x in flattenned_sentences],["Sentence"])
        df.write.mode("overwrite").text('DE-prj/Sentences')

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
        result_list = tokenize_map_output.collect()
        flattened_tokenized_words = [item for sublist in result_list for item in sublist]
        df = self.spark.createDataFrame([(x,) for x in flattened_tokenized_words],["Word"])
        df.write.mode("overwrite").text('DE-prj/Words')
        
        # Stem
        stemmer = sastrawi()
        words_rdd = self.spark.sparkContext.parallelize(flattened_tokenized_words)
        stemmed_output = words_rdd.map(stemmer.stem)
        result_list = stemmed_output.collect()
        flattened_stemmed_words = [item for sublist in result_list for item in sublist]
        df = self.spark.createDataFrame([(x,) for x in flattened_tokenized_words],["StemmedWord"])
        df.write.mode("overwrite").text('DE-prj/StemmedWords')

        
        return flattenned_sentences, flattened_tokenized_words    

    @classmethod
    def wordCountMapReduce(cls):
        outputDir = "hdfs://localhost:9000/user/student/DE-prj/MR_WC_Result"
        inputFiles = cls.get_processed_words_file_path("/user/student/DE-prj/Words/")
        try:
            subprocess.run(f"hdfs dfs -rm -r {outputDir}", shell=True, check=True)
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
            "-mapper", "PyClasses/MapReduce/WordCount/mapper.py",
            "-reducer", "PyClasses/MapReduce/WordCount/reducer.py",
            "-file", "PyClasses/MapReduce/WordCount/mapper.py",
            "-file", "PyClasses/MapReduce/WordCount/reducer.py"
        ]
        print(hadoop_streaming_command)
        # Run the command using subprocess
        try:
            result = subprocess.run(hadoop_streaming_command, check=True, capture_output=True, text=True)
            # Print the output from the Hadoop job
            print("Hadoop Streaming Command Output:")
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print("Error running the Hadoop streaming job:")
            print(e.stderr)
            
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
            if file_path.startswith('hdfs://localhost:9000/user/student/DE-prj/Words/part'):
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
