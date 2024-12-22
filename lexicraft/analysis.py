import malaya
import pandas as pd
import ast
import numpy as np
from pyspark.sql import SparkSession
from malaya.stem import sastrawi
from pyspark.sql import Row
import malaya
from kafka import KafkaProducer
import json
from sklearn.feature_extraction.text import TfidfVectorizer,  CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re
from sklearn.naive_bayes import MultinomialNB
import joblib
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
from malaya.supervised.huggingface import load
from malaya.torch_model.huggingface import Classification
from datetime import datetime

from lexicraft.util.neo4j import LexiconNodeManager
from lexicraft.scrapers.wiki_scraper import WikiScraper

# Function to analyze morphological structure using Sastrawi stemmer
class Analysis():
    def __init__(self,spark,uri,auth):
        self.kafka_bootstrap_servers = "localhost:9092"
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.spark = spark
        self.uri = uri
        self.auth = auth

    def on_send_success(self, record_metadata):
        print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} at offset {record_metadata.offset}")

    def on_send_error(self, exception):
        print(f"Error occurred: {exception}")
    
    def lemma_length_analysis(self):
        lnm = LexiconNodeManager(self.uri, self.auth)
        analysis_query = """
            MATCH (n:WORD)-[:LEMMATIZED]->(lematizedNode)
            RETURN DISTINCT lematizedNode.word AS lemmatized
        """
        all_words = lnm.create_custom_query(analysis_query)
        words_list = [item['lemmatized'] for item in all_words]
        word_lengths = [len(word) for word in words_list]
        average_word_length = np.mean(word_lengths)
        most_freq_length =  np.argmax(np.bincount(word_lengths))

        message = {"word_lengths":word_lengths,"avg_word_length":float(average_word_length),"most_freq_length":int(most_freq_length)}
        self.producer.send("lemma_length_analysis", partition=0,value=message).add_callback(self.on_send_success).add_errback(self.on_send_error)
        self.producer.flush()
    
    
    def lexicon_analysis(self):
        lnm = LexiconNodeManager(self.uri, self.auth)
        # Return everything
        query = """
        MATCH (n:WORD)
        WITH COUNT(n) AS totalWords
        MATCH (n:PERIBAHASA)
        RETURN totalWords, COUNT(n) AS totalPeri;
        """
        result = lnm.create_custom_query(query)
        
        message = result
        self.producer.send("lexicon_analysis", partition=0,value=message).add_callback(self.on_send_success).add_errback(self.on_send_error)
        self.producer.flush()
        

    def word_length_analysis(self):
        lnm = LexiconNodeManager(self.uri, self.auth)
        all_words = lnm.get_all_words()
        words_list = [item['n.word'] for item in all_words]
        word_lengths = [len(word) for word in words_list]
        average_word_length = np.mean(word_lengths)
        most_freq_length =  np.argmax(np.bincount(word_lengths))

        message = {"word_lengths":word_lengths,"avg_word_length":float(average_word_length),"most_freq_length":int(most_freq_length)}
        self.producer.send("word_length_analysis", partition=0,value=message).add_callback(self.on_send_success).add_errback(self.on_send_error)
        self.producer.flush()

    def sentiment_dist(self):
        lnm = LexiconNodeManager(self.uri, self.auth)
        # # Get all result
        # result = lnm.get_all_words()
        # result = [d["n.word"] for d in result]
        
        # Word Frequency Analysis
        analysis_query = """
            MATCH (n:WORD)
            RETURN n.Label AS label, COUNT(n) AS total_count
            ORDER BY total_count DESC
            """
        result = lnm.create_custom_query(analysis_query)
        
        message = result
        self.producer.send("sentiment_dist_analysis", partition=0,value=message).add_callback(self.on_send_success).add_errback(self.on_send_error)
        self.producer.flush()


    def word_freq_with_stopwords(self):
        lnm = LexiconNodeManager(self.uri, self.auth)
        # # Get all result
        # result = lnm.get_all_words()
        # result = [d["n.word"] for d in result]
        # Word Frequency Analysis
        analysis_query = """
            MATCH (n:WORD)
            RETURN n.word AS word, n.word_count AS count
            ORDER BY n.word_count DESC
            LIMIT 15
            """
        result = lnm.create_custom_query(analysis_query)    
        
        message = result
        self.producer.send("word_freq_with_stopwords", partition=0,value=message).add_callback(self.on_send_success).add_errback(self.on_send_error)
        self.producer.flush()
    
    def word_freq_without_stopwords(self):
        # Stopwords List, https://github.com/stopwords-iso/stopwords-ms
        stopwords = [
            'abdul', 'abdullah', 'acara', 'ada', 'adalah', 'ahmad', 'air', 'akan', 'akhbar', 
            'akhir', 'aktiviti', 'alam', 'amat', 'amerika', 'anak', 'anggota', 'antara', 
            'antarabangsa', 'apa', 'apabila', 'april', 'as', 'asas', 'asean', 'asia', 'asing', 
            'atas', 'atau', 'australia', 'awal', 'awam', 'bagaimanapun', 'bagi', 'bahagian', 
            'bahan', 'baharu', 'bahawa', 'baik', 'bandar', 'bank', 'banyak', 'barangan', 
            'baru', 'baru-baru', 'bawah', 'beberapa', 'bekas', 'beliau', 'belum', 'berada', 
            'berakhir', 'berbanding', 'berdasarkan', 'berharap', 'berikutan', 'berjaya', 
            'berjumlah', 'berkaitan', 'berkata', 'berkenaan', 'berlaku', 'bermula', 'bernama', 
            'bernilai', 'bersama', 'berubah', 'besar', 'bhd', 'bidang', 'bilion', 'bn', 'boleh', 
            'bukan', 'bulan', 'bursa', 'cadangan', 'china', 'dagangan', 'dalam', 'dan', 'dana', 
            'dapat', 'dari', 'daripada', 'dasar', 'datang', 'datuk', 'demikian', 'dengan', 'depan', 
            'derivatives', 'dewan', 'di', 'diadakan', 'dibuka', 'dicatatkan', 'dijangka', 
            'diniagakan', 'dis', 'disember', 'ditutup', 'dolar', 'dr', 'dua', 'dunia', 'ekonomi', 
            'eksekutif', 'eksport', 'empat', 'enam', 'faedah', 'feb', 'global', 'hadapan', 'hanya', 
            'harga', 'hari', 'hasil', 'hingga', 'hubungan', 'ia', 'iaitu', 'ialah', 'indeks', 'india', 
            'indonesia', 'industri', 'ini', 'islam', 'isnin', 'isu', 'itu', 'jabatan', 'jalan', 'jan', 
            'jawatan', 'jawatankuasa', 'jepun', 'jika', 'jualan', 'juga', 'julai', 'jumaat', 'jumlah', 
            'jun', 'juta', 'kadar', 'kalangan', 'kali', 'kami', 'kata', 'katanya', 'kaunter', 'kawasan', 
            'ke', 'keadaan', 'kecil', 'kedua', 'kedua-dua', 'kedudukan', 'kekal', 'kementerian', 'kemudahan', 
            'kenaikan', 'kenyataan', 'kepada', 'kepentingan', 'keputusan', 'kerajaan', 'kerana', 'kereta', 
            'kerja', 'kerjasama', 'kes', 'keselamatan', 'keseluruhan', 'kesihatan', 'ketika', 'ketua', 
            'keuntungan', 'kewangan', 'khamis', 'kini', 'kira-kira', 'kita', 'klci', 'klibor', 'komposit', 
            'kontrak', 'kos', 'kuala', 'kuasa', 'kukuh', 'kumpulan', 'lagi', 'lain', 'langkah', 'laporan', 
            'lebih', 'lepas', 'lima', 'lot', 'luar', 'lumpur', 'mac', 'mahkamah', 'mahu', 'majlis', 'makanan', 
            'maklumat', 'malam', 'malaysia', 'mana', 'manakala', 'masa', 'masalah', 'masih', 'masing-masing', 
            'masyarakat', 'mata', 'media', 'mei', 'melalui', 'melihat', 'memandangkan', 'memastikan', 'membantu', 
            'membawa', 'memberi', 'memberikan', 'membolehkan', 'membuat', 'mempunyai', 'menambah', 'menarik', 
            'menawarkan', 'mencapai', 'mencatatkan', 'mendapat', 'mendapatkan', 'menerima', 'menerusi', 
            'mengadakan', 'mengambil', 'mengenai', 'menggalakkan', 'menggunakan', 'mengikut', 'mengumumkan', 
            'mengurangkan', 'meningkat', 'meningkatkan', 'menjadi', 'menjelang', 'menokok', 'menteri', 
            'menunjukkan', 'menurut', 'menyaksikan', 'menyediakan', 'mereka', 'merosot', 'merupakan', 
            'mesyuarat', 'minat', 'minggu', 'minyak', 'modal', 'mohd', 'mudah', 'mungkin', 'naik', 'najib', 
            'nasional', 'negara', 'negara-negara', 'negeri', 'niaga', 'nilai', 'nov', 'ogos', 'okt', 'oleh', 
            'operasi', 'orang', 'pada', 'pagi', 'paling', 'pameran', 'papan', 'para', 'paras', 'parlimen', 
            'parti', 'pasaran', 'pasukan', 'pegawai', 'pejabat', 'pekerja', 'pelabur', 'pelaburan', 'pelancongan', 
            'pelanggan', 'pelbagai', 'peluang', 'pembangunan', 'pemberita', 'pembinaan', 'pemimpin', 
            'pendapatan', 'pendidikan', 'penduduk', 'penerbangan', 'pengarah', 'pengeluaran', 'pengerusi', 
            'pengguna', 'pengurusan', 'peniaga', 'peningkatan', 'penting', 'peratus', 'perdagangan', 'perdana', 
            'peringkat', 'perjanjian', 'perkara', 'perkhidmatan', 'perladangan', 'perlu', 'permintaan', 
            'perniagaan', 'persekutuan', 'persidangan', 'pertama', 'pertubuhan', 'pertumbuhan', 'perusahaan', 
            'peserta', 'petang', 'pihak', 'pilihan', 'pinjaman', 'polis', 'politik', 'presiden', 'prestasi', 
            'produk', 'program', 'projek', 'proses', 'proton', 'pukul', 'pula', 'pusat', 'rabu', 'rakan', 'rakyat', 
            'ramai', 'rantau', 'raya', 'rendah', 'ringgit', 'rumah', 'sabah', 'sahaja', 'saham', 'sama', 'sarawak', 
            'satu', 'sawit', 'saya', 'sdn', 'sebagai', 'sebahagian', 'sebanyak', 'sebarang', 'sebelum', 'sebelumnya', 
            'sebuah', 'secara', 'sedang', 'segi', 'sehingga', 'sejak', 'sekarang', 'sektor', 'sekuriti', 'selain', 
            'selama', 'selasa', 'selatan', 'selepas', 'seluruh', 'semakin', 'semalam', 'semasa', 'sementara', 
            'semua', 'semula', 'sen', 'sendiri', 'seorang', 'sepanjang', 'seperti', 'sept', 'september', 
            'serantau', 'seri', 'serta', 'sesi', 'setiap', 'setiausaha', 'sidang', 'singapura', 'sini', 'sistem', 
            'sokongan', 'sri', 'sudah', 'sukan', 'suku', 'sumber', 'supaya', 'susut', 'syarikat', 'syed', 'tahap', 
            'tahun', 'tan', 'tanah', 'tanpa', 'tawaran', 'teknologi', 'telah', 'tempat', 'tempatan', 'tempoh', 
            'tenaga', 'tengah', 'tentang', 'terbaik', 'terbang', 'terbesar', 'terbuka', 'terdapat', 'terhadap', 
            'termasuk', 'tersebut', 'terus', 'tetapi', 'thailand', 'tiada', 'tidak', 'tiga', 'timbalan', 'timur', 
            'tindakan', 'tinggi', 'tun', 'tunai', 'turun', 'turut', 'umno', 'unit', 'untuk', 'untung', 'urus', 'usaha', 
            'utama', 'walaupun', 'wang', 'wanita', 'wilayah', 'yang'
        ]
        lnm = LexiconNodeManager(self.uri, self.auth)

        
        # Word Frequency Analysis
        analysis_query = """
            MATCH (n:WORD)
            RETURN n.word AS word, n.word_count AS count
            ORDER BY n.word_count DESC
            LIMIT 1500
            """
    
        result = lnm.create_custom_query(analysis_query)
        result = [item for item in result if item['word'] not in stopwords]
        top_15_result = result[:15]

        message = top_15_result
        self.producer.send("word_freq_without_stopwords", partition=0,value=message).add_callback(self.on_send_success).add_errback(self.on_send_error)
        self.producer.flush()

    
    def POSDistribution(self):
        lnm = LexiconNodeManager(self.uri, self.auth)
        # # Get all result
        # result = lnm.get_all_words()
        # result = [d["n.word"] for d in result]
        
        # Word Frequency Analysis
        analysis_query = """
            MATCH (n:WORD)
            RETURN n.POS AS pos, COUNT(n) AS total_count
            ORDER BY total_count DESC
            """
        result = lnm.create_custom_query(analysis_query)    
        
        message = result
        self.producer.send("POSDistribution", partition=0,value=message).add_callback(self.on_send_success).add_errback(self.on_send_error)
        self.producer.flush()

        
    def morphological_analysis(self):
        def analyze_token(tokens):
            # token_stem_tuple = []
            # prefix_list = []
            # suffix_list = []
            result = []
            stemmer = sastrawi()
            for token in tokens:
                stem = stemmer.stem(token)  # Get the root/stem of the word
                prefix = None
                suffix = None
                core_word = stem
            
                # Check if the token has a prefix and/or suffix
                if token != stem:
                    # The stemmer removes affixes; calculate what was removed
                    prefix = token[:token.index(stem)] if stem in token else None
                    suffix = token[token.index(stem) + len(stem):] if stem in token else None
                
                result.append( {
                    'word': token,
                    'stem': stem,
                    'prefix': prefix,
                    'suffix': suffix,
                    'core_word': stem.strip()  # Root word after removing affixes
                })

            return result

        lnm = LexiconNodeManager(self.uri, self.auth)
        # Get all result
        word_list = lnm.get_all_words()
        word_list = [d["n.word"] for d in word_list]
        word_list_rdd = self.spark.sparkContext.parallelize(word_list)
        word_list_rdd = word_list_rdd.repartition(6)
        result = word_list_rdd.mapPartitions(analyze_token).collect()
        
    
        df_result = pd.DataFrame(result)

        message = df_result.to_json(orient='records')
        self.producer.send("morphological_analysis", partition=0,value=message).add_callback(self.on_send_success).add_errback(self.on_send_error)
        self.producer.flush()

    @staticmethod
    def emotion_analysis_with_graph(spark):
        # 1. Define available Hugging Face model
        available_huggingface = {
            'mesolitica/emotion-analysis-nanot5-tiny-malaysian-cased': {
                'Size (MB)': 93,
                'macro precision': 0.96107,
                'macro recall': 0.96270,
                'macro f1-score': 0.96182,
            },
            'mesolitica/emotion-analysis-nanot5-small-malaysian-cased': {
                'Size (MB)': 167,
                'macro precision': 0.96814,
                'macro recall': 0.97004,
                'macro f1-score': 0.96905,
            },
        }
        
        # 2. Load the emotion model
        model_name = 'mesolitica/emotion-analysis-nanot5-small-malaysian-cased'
        emotion_model = load(
            model=model_name,
            class_model=Classification,
            available_huggingface=available_huggingface,
            force_check=True
        )
        
        # Define the emotion labels
        label = ['anger', 'fear', 'happy', 'love', 'sadness', 'surprise']

        def sentence_level_analysis(cleaned_text_array):
            """
            Perform emotion analysis for each sentence in the array using the emotion classification model.
            """
            if not cleaned_text_array:
                return ['neutral'] * len(cleaned_text_array)  # Default 'neutral' if the array is empty
            
            try:
                # Perform emotion classification on each sentence in the array
                results = emotion_model.predict(cleaned_text_array)
                return [result.lower() for result in results]  # Convert each result to lowercase
            except Exception as e:
                print(f"Error during prediction: {e}")
                return ['neutral'] * len(cleaned_text_array)  # Return 'neutral' if an error occurs

        def visualize_emotion_distribution(sentiment_df):
            """
            Visualize the distribution of emotions in the dataset.
            """
            # Count the occurrences of each emotion
            emotion_counts = sentiment_df['label'].value_counts()
            
            # Create a bar plot for emotion distribution
            plt.figure(figsize=(10, 6))
            sns.barplot(x=emotion_counts.index, y=emotion_counts.values, palette="viridis")
            plt.title("Emotion Distribution")
            plt.xlabel("Emotion")
            plt.ylabel("Frequency")
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()
        
            # Create a word cloud for the most common emotions
            plt.figure(figsize=(8, 8))
            wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(emotion_counts.to_dict())
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.title("Word Cloud of Emotions")
            plt.axis("off")
            plt.tight_layout()
            plt.show()

        sentiment_results = []
        lines_rdd = spark.sparkContext.textFile("DE-prj/Sentences")
        lines_list = lines_rdd.collect()
        sentence_predictions = sentence_level_analysis(lines_list)
        for sentence, prediction in zip(lines_list, sentence_predictions):
                sentiment_results.append({"cleaned_text": sentence, "label": prediction})
        
        # for index, row in lines_pd_df.iterrows():
        #     cleaned_text_array = row['cleaned_text']  # Assuming this is an array of sentences
            
        #     # If 'cleaned_text' is a string representation of a list, convert it
        #     if isinstance(cleaned_text_array, str):
        #         cleaned_text_array = eval(cleaned_text_array)  # Converts string representation of list to actual list
            
        #     # Perform sentence-level emotion analysis
        #     sentence_predictions = sentence_level_analysis(cleaned_text_array)
            
        #     # Append the results as a dictionary with 'cleaned_text' and corresponding 'label'
        #     for sentence, prediction in zip(cleaned_text_array, sentence_predictions):
        #         sentiment_results.append({"cleaned_text": sentence, "label": prediction})
         
        # Create DataFrame from the sentiment results
        sentiment_df = pd.DataFrame(sentiment_results)
         
        # Save results to a new CSV containing only 'cleaned_text' and 'label'
        output_csv_path = f"analysis_csv_result/sentence_emotion_analysis_results_{datetime.now()}.csv"
        sentiment_df.to_csv(output_csv_path, index=False)
        visualize_emotion_distribution(sentiment_df)
        
    
    @staticmethod
    def intrinsic_accuracy(word_to_find,uri,auth):
        lnm = LexiconNodeManager(uri, auth)
        single_word_query = f"""
            MATCH (n:WORD {{word: '{word_to_find}'}})
            RETURN n.word AS word, n.definitions AS definitions
            LIMIT 1
            """
        
        neoresult = lnm.create_custom_query(single_word_query)

        wk = WikiScraper()
        wikiresult = wk.findWordMetaData(word_to_find)
        
        if wikiresult and 'meanings' in wikiresult[word_to_find]:
            raw_meanings = wikiresult[word_to_find]['meanings']
            cleaned_meanings = prpm_cleaner.clean_all_meanings(raw_meanings)
            wikiresult[word_to_find]['meanings'] = cleaned_meanings
        
        wiki_meaning = wikiresult[word_to_find]['WikiDefi'][0]
        neo_combined = ' '.join(neoresult[0]['definitions'])

        def clean_text(text):
            text = re.sub(r'[^\w\s]', '', text)  # Remove all punctuation
            text = ' '.join(text.split())       # Normalize multiple spaces to a single space
            return text.lower()                 # Convert to lowercase
        
        # Clean the input texts
        wiki_cleaned = clean_text(wiki_meaning)
        neo_cleaned = clean_text(neo_combined)
        
        # Tokenize after cleaning
        wiki_tokens = set(wiki_cleaned.split())
        neo_tokens = set(neo_cleaned.split())
        
        # Calculate intersection and accuracy
        intersection = wiki_tokens & neo_tokens
        accuracy = len(intersection) / len(wiki_tokens)
        
        # Output
        print(f"Token Overlap Accuracy: {accuracy:.2f}")

    def intrinsic_coverage(self):
        lnm = LexiconNodeManager(self.uri, self.auth)
        analysis_query = """
            MATCH (n:WORD)
            RETURN n.word AS word, n.word_count AS count, n.definitions AS definitions
            ORDER BY n.word_count DESC
            LIMIT 1000
            """
        
        result = lnm.create_custom_query(analysis_query)

        # Analyze coverage
        total_words = len(result)
        covered_words = 0
        not_covered_words = []
        
        for word in result:
            definitions = word['definitions']
            if definitions and any(defn.strip() for defn in definitions):
                covered_words += 1
            else: 
                not_covered_words.append(word['word'])
        
        coverage_percentage = (covered_words / total_words) * 100 if total_words > 0 else 0

        print(f"Total Words: {total_words}")
        print(f"Covered Words: {covered_words}")
        print(f"Coverage Percentage: {coverage_percentage:.2f}%")
        
        print("\nExample of Not Covered Words:")
        print(", ".join(not_covered_words))

        message = {"TotalWords":total_words,"CoveredWords":covered_words,"CoverPercentage":coverage_percentage,"NotCoveredWords":not_covered_words}
        self.producer.send("intrinsic_coverage", partition=0,value=message).add_callback(self.on_send_success).add_errback(self.on_send_error)
        self.producer.flush()

    @staticmethod
    def intrinsic_consistency(list_of_input,uri,auth):
        lnm = LexiconNodeManager(uri, auth)
        neoresults = []
        for word in list_of_input: 
            single_word_query = f"""
                MATCH (n:WORD {{word: '{word}'}})
                RETURN n.word AS word, n.POS AS pos
                LIMIT 1
                """
            neoresult = lnm.create_custom_query(single_word_query)
            neoresults.append(neoresult)
            
        print(neoresults) 


    @staticmethod
    def custom_tokenizer(text):
        return text.split()
    
    @staticmethod
    def extrinsic_word_sentiment(csv_dataset_filepath,uri,auth):
        lnm = LexiconNodeManager(uri, auth)
        neo_query = """
        MATCH (n)
        WHERE n.word IS NOT NULL AND n.Label IS NOT NULL
            AND n.Label IN ['positive', 'neutral', 'negative']
        RETURN n.word AS word, 
           CASE 
               WHEN n.Label = 'positive' THEN 1
               WHEN n.Label = 'neutral' THEN 0
               WHEN n.Label = 'negative' THEN -1
           END AS label_int;
        """
        
        result = lnm.create_custom_query(neo_query)
        df = pd.DataFrame(result)

        # Train the model
        vectorizer = CountVectorizer(tokenizer=Analysis.custom_tokenizer, binary=True)
        X_vec = vectorizer.fit_transform(df['word'])
        y = df['label_int']
        
        # Train a Naive Bayes classifier
        model = MultinomialNB()
        model.fit(X_vec, y)
        
        # Save the trained model and vectorizer
        joblib.dump(model, 'sentiment_model.pkl')
        joblib.dump(vectorizer, 'vectorizer.pkl')
        print("Model trained successfully on the lexicon dataset.")

        ################################# now perfrom the extrinsic evaluation ##########################################################
        extrinsic_df = pd.read_csv(csv_dataset_filepath)
        if 'word' not in extrinsic_df.columns or 'sentiment' not in extrinsic_df.columns:
                raise ValueError("The extrinsic dataset must contain 'word' and 'sentiment' columns.")

        # Rename columns to match the expected format
        extrinsic_df.rename(columns={'word': 'filtered_tokens', 'sentiment': 'sentiment_label'}, inplace=True)
        
        # Check for missing values in the sentiment_label column
        if extrinsic_df['sentiment_label'].isnull().any():
            # Drop rows with missing values
            extrinsic_df = extrinsic_df.dropna(subset=['sentiment_label'])

        # Check for non-numeric values in the sentiment_label column
        non_numeric_values = extrinsic_df[~extrinsic_df['sentiment_label'].apply(lambda x: isinstance(x, (int, float)))]
        if not non_numeric_values.empty:
            print("Non-numeric values in sentiment_label:")
            print(non_numeric_values)
            # Drop rows with non-numeric values
            extrinsic_df = extrinsic_df[extrinsic_df['sentiment_label'].apply(lambda x: isinstance(x, (int, float)))]

        # Ensure sentiment_label is integer
        extrinsic_df['sentiment_label'] = extrinsic_df['sentiment_label'].astype(int)
    
        # Preprocess the extrinsic dataset
        extrinsic_df['sentiment_label'] = extrinsic_df['sentiment_label'].astype(int)
    
        # Load the trained model and vectorizer
        model = joblib.load('sentiment_model.pkl')
        vectorizer = joblib.load('vectorizer.pkl')
    
        # Vectorize the extrinsic dataset
        X_extrinsic = vectorizer.transform(extrinsic_df['filtered_tokens'])
        y_extrinsic = extrinsic_df['sentiment_label']
    
        # Make predictions on the extrinsic dataset
        y_pred_extrinsic = model.predict(X_extrinsic)
    
        # Evaluate the model on the extrinsic dataset
        accuracy_extrinsic = accuracy_score(y_extrinsic, y_pred_extrinsic)
        print(f"Extrinsic Evaluation Accuracy: {accuracy_extrinsic:.2f}")
        print("\nExtrinsic Evaluation Classification Report:")
        print(classification_report(y_extrinsic, y_pred_extrinsic, target_names=['Negative', 'Neutral', 'Positive']))
    
        # Confusion Matrix
        conf_matrix_extrinsic = confusion_matrix(y_extrinsic, y_pred_extrinsic)
        plt.figure(figsize=(8, 6))
        sns.heatmap(conf_matrix_extrinsic, annot=True, fmt='d', cmap='Blues', xticklabels=['Negative', 'Neutral', 'Positive'], yticklabels=['Negative', 'Neutral', 'Positive'])
        plt.title("Extrinsic Evaluation Confusion Matrix")
        plt.xlabel("Predicted")
        plt.ylabel("Actual")
        plt.show()

    @staticmethod
    def extrinsic_emotion_analysis(lexicon_data_path, extrinsic_sample_data):
        def load_data(csv_file_path):
            """
            Load the emotion analysis dataset from a CSV file.
            """
            df = pd.read_csv(csv_file_path)
            if 'cleaned_text' not in df.columns or 'label' not in df.columns:
                raise ValueError("The CSV file must contain 'cleaned_text' and 'label' columns.")
            return df
        
        # Preprocess data
        def preprocess_data(df):
            """
            Preprocess the dataset by ensuring labels are standardized.
            """
            # Normalize label names to include all emotions
            df['label'] = df['label'].replace({
                'sad': 'sadness', 
                'angry': 'anger', 
                'happy': 'happy', 
                'fear': 'fear', 
                'love': 'love', 
                'surprise': 'surprise',
            })
            
            # Ensure only valid emotions are included
            valid_labels = ['sadness', 'anger', 'happy', 'fear', 'love', 'surprise']
            df = df[df['label'].isin(valid_labels)]
            
            return df
        
        # Train the model with the entire dataset
        def train_model(df):
            """
            Train an emotion classification model using Naive Bayes on the entire dataset.
            """
            # Vectorize text data using TF-IDF
            vectorizer = TfidfVectorizer(max_features=5000, ngram_range=(1, 2))
            X_vec = vectorizer.fit_transform(df['cleaned_text'])
            y = df['label']
            
            # Train a Naive Bayes classifier
            model = MultinomialNB()
            model.fit(X_vec, y)
            
            # Evaluate the model on the entire dataset
            y_pred = model.predict(X_vec)
            
            # Evaluation
            print("Classification Report:")
            print(classification_report(y, y_pred))
            accuracy = accuracy_score(y, y_pred)
            print(f"Accuracy: {accuracy:.4f}")
            
            # Confusion matrix
            plot_confusion_matrix(y, y_pred, model.classes_)
            
            return model, vectorizer
        # Plot confusion matrix
        def plot_confusion_matrix(true_labels, predicted_labels, class_names):
            """
            Plot a confusion matrix.
            """
            cm = confusion_matrix(true_labels, predicted_labels, labels=class_names)
            plt.figure(figsize=(8, 6))
            sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=class_names, yticklabels=class_names)
            plt.title("Confusion Matrix")
            plt.xlabel("Predicted Label")
            plt.ylabel("True Label")
            plt.tight_layout()
            plt.show()
        
        # Visualize emotion distribution
        def visualize_emotion_distribution(df):
            """
            Visualize the distribution of emotions in the dataset.
            """
            emotion_counts = df['label'].value_counts()
            
            # Bar plot
            plt.figure(figsize=(10, 6))
            sns.barplot(x=emotion_counts.index, y=emotion_counts.values, palette="viridis")
            plt.title("Emotion Distribution")
            plt.xlabel("Emotion")
            plt.ylabel("Frequency")
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()
        
            # Word cloud
            plt.figure(figsize=(8, 8))
            wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(emotion_counts.to_dict())
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.title("Word Cloud of Emotions")
            plt.axis("off")
            plt.tight_layout()
            plt.show()
        csv_file_path = "analysis_csv_result/sentence_emotion_analysis_results.csv"
        ########################################### TRAIN MODEL USING LEXICON DATA #############################################
        # Load and preprocess data
        df = load_data(csv_file_path)
        df = preprocess_data(df)
        
        # Visualize the dataset
        visualize_emotion_distribution(df)
        
        # Train the model with the entire dataset
        model, vectorizer = train_model(df)
        # Load the trained model and vectorizer
        def load_trained_model():
            """
            Load the trained Naive Bayes model and TF-IDF vectorizer.
            """
            try:
                model = joblib.load('emotion_model.pkl')
                vectorizer = joblib.load('tfidf_vectorizer.pkl')
                return model, vectorizer
            except FileNotFoundError:
                raise FileNotFoundError("Trained model or vectorizer files not found. Please ensure they exist.")
        
        # Preprocess the extrinsic dataset
        def preprocess_extrinsic_data(extrinsic_df):
            """
            Preprocess the extrinsic dataset by renaming columns and ensuring labels are standardized.
            """
            # Convert 'emotion' column to lowercase (handle case sensitivity)
            extrinsic_df['emotion'] = extrinsic_df['emotion'].str.lower()
            
            # Rename columns to match the training dataset
            extrinsic_df.rename(columns={'sentence': 'cleaned_text', 'emotion': 'label'}, inplace=True)
            
            # Normalize label names
            extrinsic_df['label'] = extrinsic_df['label'].replace({
                'sad': 'sadness', 
                'angry': 'anger', 
                'happy': 'happy', 
                'fear': 'fear', 
                'love': 'love', 
                'surprise': 'surprise',
            })
            
            # Drop rows with missing values
            extrinsic_df = extrinsic_df.dropna(subset=['cleaned_text', 'label'])
            
            # Ensure 'cleaned_text' contains only strings
            extrinsic_df = extrinsic_df[extrinsic_df['cleaned_text'].apply(lambda x: isinstance(x, str))]
            
            # Ensure 'label' contains only valid emotions
            valid_labels = ['sadness', 'anger', 'happy', 'fear', 'love', 'surprise', 'neutral', 'jealous']
            extrinsic_df = extrinsic_df[extrinsic_df['label'].isin(valid_labels)]
            
            # Extract only the 'cleaned_text' and 'label' columns
            extrinsic_df = extrinsic_df[['cleaned_text', 'label']]
            
            # Debugging: Print the first few rows and column names
            print(extrinsic_df.head())
            print(extrinsic_df.columns)
            print(extrinsic_df['label'].unique())
            
            return extrinsic_df
        
        # Perform extrinsic evaluation
        def extrinsic_evaluation(model, vectorizer, extrinsic_df):
            """
            Perform extrinsic evaluation on the new dataset using the trained model.
            """
            # Vectorize the extrinsic dataset
            X_extrinsic = vectorizer.transform(extrinsic_df['cleaned_text'].tolist())
            y_extrinsic = extrinsic_df['label']
            
            # Make predictions on the extrinsic dataset
            y_pred_extrinsic = model.predict(X_extrinsic)
            
            # Evaluate the model
            accuracy_extrinsic = accuracy_score(y_extrinsic, y_pred_extrinsic)
            print(f"Extrinsic Evaluation Accuracy: {accuracy_extrinsic:.2f}")
            print("\nExtrinsic Evaluation Classification Report:")
            print(classification_report(y_extrinsic, y_pred_extrinsic, target_names=model.classes_))
            
            # Confusion matrix
            conf_matrix_extrinsic = confusion_matrix(y_extrinsic, y_pred_extrinsic, labels=model.classes_)
            plt.figure(figsize=(8, 6))
            sns.heatmap(conf_matrix_extrinsic, annot=True, fmt='d', cmap='Blues', xticklabels=model.classes_, yticklabels=model.classes_)
            plt.title("Extrinsic Evaluation Confusion Matrix")
            plt.xlabel("Predicted")
            plt.ylabel("Actual")
            plt.show()
        
        # Visualize emotion distribution
        def visualize_emotion_distribution(df):
            """
            Visualize the distribution of emotions in the dataset.
            """
            emotion_counts = df['label'].value_counts()
            
            # Bar plot
            plt.figure(figsize=(10, 6))
            sns.barplot(x=emotion_counts.index, y=emotion_counts.values, palette="viridis")
            plt.title("Emotion Distribution")
            plt.xlabel("Emotion")
            plt.ylabel("Frequency")
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()
        
            # Word cloud
            plt.figure(figsize=(8, 8))
            wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(emotion_counts.to_dict())
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.title("Word Cloud of Emotions")
            plt.axis("off")
            plt.tight_layout()
            plt.show()
        # Path to your extrinsic dataset
        extrinsic_csv_file_path = "extrinsic_sample_data/emotion_sample.csv"
        
        
        # Load and preprocess the extrinsic dataset
        extrinsic_df = pd.read_csv(extrinsic_csv_file_path)
        extrinsic_df = preprocess_extrinsic_data(extrinsic_df)
        
        # Visualize the extrinsic dataset
        visualize_emotion_distribution(extrinsic_df)
        
        # Perform extrinsic evaluation
        extrinsic_evaluation(model, vectorizer, extrinsic_df)

            

    



















        
