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




















        