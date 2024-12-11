from neo4j import GraphDatabase
from PyClasses.Scrapers.PRPMScraper import PRPMScraper

class LexiconDBManager:
    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)
        self._prpm_scraper = PRPMScraper()

    def close(self):
        self.driver.close()

    def create_word_node(self, word):
        word_meta = self._prpm_scraper.findWordMetaData(word)

        if not word_meta:
            print(f"No metadata was found for {word}.")
            return None

        word_info = word_meta.get(word, {})
        word_meanings = word_info.get("meanings", [])
        word_synonyms = word_info.get("synonym", [])

        part_of_speech = "Unknown" # Temporarily define this

        # Write Cypher query to create a word node
        query = """
        MERGE (w:WORD {text: $word})
        ON CREATE SET
            w.part_of_speech = $part_of_speech,
            w.definitions = $definitions,
            w.synonyms = $word_synonyms
        ON MATCH SET
            w.part_of_speech = COALESCE(w.part_of_speech, $part_of_speech)
            w.definitions = CASE
                WHEN w.definitions IS NULL THEN $definitions
                ELSE w.definitions + $new_definitions
            END,
            w.synonyms = CASE
                WHEN w.synonyms IS NULL THEN $word_synonyms
                ELSE w.synonyms + $new_synonyms
            END,
        RETURN w
        """

        with self.driver.session() as session:
            result = session.run(query, {
                'word': word,
                'part_of_speech': part_of_speech,
                'definitions': word_meanings,
                'synonyms': word_synonyms,
                'new_definitions': word_meanings,
                'new_synonyms': word_synonyms
            })

        return result.single()[0]

    def batch_create_nodes(self, words):
        created_words = []
        for word in words:
            word_node = self.create_word_node(word)
            if word_node:
                created_words.append(word_node)
        return created_words

    def create_article_rel(self, word, article_meta):
        # This will be used to create a relationship between a word and an article
        with self.driver.session() as session:
            query = """
            MATCH (w:WORD {text: $word})
            MERGE (a:ARTICLE {url: $url})
            ON CREATE SET
                a.title = $title,
                a.publish_time = $publish_time
                a.author = $author
            MERGE (w)-[:MENTIONED_IN]->(a)
            RETURN w, a
            """

            result = session.run(query, {
                'word': word,
                'url': article_meta.get("url", ""),
                'title': article_meta.get("title", ""),
                'publish_time': article_meta.get("publish_time", ""),
                'author': article_meta.get("authorDetails",{}).get("authName", "")
            })

            return result.single()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc, tb):
        self.close()

    
            
            



        
            
            