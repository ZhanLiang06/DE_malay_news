from neo4j import GraphDatabase

class LexiconDBManager:
    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def close(self):
        self.driver.close()

    def create_word_node(self, word_meta):
        try:
            # Extract word information
            word_meanings = word_meta.get("meanings", []) or [] # Returns meaning(s) of the word
            word_synonyms = word_meta.get("synonym", []) or [] # Returns synonym(s) of the word
            word_count = len(word_meta.get("word", "")) # Returns word count of the word
            
            # Write Cypher query to create a word node
            query = """
            CREATE (w:WORD {word: $word, definitions: $definitions, part_of_speech: $part_of_speech})
            RETURN w
            """
            
            # Prepare parameters, using empty lists as fallbacks
            params = {
                'word': word_meta['word'],
                'part_of_speech': "Unknown",  # Default part of speech
                'definitions': word_meanings,
                'synonyms': word_synonyms,
                'word_count': word_count,
                'new_definitions': word_meanings,
                'new_synonyms': word_synonyms
            }
            
            # Execute the query
            with self.driver.session() as session:
                result = session.run(query, params)
                return result.single()[0]
            
        except Exception as e:
            print(f"Error creating word node for '{word}': {e}")
            import traceback
            traceback.print_exc()
            return None

    def batch_create_nodes(self, words):
        created_words = []
        for word in words:
            word_node = self.create_word_node(word)
            if word_node:
                created_words.append(word_node)
        return created_words

    def create_node_relationship(self, node1_label, node1_property, node1_name, node2_label, node2_property, node2_name, relationship_type):
        query = f"""
        MATCH (n1:{node1_label} {{{node1_property}: $node1_name}}), (n2:{node2_label} {{{node1_property}: $node2_name}})
        CREATE (n1)-[:{relationship_type}]->(n2)
        """
        
        params = {
            'node1_label': node1_label,
            'node1_property': node1_property,
            'node1_name': node1_name,
            'node2_label': node2_label,
            'node2_property': node2_property,
            'node2_name': node2_name,
            'relationship_type': relationship_type
        }
        
        try:
            self.driver.execute_query(query, params)
        except Exception as e:
            print(f"Error creating relationship: {e}")

    def update_node(self, node_label, node_property, node_property_name, updates):
        try:
            # Construct the Cypher query for dynamic updates
            set_clause = ", ".join([f"n.{key} = ${key}" for key in updates.keys()])
            query = f"""
            MATCH (n:{node_label} {{{node_property}: $node_property_name}})
            SET {set_clause}
            RETURN n
            """
            
            # Combine the word and updates into parameters
            params = {
                'node_label': node_label,
                'node_property': node_property,
                'node_property_name': node_property_name
            }
            params.update(updates)
            
            # Execute the query
            with self.driver.session() as session:
                result = session.run(query, params)
                return result.single()[0] if result.peek() else None
                
        except Exception as e:
            print(f"Error updating word node for '{node_property_name}': {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def clear_db(self):
        query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]-()
        DELETE n,r
        """
        
        try:
            self.driver.execute_query(query)
        except Exception as e:
            print(f"Error clearing database: {e}")
    
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

    
            
            



        
            
            