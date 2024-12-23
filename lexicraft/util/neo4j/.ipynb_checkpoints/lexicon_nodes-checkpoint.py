from neo4j import GraphDatabase

class LexiconNodeManager:
    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)
        print("Established Connection")

    def close(self):
        print("connection lost")
        self.driver.close()

    def create_custom_query(self,query):
        try:
            with self.driver.session() as session:
                    result = session.run(query).data()
                    print(f"Successfully run {query}!")
                    #return result.single()[node_count]
                    return result
        except Exception as e:
            print(f"Error executing query! Query: {query}")
            return None

    def get_all_words(self):
        query = """ 
        MATCH (n:WORD)
        RETURN n.word;
        """
        try:
            with self.driver.session() as session:
                    result = session.run(query).data()
                    print(f"Successfully retrieved all words no. of words = {len(result)}!")
                    #return result.single()[node_count]
                    return result
        except Exception as e:
            print(f"Error retrieving all words. {e}")
            return None

            
    # This method is used to create the WORD node
    def create_word_node(self, word_meta, article_word):
        #{'word':'ab': ,'meanings': ['kep anggaran belanja. (kamus dewan edisi keempat)'], 'synonym': [], 'antonym': [], 'base': 'ab', 'count': 3, 'POS': 'Proper noun', 'SentimentLabel': 'neutral'}
        try:
            # Extract word information
            word = word_meta.get("word", "") or""
            definitions = word_meta.get("meanings", []) or [] # Returns meaning(s) of the word
            synonyms = word_meta.get("synonym", []) or [] # Returns synonym(s) of the word
            antonyms = word_meta.get("antonym", []) or []
            base_word = word_meta.get("base", "") or ""
            word_count = word_meta.get("count", 0) or 0
            pos = word_meta.get("POS", "") or ""
            label = word_meta.get("SentimentLabel", "") or ""
            #word_count = content.lower().count(word.lower()) # Returns the no. of occurence of the word in the article --> to be changed

            # Write Cypher query to create a WORD node
            if article_word:
                query = """
                MERGE (w:WORD {word: $word})
                ON CREATE SET w.definitions = $definitions,
                              w.word_count = $word_count,
                              w.POS = $pos,
                              w.Label = $label
                ON MATCH SET w.word_count = w.word_count + $word_count
                RETURN w, count(w) AS node_count
                """
            else:
                query = """
                MERGE (w:WORD {word: $word})
                ON CREATE SET w.definitions = $definitions,
                              w.word_count = $word_count,
                              w.POS = $pos,
                              w.Label = $label
                RETURN w, count(w) AS node_count
                """
            # Parameters
            params = {
                'word': word,
                'definitions': definitions,
                'word_count': word_count,
                'pos': pos,
                'label': label
            }
            # Execute the query
            with self.driver.session() as session:
                result = session.run(query, params)
                node_count = result.single()["node_count"]
                print(f"Successfully created \"{word}\" new WORD node(s)! {node_count}")
                #return result.single()[node_count]
                return None
            
        except Exception as e:
            print(f"Error creating WORD node for '{word}': {e}")
            return None

    # This method is used to create the WORD node
    def create_peri_node(self, peri_metadata):
        #{'peri': 'agihagih kungkang kungkang kera kecil', 'meaning': 'Terlampau murah hati atau suka memberi, hingga menderita kesusahan sendiri.'}
        try:
            # Extract word information
            peribahasa = peri_metadata.get("peri", "") or""
            meaning = peri_metadata.get("meaning", "") or "" # Returns meaning(s) of the word

            # Write Cypher query to create a WORD node
            query = """
            MERGE (w:PERIBAHASA {peribahasa: $peribahasa})
            ON CREATE SET w.meaning = $meaning
            RETURN w, count(w) AS node_count
            """
            # Parameters
            params = {
                'peribahasa': peribahasa,
                'meaning': meaning
            }
            # Execute the query
            with self.driver.session() as session:
                result = session.run(query, params)
                node_count = result.single()["node_count"]
                print(f"Successfully created \"{peribahasa}\" new PERIBAHASA node(s)! {node_count}")
                #return result.single()[node_count]
                return None
            
        except Exception as e:
            print(f"Error creating WORD node for '{word}': {e}")
            return None


    # This method is used to create the ARTICLES node
    def create_articles_node(self, article_meta):
        try:
            # Extract article information
            title = article_meta.get("title", "") or ""
            author = article_meta.get("authDetails", {}).get("authName", "") or "" # Get the author's name only
            articleBrief = article_meta.get("articleBrief", "") or ""
            contents = article_meta.get("contents", []) or []
            url = article_meta.get("url", "") or ""

            # Write Cypher query to create a ARTICLE node
            query = """
            MERGE (a:ARTICLE {title: $title})
            ON CREATE SET a.author = $author, 
                          a.articleBrief = $articleBrief,
                          a.contents = $contents,
                          a.url = $url
            RETURN a, count(a) AS node_count
            """

            # Parameters
            params = {
                'title': title,
                'author': author,
                'articleBrief': articleBrief,
                'contents': contents,
                'url': url
            }

            # Execute the query
            with self.driver.session() as session:
                result = session.run(query, params)
                node_count = result.single()["node_count"]
                print(f"Successfully created {node_count} new ARTICLE node(s)!")
                #return result.single()[0]
                return None
            
        except Exception as e:
            print(f"Error creating ARTICLE node for '{title}': {e}")
            return None


    # This method is used to create the AUTHORS node
    def create_authors_node(self, auth_meta):
        try:
            authDetails = auth_meta.get("authDetails", {}) or {}
            authName = auth_meta.get(authDetails).get("authName", "") or ""
            relLink = auth_meta.get(authDetails).get("relLink", "") or ""
            email = auth_meta.get(authDetails).get("email", "") or ""

            # Write Cypher query to create a ARTICLE node
            query = """
            MERGE (o:AUTHORS {name: $authName})
            ON CREATE SET o.relLink = $relLink, 
                          o.email = $email,
            RETURN o
            """

            # Parameters
            params = {
                'name': authName,
                'relLink': relLink,
                'email': email
            }

            # Execute the query
            with self.driver.session() as session:
                result = session.run(query, params)
                # print("Successfully created new ARTICLE nodes!") # This message should be changed --> should return 'Successfully created 6 new nodes'
                return result.single()[0]
            
        except Exception as e:
            print(f"Error creating AUTHORS node for '{authName}': {e}")
            return None
        

    # This method is used to create the PERIBAHASA node
    #def create_peri_node(self, peri_meta): --> DO LATER


    def batch_create_nodes(self, words):
        created_words = []
        for word in words:
            word_node = self.create_word_node(word)
            if word_node:
                created_words.append(word_node)
        return created_words

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
    

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc, tb):
        self.close()

    
            
            



        
            
            