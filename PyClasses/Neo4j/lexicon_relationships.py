from neo4j import GraphDatabase

class LexiconRelManager:
    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def close(self):
        self.driver.close()
        
    # This method is used to establish relationships between nodes
    def create_node_relationship(self, node1_label, node1_property, node1_name, node2_label, node2_property, node2_name, relationship_type):
        query = f"""
        MATCH (n1:{node1_label} {{{node1_property}: $node1_name}}), (n2:{node2_label} {{{node1_property}: $node2_name}})
        CREATE (n1)-[:{relationship_type}]->(n2)
        """
        # Missing: should check if relationship is already established
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
            print(f"Successfully established \"{node1_name} {relationship_type} {node2_name}\" relationship(s)!")
        except Exception as e:
            print(f"Error creating relationship: {e}")

    # This method will create a SYNONYM relationship between nodes
    # def create_synonym_rel(self, word_meta):
    #     synonyms = word_meta.get("syonyms", []) or []
        


    # This method will create an ANTONYM relationship between nodes

    

    # This method will be used to create a relationship between a word and an article --> don't use yet
    # def create_article_rel(self, word, article_meta):
    #     with self.driver.session() as session:
    #         query = """
    #         MATCH (w:WORD {text: $word})
    #         MERGE (a:ARTICLE {url: $url})
    #         ON CREATE SET
    #             a.title = $title,
    #             a.publish_time = $publish_time
    #             a.author = $author
    #         MERGE (w)-[:MENTIONED_IN]->(a)
    #         RETURN w, a
    #         """

    #         result = session.run(query, {
    #             'word': word,
    #             'url': article_meta.get("url", ""),
    #             'title': article_meta.get("title", ""),
    #             'publish_time': article_meta.get("publish_time", ""),
    #             'author': article_meta.get("authorDetails",{}).get("authName", "")
    #         })

    #         return result.single()