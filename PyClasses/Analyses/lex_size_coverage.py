class LexSizeCoverage:
    def analyze_lexicon(self):
        
        # Return everything
        query = """
        MATCH (n)
        RETURN n
        """
        
        try:
            with self.driver.session() as session:
                results = session.run(query)
                lexicon = []
                for record in results:
                    lexicon.append({
                        "word": record["word"],
                        "meaningCount": record["meaningCount"],
                        "synonymCount": record["synonymCount"],
                        "antonymCount": record["antonymCount"]
                    })
                
                # Perform lexicon size and coverage analysis
                lexicon_size = len(lexicon)
                total_meanings = sum(item["meaningCount"] for item in lexicon)
                total_synonyms = sum(item["synonymCount"] for item in lexicon)
                total_antonyms = sum(item["antonymCount"] for item in lexicon)
                
                print(f"Lexicon Size: {lexicon_size}")
                print(f"Total Meanings: {total_meanings}")
                print(f"Total Synonyms: {total_synonyms}")
                print(f"Total Antonyms: {total_antonyms}")
                
                return {
                    "lexicon_size": lexicon_size,
                    "total_meanings": total_meanings,
                    "total_synonyms": total_synonyms,
                    "total_antonyms": total_antonyms
                }
        except Exception as e:
            print(f"Error performing lexicon analysis: {e}")
            return None
