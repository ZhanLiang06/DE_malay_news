import redis
from typing import Dict, List, Any, Optional
import json
from neo4j import GraphDatabase
from datetime import datetime
import math
import json
import sys, tty, termios # For Linux
import os


class RedisMenuManager:
    def __init__(self, neo4j_uri, neo4j_auth, redis_host, redis_port, redis_db):
        # Redis connection
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True
        )
        self.QUERIES_KEY = 'stored_cypher_queries'
        
        # Neo4j connection
        self.neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=neo4j_auth)

    def __del__(self):
        if hasattr(self, 'neo4j_driver'):
            self.neo4j_driver.close()
            
    def clear_screen():
        os.system('clear') # Cls --> Not working properly

    # Search word or proverb
    # rmb tolower the word/peribahsa
    # should check if the input is a word or a peribahasa
    def search_lexicon(self, search_term: str): # Why return got a None?
        try:
            # Convert to lowercase
            search_term = search_term.lower().strip()
            
            # Check if the search term exists or not
            validation_query = """
            MATCH (n)
            WHERE (n:WORD OR n:PERIBAHASA) AND 
                  (n.word = $term OR n.proverb = $term)
            RETURN labels(n) as type
            """
            
            with self.neo4j_driver.session() as session:
                result = session.run(validation_query, term=search_term)
                record = result.single()
                
                if not record:
                    print(f"\nStatus: '{search_term}' not found. Try another word or proverb.")
                    return
                
                # Determine if input is a word or peribahasa
                node_type = record['type'][0]  # Get first label
                
                # Execute appropriate query based on type
                if node_type == 'WORD':
                    word_query = """
                    MATCH (n:WORD {word: $term})
                    RETURN n.word as word,
                           n.definitions as definitions,
                           n.synonyms as synonyms,
                           n.antonyms as antonyms
                    """
                    result = session.run(word_query, term=search_term)
                    data = result.single()
                    
                    if data:
                        print("\n=== Hasil Carian (Search Results) ===")
                        print(f"\nPerkataan (Word): {data['word']}")
                        
                        print("\nDefinisi (Definitions):")
                        for i, def_ in enumerate(data['definitions'] or [], 1):
                            print(f"{i}. {def_}")
                        
                        if data['synonyms']:
                            print("\nSinonim (Synonyms):")
                            print(", ".join(data['synonyms']))
                        
                        if data['antonyms']:
                            print("\nAntonim (Antonyms):")
                            print(", ".join(data['antonyms']))
                    
                else:  # PERIBAHASA
                    peri_query = """
                    MATCH (n:PERIBAHASA {proverb: $term})
                    RETURN n.proverb as proverb,
                           n.meaning as meaning
                    """
                    result = session.run(peri_query, term=search_term)
                    data = result.single()
                    
                    if data:
                        print("\n=== Hasil Carian (Search Results) ===")
                        print(f"\nPeribahasa: {data['proverb']}")
                        print(f"Maksud (Meaning): {data['meaning']}")
                
                if not data:
                    print("\nError while searching database. Please try again.")
                
        except Exception as e:
            print(f"\nError: {str(e)}")

    # Create new query
    def create_new_query(self, query: str) -> None:
        try:
            # Validate the query structure first
            if not self.validate_query(query):
                print("Error: Invalid query structure. Query must contain 'CREATE', 'MATCH' and 'RETURN' statements.")
                return

            query_name = input("Enter a name for this query: ").strip()
            
            # Validate inputs
            if not query_name or not query:
                print("Error: Query name and query are required.")
                return
                
            # Check if query name already exists
            queries = self.redis_client.get('stored_queries') # Get existing queries
            current_queries = json.loads(queries) if queries else {}
            
            if query_name in current_queries:
                overwrite = input(f"Query '{query_name}' already exists. Overwrite? (y/n): ").lower()
                if overwrite != 'y':
                    print("Query creation cancelled.")
                    return
            
            # Create query object with metadata
            query_object = {
                'query': query,
                'created_at': datetime.now().isoformat(),
                'last_used': None
            }
            
            # Store in Redis
            current_queries[query_name] = query_object
            self.redis_client.set('stored_queries', json.dumps(current_queries))
            
            print(f"\nQuery '{query_name}' stored successfully!")
            print("\nStored Query Details:")
            print(f"Name: {query_name}")
            print(f"Query: {query}")
            print(f"Created at: {query_object['created_at']}")
            
        except redis.RedisError as e:
            print(f"Redis Error: {str(e)}")
        except Exception as e:
            print(f"Error: {str(e)}")
    
    def validate_query(self, query: str) -> bool:
        try:
            required_keywords = ['MATCH', 'RETURN', 'CREATE'] # Keywords that should be present in a Cypher query
            query_upper = query.upper()
            return any(keyword in query_upper for keyword in required_keywords)
        except Exception:
            return False

    # For Linux
    def get_key(self):
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(sys.stdin.fileno())
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return ch
    
    # List all queries --> one error 'Inappropriate ioctl for device'
    def list_all_queries(self) -> None:
        try:
            # Get all queries from Redis
            stored_queries = self.redis_client.get('stored_queries')
            if not stored_queries:
                print("\nNo queries stored.")
                return
    
            queries = json.loads(stored_queries)
            if not queries:
                print("\nNo queries stored.")
                return
    
            # Convert to list for easier pagination
            query_list = list(queries.items())
            page_size = 10
            total_pages = math.ceil(len(query_list) / page_size)
            current_page = 1
    
            while True:
                # Clear screen (Windows)
                import os
                os.system('cls' if os.name == 'nt' else 'clear')
    
                # Calculate current page items
                start_idx = (current_page - 1) * page_size
                end_idx = min(start_idx + page_size, len(query_list))
                current_items = query_list[start_idx:end_idx]
    
                # Display header
                print("\n=== Stored Queries ===")
                print(f"Page {current_page} of {total_pages}")
                print("=" * 50)
    
                # Display queries
                for i, (name, data) in enumerate(current_items, 1):
                    print(f"\n{start_idx + i}. Query Name: {name}")
                    print(f"   Query: {data['query'][:100]}..." if len(data['query']) > 100 else f"   Query: {data['query']}")
                    print(f"   Created: {data['created_at']}")
                    print(f"   Last Used: {data['last_used'] or 'Never'}")
                    print("-" * 50)
    
                # Display navigation instructions
                print("\nNavigation:")
                print("→ Next Page (Right Arrow)")
                print("← Previous Page (Left Arrow)")
                print("ESC to return to main menu")
    
                # Get keyboard input
                while True:
                    key = self.get_key()
                    
                    if key == '\x1b':  # ESC key
                        return
                        
                    if key == '\x1b':  # Arrow keys
                        next1 = self.get_key()
                        next2 = self.get_key()
                        
                        if next1 == '[':
                            if next2 == 'C' and current_page < total_pages:  # Right arrow
                                current_page += 1
                                break
                            elif next2 == 'D' and current_page > 1:  # Left arrow
                                current_page -= 1
                                break
    
        except redis.RedisError as e:
            print(f"Redis Error: {str(e)}")
        except Exception as e:
            print(f"Error: {str(e)}")
            input("Press Enter to continue...")

    # Execute queries
    def execute_queries(self) -> None:
        try:
            # Get stored queries from Redis
            stored_queries = self.redis_client.get('stored_queries')
            if not stored_queries:
                print("\nNo queries stored to execute.")
                input("Press Enter to continue...")
                return
            
            queries = json.loads(stored_queries)
            if not queries:
                print("\nNo queries stored to execute.")
                input("Press Enter to continue...")
                return
    
            while True:
                # Display all queries with numbers
                print("\n=== Available Queries ===")
                print("=" * 50)
                
                # Convert to list and sort by last_used (most recent first)
                query_list = list(queries.items())
                query_list.sort(key=lambda x: (x[1]['last_used'] or '0000', x[0]), reverse=True)
    
                for idx, (name, data) in enumerate(query_list, 1):
                    print(f"\n{idx}. Query Name: {name}")
                    print(f"   Query: {data['query'][:100]}..." if len(data['query']) > 100 else f"   Query: {data['query']}")
                    print(f"   Last Used: {data['last_used'] or 'Never'}")
                    print("-" * 50)
    
                # Get user selection
                print(f"\nOptions:")
                print("Enter the number of the query to execute.")
                print("Or enter 'q' to return to main menu.")
                
                choice = input("\nYour choice: ").strip().lower()
                
                if choice == 'q':
                    return
    
                try:
                    selection = int(choice)
                    if 1 <= selection <= len(query_list):
                        # Get the selected query
                        query_name = query_list[selection - 1][0]
                        query_data = queries[query_name]
                        
                        print("\nExecuting query...")
                        
                        # Execute the query using Neo4j session
                        with self.neo4j_driver.session() as session:
                            result = session.run(query_data['query'])
                            records = list(result)
                            
                            # Update last used timestamp
                            queries[query_name]['last_used'] = datetime.now().isoformat()
                            self.redis_client.set('stored_queries', json.dumps(queries))
                            
                            # Display results
                            print("\n=== Query Results ===")
                            print(f"Query Name: {query_name}")
                            print("=" * 50)
                            
                            if not records:
                                print("\nNo results found.")
                            else:
                                columns = records[0].keys()
                                for record in records:
                                    values = [str(record[col]) for col in columns]
                                    print(values) # here got bug --> cannot access only properties and meanings but can return query
                            
                            print(f"\nTotal records: {len(records)}")
                            
                        input("\nPress Enter to continue...\n")
                    else:
                        print("\nInvalid selection. Please choose a valid number.")
                        input("Press Enter to continue...\n")
                except ValueError:
                    print("\nInvalid input. Please enter a number or 'q'.")
                    input("Press Enter to continue...\n")
    
        except redis.RedisError as e:
            print(f"Redis Error: {str(e)}")
            input("Press Enter to continue...\n")
        except Exception as e:
            print(f"Error: {str(e)}")
            input("Press Enter to continue...\n")
        
    # Did not include delete query function
    def main_menu(self):
        while True:
            print(f"=== Lexicon Menu ===")
            print(f"[1] Search word or proverb")
            print(f"[2] Create new query")
            print(f"[3] List all queries")
            print(f"[4] Execute queries")
            print(f"[0] Exit")
            choice = input(f"\nEnter your choice (1-3 or 0): ")

            if choice == '1':
                #self.clear_screen()
                print(f"==========================================")
                search_term = input(f"What Bahasa Melayu word or proverb would you like to find?:\n")
                self.search_lexicon(search_term)

            elif choice == '2':
                #self.clear_screen()
                query = input(f"\nType your query here:\n")
                self.create_new_query(query)

            elif choice == '3':
                #self.clear_screen()
                self.list_all_queries()

            elif choice == '4':
                #self.clear_screen()
                self.execute_queries()

            elif choice == '0':
                print(f"\nExiting...")
                #self.clear_screen()
                break

            else:
                print(f"'{choice}' is an invalid input. Please try again.")
                


            