import redis
from typing import List, Optional, Union, Dict, Tuple

class RedisManager:
    def __init__(self, 
                 host: str = 'localhost', 
                 port: int = 6379, 
                 db: int = 0, 
                 password: Optional[str] = None):
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True
        )
    
    def add_numbered_query(self, key: str, queries: List[str]) -> List[Dict[str, Union[int, str]]]:
        current_length = self.redis_client.llen(key) # Get current list length 
        added_queries = []
        no_of_queries = 0
        
        for query in queries:
            # Only add if unique
            if self.is_unique(key, query):
                # Generate an index starting from current length
                number = current_length
                no_of_queries += 1
            
                # Create a dictionary with index and item
                query_entry = {
                    'no.': number,
                    'query': query
                }
                
                # Push to Redis list
                self.redis_client.rpush(key, f"{number}:{query}")
                added_queries.append(query_entry)
                
                # Increment for next potential item
                current_length += 1
        
        if no_of_queries == 0:
            print("No new queries were added.")
        else:
            print(f"[REDIS DATABASE] {no_of_queries} new query/queries have been added!\n")
        
        return added_queries
    
    def is_unique(self, key: str, query: str) -> bool:
        existing_list = self.redis_client.lrange(key, 0, -1)
        return not any(query in entry.split(':', 1)[1] for entry in existing_list)
    
    def get_query(self, key: str, number: int) -> Optional[str]:

        # Get the full list
        full_list = self.redis_client.lrange(key, 0, -1)
        
        # Find the entry with the matching number
        if 1 <= number <= len(full_list):
            entry = full_list[number - 1]  
            return entry.split(':', 1)[1]  
        
        return None

    # Return full list of queries
    def query_list(self, key: str) -> List[Dict[str, Union[int, str]]]:
        full_list = self.redis_client.lrange(key, 0, -1)
        numbered_queries = [] # Parse entries into a list of dictionaries
        
        for index, entry in enumerate(full_list, start=1):
            # Split the entry into number and item
            _, query = entry.split(':', 1)
            numbered_queries.append({
                'no.': index,
                'query': query
            })
        
        return numbered_queries
    
    def get_list(self, key: str) -> List[str]:
        return self.redis_client.lrange(key, 0, -1)
    
    def remove_query_by_number(self, key: str, number: int) -> bool:
        full_list = self.redis_client.lrange(key, 0, -1)
        
        for entry in full_list:
            if entry.startswith(f"{number}:"):
                # Remove the specific entry
                print("[REDIS DATABASE] A query has been removed.")
                return self.redis_client.lrem(key, 1, entry) > 0
        
        return False
    
    def clear_list(self, key: str) -> None:
        self.redis_client.delete(key)
        print(f"\n[REDIS DATABASE] Query list has been cleared.")
    
    def close(self):
        self.redis_client.close()