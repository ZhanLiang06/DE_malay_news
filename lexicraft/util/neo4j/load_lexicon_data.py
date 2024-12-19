from neo4j import GraphDatabase
import os
import pandas as pd

import os
import pandas as pd
from neo4j import GraphDatabase

class LoadLexData:
    def __init__(self, uri, auth, csv_directory):
        self.driver = GraphDatabase.driver(uri, auth=auth)
        self.csv_directory = csv_directory

    def close(self):
        self.driver.close()

    def load_csv_files(self):
        csv_files = [f for f in os.listdir(self.csv_directory) if f.endswith('.csv')]
        
        if not csv_files:
            raise ValueError(f"No CSV files found in {self.csv_directory}")
        
        dataframes = []
        for file in csv_files:
            file_path = os.path.join(self.csv_directory, file)
            try:
                df = pd.read_csv(file_path)
                dataframes.append((file, df))
                print(f"Loaded CSV file: {file}")
            except Exception as e:
                print(f"Error loading {file}: {e}")
        
        return dataframes

    def create_cypher_query(self, df, node_label):
        # Dynamically create node properties from DataFrame columns
        columns = df.columns.tolist()
        
        # Create Cypher query
        properties = ', '.join([f'{col}: row.{col}' for col in columns])
        query = f"""
        UNWIND $batch AS row
        CREATE (n:{node_label} {{ {properties} }})
        """
        
        return query, df.to_dict('records')

    def load_data_to_neo4j(self, node_label=None):
        # Load CSV files
        csv_dataframes = self.load_csv_files()
        
        # Process each DataFrame
        with self.driver.session() as session:
            for filename, df in csv_dataframes:
                # Use provided node_label or derive from filename
                current_node_label = node_label or os.path.splitext(filename)[0]
                
                # Create Cypher query
                query, batch = self.create_cypher_query(df, current_node_label)
                
                # Batch processing to improve performance
                batch_size = 1000
                for i in range(0, len(batch), batch_size):
                    batch_chunk = batch[i:i+batch_size]
                    try:
                        session.run(query, {'batch': batch_chunk})
                        print(f"Loaded {len(batch_chunk)} nodes with label {current_node_label}")
                    except Exception as e:
                        print(f"Error loading batch: {e}")
        
        print("CSV data loading complete.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# Example usage
# loader = CSVToNeo4jLoader(uri='bolt://localhost:7687', 
#                            auth=('neo4j', 'your_password'), 
#                            csv_directory='/path/to/csv/files')
# loader.load_data_to_neo4j(node_label='YourCustomNodeLabel')
        
    
        
        
        
        