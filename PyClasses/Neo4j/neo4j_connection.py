from neo4j import GraphDatabase
import os

class Neo4jConnection():
    def __init__(self, uri, auth):
        self.driver = None
        self.uri = uri
        self.auth = auth

    def connect(self):
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=self.auth)
            self.driver.verify_connectivity()
            print("Connection established successfully!")
            return self.driver
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            raise

    def close(self):
        if self.driver:
            self.driver.close()
            print("Database connection lost.")

    def get_driver(self):
        if not self.driver:
            self.connect()
        return self.driver

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exec_type, exec_val, exc_tb):
        self.close()
        
    