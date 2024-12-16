# Create a class that will be used by clients so that they can access the db and retrieve any words they want
# Implement Redis
# Set up Redis
# Will keep frequently queried queries --> will check if present, if no then will store again

from neo4j import GraphDatabase
import redis

class Neo4j_Redis_Client:
    