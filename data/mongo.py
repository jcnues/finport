from functools import wraps
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from config import Config
import pandas as pd

_cfg = Config()

class MongoConnector:
    def __init__ (self, database: str = None, collection: str = None):
        self.uri = f"mongodb+srv://{_cfg.mongo_user}:{_cfg.mongo_pass}@finport.eecbysa.mongodb.net/?retryWrites=true&w=majority&appName=FinPort"
        
        self.client = None
        self.database = database
        self.collection = collection
        
    def manage_conn_decorator(func):
        """
        Decorator to handle connecting and closing MongoDB for a method.
        """
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            close_conn = self.client is None
            self.connect()
            try:
                return func(self, *args, **kwargs)
            except MongoConnectorError as e:
                print(e)
            finally:
                if close_conn:
                    self.close()
        return wrapper
        
    def connect(self):
        # if client has not been initialized, create one
        if self.client is None:
            # connect to MongoClient
            self.client = MongoClient(self.uri, server_api=ServerApi('1'))

            try:
                # ping to validate connection
                self.client.admin.command('ping')
                print("Pinged your deployment. You successfully connected to MongoDB!")
            except Exception as e:
                self.client = None
                print(e)
            
    def get_collection(self, database: str = None, collection: str = None):
        """
        Gets collection from database
        Args:
            database (str, optional): the database the collection is in. Defaults to None.
            collection (str, optional): the collection we are getting. Defaults to None.

        Raises:
            MongoConnectorError: Raised if collection or database is invalid

        Returns:
            Collection: The collection from the database
        """
        # chooses to use provided collection or class connection, same with db
        coll = collection or self.collection
        db = database or self.database
        
        # gets database client
        db_client = self.client.get_database(db)
        
        # validate collection exists in database, if not, raise error
        if not coll in db_client.list_collection_names():
            raise MongoConnectorError(f"Invalid database or collection: {database}.{collection}")
        
        # return collection
        return db_client.get_collection(coll)

    @manage_conn_decorator
    def query(self, query: dict = None, database: str = None, collection: str = None, as_df: bool = False):
        """
        Queries MongoDB from provided database/collection or from initialized database/collection

        Args:
            query (dict, optional): Query for MongoDb. Defaults to None.
            database (str, optional): database to query. Defaults to None.
            collection (str, optional): collection to query. Defaults to None.
            as_df (bool, optional): Flag indicating to return as dataframe. Defaults to False.

        Returns:
            pd.DataFrame | list: result of the query
        """
        # get collection and query it 
        coll = self.get_collection(database, collection)
        res = list(coll.find(query))
        
        # return result
        return pd.DataFrame(res) if as_df else res
        
    @manage_conn_decorator
    def insert(self, item: list[dict] | dict, database: str = None, collection:str = None) -> bool:
        """
            Queries MongoDB from provided database/collection or from initialized database/collection

            Args:
                query (dict, optional): Query for MongoDb. Defaults to None.
                database (str, optional): database to query. Defaults to None.
                collection (str, optional): collection to query. Defaults to None.
                as_df (bool, optional): Flag indicating to return as dataframe. Defaults to False.

            Returns:
                bool: Indicating insertion has succeeded
        """
        # get collection and query it 
        coll = self.get_collection(database, collection)
        item_l = item if isinstance(item, list[dict]) else list(item)
        
        return coll.insert_many(item_l).acknowledged

    def close(self):
        self.client.close()
        
class MongoConnectorError(Exception):
    def __init__ (self, msg: str):
        super().__init__(msg)
        
if __name__ == '__main__':
    mongo = MongoConnector()
    mongo.connect()