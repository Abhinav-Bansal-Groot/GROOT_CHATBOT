from pymongo import MongoClient
from app.core import config

class MongoRepository:
    def __init__(self, uri: str = config.settings.MONGODB_URI, db_name: str = config.settings.MONGO_DB):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]


    def get_documents(self, collection_name: str) -> list[dict]:
        return list(self.db[collection_name].find())


    def seed_collections(self, seed_data: dict):
        for coll, docs in seed_data.items():
            if isinstance(docs, list):
                self.db[coll].delete_many({})
                if docs:
                    self.db[coll].insert_many(docs)