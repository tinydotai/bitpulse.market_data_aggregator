import os
from typing import List, Dict, Any
from pymongo import MongoClient, UpdateOne
from pymongo.database import Database
from pymongo.collection import Collection
from bson import CodecOptions
from dotenv import load_dotenv

load_dotenv()

MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASS")
MONGO_URI = f"mongodb+srv://{MONGO_USERNAME}:{MONGO_PASSWORD}@marketdata.kjzon.mongodb.net/?retryWrites=true&w=majority&appName=MarketData"

class MongoDBHelper:
    def __init__(self, database_name: str):
        self.client = MongoClient(MONGO_URI)
        self.db: Database = self.client[database_name]
        self._collection: Collection = None
        self._codec_options: CodecOptions = None

    def set_collection(self, collection_name: str) -> None:
        if self._codec_options:
            self._collection = self.db.get_collection(collection_name, codec_options=self._codec_options)
        else:
            self._collection = self.db[collection_name]

    def set_codec_options(self, codec_options: CodecOptions) -> None:
        self._codec_options = codec_options
        if self._collection:
            self._collection = self._collection.with_options(codec_options)

    @property
    def collection(self) -> Collection:
        if self._collection is None:
            raise ValueError("Collection is not set. Use set_collection() method.")
        return self._collection

    def insert_one(self, document: Dict[str, Any]) -> str:
        result = self.collection.insert_one(document)
        return str(result.inserted_id)

    def insert_many(self, documents: List[Dict[str, Any]]) -> List[str]:
        result = self.collection.insert_many(documents)
        return [str(id) for id in result.inserted_ids]

    def find_one(self, query: Dict[str, Any]) -> Dict[str, Any]:
        return self.collection.find_one(query)

    def find_many(self, query: Dict[str, Any], limit: int = 0) -> List[Dict[str, Any]]:
        return list(self.collection.find(query).limit(limit))

    def update_one(self, query: Dict[str, Any], update: Dict[str, Any]) -> int:
        result = self.collection.update_one(query, {"$set": update})
        return result.modified_count

    def update_many(self, query: Dict[str, Any], update: Dict[str, Any]) -> int:
        result = self.collection.update_many(query, {"$set": update})
        return result.modified_count

    def delete_one(self, query: Dict[str, Any]) -> int:
        result = self.collection.delete_one(query)
        return result.deleted_count

    def delete_many(self, query: Dict[str, Any]) -> int:
        result = self.collection.delete_many(query)
        return result.deleted_count

    def bulk_write(self, operations: List[UpdateOne]) -> None:
        self.collection.bulk_write(operations)

    def close_connection(self) -> None:
        self.client.close()