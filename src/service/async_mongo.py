import os
from typing import List, Dict, Any
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from pymongo import UpdateOne
from bson import CodecOptions
from dotenv import load_dotenv

load_dotenv()

MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASS")
MONGO_URI = f"mongodb+srv://{MONGO_USERNAME}:{MONGO_PASSWORD}@marketdata.kjzon.mongodb.net/?retryWrites=true&w=majority&appName=MarketData"

class AsyncMongoDBHelper:
    def __init__(self, database_name: str):
        self.client: AsyncIOMotorClient = AsyncIOMotorClient(MONGO_URI)
        self.db: AsyncIOMotorDatabase = self.client[database_name]
        self._collection: AsyncIOMotorCollection = None
        self._codec_options: CodecOptions = None

    def set_collection(self, collection_name: str) -> None:
        if self._codec_options:
            self._collection = self.db.get_collection(collection_name).with_options(self._codec_options)
        else:
            self._collection = self.db[collection_name]

    def set_codec_options(self, codec_options: CodecOptions) -> None:
        self._codec_options = codec_options
        if self._collection:
            self._collection = self._collection.with_options(codec_options)

    @property
    def collection(self) -> AsyncIOMotorCollection:
        if self._collection is None:
            raise ValueError("Collection is not set. Use set_collection() method.")
        return self._collection

    async def insert_one(self, document: Dict[str, Any]) -> str:
        result = await self.collection.insert_one(document)
        return str(result.inserted_id)

    async def insert_many(self, documents: List[Dict[str, Any]]) -> List[str]:
        result = await self.collection.insert_many(documents)
        return [str(id) for id in result.inserted_ids]

    async def find_one(self, query: Dict[str, Any]) -> Dict[str, Any]:
        return await self.collection.find_one(query)

    async def find_many(self, query: Dict[str, Any], limit: int = 0) -> List[Dict[str, Any]]:
        cursor = self.collection.find(query)
        if limit > 0:
            cursor = cursor.limit(limit)
        return await cursor.to_list(length=None)

    async def update_one(self, query: Dict[str, Any], update: Dict[str, Any]) -> int:
        result = await self.collection.update_one(query, {"$set": update})
        return result.modified_count

    async def update_many(self, query: Dict[str, Any], update: Dict[str, Any]) -> int:
        result = await self.collection.update_many(query, {"$set": update})
        return result.modified_count

    async def delete_one(self, query: Dict[str, Any]) -> int:
        result = await self.collection.delete_one(query)
        return result.deleted_count

    async def delete_many(self, query: Dict[str, Any]) -> int:
        result = await self.collection.delete_many(query)
        return result.deleted_count

    async def bulk_write(self, operations: List[UpdateOne]) -> None:
        await self.collection.bulk_write(operations)

    async def close_connection(self) -> None:
        self.client.close()