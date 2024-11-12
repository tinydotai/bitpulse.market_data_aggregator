from datetime import datetime, timedelta
from pathlib import Path
import json
from typing import List, Dict
import os
from bson import json_util
from service.mongo import MongoDBHelper

class TradingDataProcessor:
    def __init__(self, 
                 database_name: str,
                 collection_name: str,
                 output_dir: str = "daily_data"):
        """
        Initialize the processor with MongoDB connection and output directory.
        
        Args:
            database_name: Name of the database
            collection_name: Name of the collection
            output_dir: Directory to save output JSON files
        """
        self.mongo_helper = MongoDBHelper(database_name)
        self.mongo_helper.set_collection(collection_name)
        self.output_dir = output_dir
        Path(output_dir).mkdir(parents=True, exist_ok=True)

    def get_documents_older_than_24h(self) -> List[Dict]:
        """Fetch documents older than 24 hours from MongoDB."""
        cutoff_time = datetime.utcnow() - timedelta(hours=4)
        
        query = {
            "symbol" : {"$in" : ["BTCUSDT", "ETHUSDT"] },
            "timestamp": {
                "$lt": cutoff_time
            }
        }
        from pprint import pprint
        pprint(query)
        
        return self.mongo_helper.find_many(query)

    def group_documents(self, documents: List[Dict]) -> Dict:
        """Group documents by symbol and source."""
        grouped_data = {}
        for doc in documents:
            if doc is None:
                continue
            
            key = f"{doc['symbol']}_{doc['source']}"
            if key not in grouped_data:
                grouped_data[key] = []
            grouped_data[key].append(doc)
        
        return grouped_data

    def save_to_json(self, grouped_data: Dict):
        """Save grouped data to JSON files."""
        for key, documents in grouped_data.items():
            if not documents:
                continue
                
            # Get the date from the first document
            date_str = documents[0]['timestamp'].strftime("%Y-%m-%d")
            filename = f"{key}_{date_str}.json"
            filepath = os.path.join(self.output_dir, filename)
            
            # Calculate aggregated statistics
            stats = self.calculate_statistics(documents)
            
            # Save to file using json_util to handle MongoDB specific types
            with open(filepath, 'w') as f:
                json.dump(stats, f, indent=2, default=json_util.default)
            print(f"Created file: {filepath}")

    def calculate_statistics(self, documents: List[Dict]) -> Dict:
        """Calculate daily statistics for a group of documents."""
        if not documents:
            return {}
        
        # Initialize statistics with first document's basic info
        stats = {
            "symbol": documents[0]["symbol"],
            "source": documents[0]["source"],
            "date": documents[0]["timestamp"].strftime("%Y-%m-%d"),
            "baseCurrency": documents[0]["baseCurrency"],
            "quoteCurrency": documents[0]["quoteCurrency"],
            "buy": {
                "total_count": 0,
                "total_quantity": 0,
                "total_value": 0,
                "min_price": float('inf'),
                "max_price": 0,
                "weighted_avg_price": 0
            },
            "sell": {
                "total_count": 0,
                "total_quantity": 0,
                "total_value": 0,
                "min_price": float('inf'),
                "max_price": 0,
                "weighted_avg_price": 0
            }
        }
        
        # Aggregate data
        for doc in documents:
            # Buy statistics
            if "buy_count" in doc:
                stats["buy"]["total_count"] += doc["buy_count"]
                stats["buy"]["total_quantity"] += doc["buy_total_quantity"]
                stats["buy"]["total_value"] += doc["buy_total_value"]
                stats["buy"]["min_price"] = min(stats["buy"]["min_price"], doc["buy_min_price"])
                stats["buy"]["max_price"] = max(stats["buy"]["max_price"], doc["buy_max_price"])
            
            # Sell statistics
            if "sell_count" in doc:
                stats["sell"]["total_count"] += doc["sell_count"]
                stats["sell"]["total_quantity"] += doc["sell_total_quantity"]
                stats["sell"]["total_value"] += doc["sell_total_value"]
                stats["sell"]["min_price"] = min(stats["sell"]["min_price"], doc["sell_min_price"])
                stats["sell"]["max_price"] = max(stats["sell"]["max_price"], doc["sell_max_price"])
        
        # Calculate weighted average prices
        if stats["buy"]["total_quantity"] > 0:
            stats["buy"]["weighted_avg_price"] = stats["buy"]["total_value"] / stats["buy"]["total_quantity"]
        if stats["sell"]["total_quantity"] > 0:
            stats["sell"]["weighted_avg_price"] = stats["sell"]["total_value"] / stats["sell"]["total_quantity"]
        
        # Clean up infinity values
        if stats["buy"]["min_price"] == float('inf'):
            stats["buy"]["min_price"] = 0
        if stats["sell"]["min_price"] == float('inf'):
            stats["sell"]["min_price"] = 0
            
        return stats

    def process_and_save(self):
        """Main method to process documents and save results."""
        try:
            # Fetch documents
            documents = self.get_documents_older_than_24h()
            if not documents:
                print("No documents found older than 24 hours.")
                return
            
            # Group documents
            grouped_docs = self.group_documents(documents)
            
            # Save to JSON files
            self.save_to_json(grouped_docs)
            
            print(f"Successfully processed {len(documents)} documents.")
            
        except Exception as e:
            print(f"Error processing documents: {e}")
        finally:
            self.mongo_helper.close_connection()

def main():
    # Configuration
    config = {
        "database_name": "bitpulse_v2",        # Update with your database name
        "collection_name": "transactions_stats_second",          # Update with your collection name
        "output_dir": "daily_data"           # Update if you want a different output directory
    }
    
    # Initialize and run processor
    processor = TradingDataProcessor(**config)
    processor.process_and_save()

if __name__ == "__main__":
    main()