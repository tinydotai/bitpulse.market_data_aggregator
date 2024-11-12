from datetime import datetime, timedelta
from typing import List, Dict
import json
from bson import json_util
from service.mongo import MongoDBHelper
from service.s3 import S3Helper
import io

class TradingDataProcessor:
    def __init__(self, 
                 database_name: str,
                 collection_name: str):
        self.mongo_helper = MongoDBHelper(database_name)
        self.mongo_helper.set_collection(collection_name)
        self.s3_helper = S3Helper()

    def get_documents_older_than_4h(self) -> List[Dict]:
        """Fetch documents older than 4 hours from MongoDB."""
        cutoff_time = datetime.utcnow() - timedelta(hours=4)
        
        query = {
            "symbol": {"$in": ["BTCUSDT", "ETHUSDT"]},
            "timestamp": {
                "$lt": cutoff_time
            }
        }
        from pprint import pprint
        pprint(query)
        
        return self.mongo_helper.find_many(query)

    def group_by_hour(self, documents: List[Dict]) -> Dict[str, List[Dict]]:
        """Group documents by hour intervals."""
        hourly_groups = {}
        
        for doc in documents:
            if doc is None:
                continue
            
            # Get the start of the hour for the document's timestamp
            timestamp = doc['timestamp']
            hour_start = timestamp.replace(minute=0, second=0, microsecond=0)
            hour_end = hour_start + timedelta(hours=1)
            
            # Create key for the hour group
            key = f"{doc['symbol']}_{doc['source']}_{hour_start.strftime('%Y-%m-%d_%H')}"
            
            if key not in hourly_groups:
                hourly_groups[key] = {
                    'start_time': hour_start,
                    'end_time': hour_end,
                    'symbol': doc['symbol'],
                    'source': doc['source'],
                    'documents': []
                }
            
            hourly_groups[key]['documents'].append(doc)
        
        return hourly_groups

    def upload_hour_group_to_s3(self, key: str, group: Dict) -> bool:
        """Upload a single hour group directly to S3."""
        try:
            # Prepare the output data structure
            output_data = {
                "symbol": group['symbol'],
                "source": group['source'],
                "interval_start": group['start_time'].strftime("%Y-%m-%d %H:%M:%S"),
                "interval_end": group['end_time'].strftime("%Y-%m-%d %H:%M:%S"),
                "total_documents": len(group['documents']),
                "documents": group['documents']
            }
            
            # Convert to JSON string
            json_data = json.dumps(output_data, default=json_util.default, indent=2)
            
            # Create S3 key with symbol-based path
            s3_key = f"trading_data/{group['symbol']}/{key}.json"
            
            # Upload directly to S3
            self.s3_helper.s3_client.put_object(
                Bucket=self.s3_helper.bucket_name,
                Key=s3_key,
                Body=json_data
            )
            
            print(f"Uploaded to S3: {s3_key} with {len(group['documents'])} documents")
            return True
            
        except Exception as e:
            print(f"Error uploading to S3: {e}")
            return False

    def process_and_save(self):
        """Main method to process documents and save directly to S3."""
        try:
            # Fetch documents
            documents = self.get_documents_older_than_4h()
            if not documents:
                print("No documents found older than 4 hours.")
                return
            
            # Group documents by hour
            hourly_groups = self.group_by_hour(documents)
            
            # Upload each group directly to S3
            uploaded_count = 0
            for key, group in hourly_groups.items():
                if self.upload_hour_group_to_s3(key, group):
                    uploaded_count += 1
            
            print(f"Successfully processed {len(documents)} documents.")
            print(f"Uploaded {uploaded_count} files to S3.")
            
        except Exception as e:
            print(f"Error processing documents: {e}")
        finally:
            self.mongo_helper.close_connection()

def main():
    # Configuration
    config = {
        "database_name": "bitpulse_v2",
        "collection_name": "transactions_stats_second"
    }
    
    # Initialize and run processor
    processor = TradingDataProcessor(**config)
    processor.process_and_save()

if __name__ == "__main__":
    main()