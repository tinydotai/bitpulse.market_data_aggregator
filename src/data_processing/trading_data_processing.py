from datetime import datetime, timedelta
from pathlib import Path
import json
from typing import List, Dict
import os
from bson import json_util
from service.mongo import MongoDBHelper
from service.s3 import S3Helper

class TradingDataProcessor:
    def __init__(self, 
                 database_name: str,
                 collection_name: str,
                 output_dir: str = "hourly_data"):
        self.mongo_helper = MongoDBHelper(database_name)
        self.mongo_helper.set_collection(collection_name)
        self.output_dir = output_dir
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        self.s3_helper = S3Helper()  # Initialize S3 helper

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
                    'documents': []
                }
            
            hourly_groups[key]['documents'].append(doc)
        
        return hourly_groups

    def save_hourly_files(self, hourly_groups: Dict[str, List[Dict]]):
        """Save files containing all documents for each hour interval."""
        for key, group in hourly_groups.items():
            if not group['documents']:
                continue
            
            filename = f"{key}.json"
            filepath = os.path.join(self.output_dir, filename)
            
            # Count total documents
            total_docs = len(group['documents'])
            
            # Prepare the output data structure
            output_data = {
                "symbol": group['documents'][0]['symbol'],
                "source": group['documents'][0]['source'],
                "interval_start": group['start_time'].strftime("%Y-%m-%d %H:%M:%S"),
                "interval_end": group['end_time'].strftime("%Y-%m-%d %H:%M:%S"),
                "total_documents": total_docs,
                "documents": group['documents']
            }
            
            # Save to file using json_util to handle MongoDB specific types
            with open(filepath, 'w') as f:
                json.dump(output_data, f, indent=2, default=json_util.default)
            print(f"Created file: {filepath} with {total_docs} documents")

    def process_and_save(self):
        """Main method to process documents and save results."""
        try:
            # Fetch documents
            documents = self.get_documents_older_than_4h()
            if not documents:
                print("No documents found older than 4 hours.")
                return
            
            # Group documents by hour
            hourly_groups = self.group_by_hour(documents)
            
            # Save files locally
            self.save_hourly_files(hourly_groups)
            
            # Upload files to S3
            print("Uploading files to S3...")
            uploaded_files = self.s3_helper.upload_files(
                self.output_dir,
                prefix=f"hourly_data/{datetime.utcnow().strftime('%Y-%m-%d')}"
            )
            
            print(f"Successfully processed {len(documents)} documents.")
            print(f"Uploaded {len(uploaded_files)} files to S3.")
            
        except Exception as e:
            print(f"Error processing documents: {e}")
        finally:
            self.mongo_helper.close_connection()

def main():
    # Configuration
    config = {
        "database_name": "bitpulse_v2",
        "collection_name": "transactions_stats_second",
        "output_dir": "hourly_data"
    }
    
    # Initialize and run processor
    processor = TradingDataProcessor(**config)
    processor.process_and_save()

if __name__ == "__main__":
    main()