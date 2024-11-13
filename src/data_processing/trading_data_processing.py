from datetime import datetime, timedelta
from typing import List, Dict
import json
from bson import json_util
from service.mongo import MongoDBHelper
from service.s3 import S3Helper

class TradingDataProcessor:
    def __init__(self, 
                 database_name: str,
                 collection_name: str):
        self.mongo_helper = MongoDBHelper(database_name)
        self.mongo_helper.set_collection(collection_name)
        self.s3_helper = S3Helper()

    def get_documents_older_than_24h(self) -> List[Dict]:
        """Fetch documents older than 24 hours from MongoDB."""
        current_time = datetime.utcnow()
        # Round up to the next hour
        if current_time.minute > 0 or current_time.second > 0:
            current_time = (current_time + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        
        cutoff_time = current_time - timedelta(hours=24)
        
        query = {
            "timestamp": {
                "$lt": cutoff_time
            }
        }
        from pprint import pprint
        print("Query cutoff time:")
        print(f"Current time: {datetime.utcnow()}")
        print(f"Rounded current time: {current_time}")
        print(f"Processing documents older than: {cutoff_time}")
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
                    'documents': [],
                    'document_ids': []
                }
            
            hourly_groups[key]['documents'].append(doc)
            hourly_groups[key]['document_ids'].append(doc['_id'])
        
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
            s3_key = f"data/{group['symbol']}/{key}.json"
            
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

    def delete_processed_documents(self, document_ids: List) -> int:
        """Delete processed documents from MongoDB."""
        try:
            query = {
                "_id": {"$in": document_ids}
            }
            deleted_count = self.mongo_helper.delete_many(query)
            print(f"Deleted {deleted_count} documents from MongoDB")
            return deleted_count
            
        except Exception as e:
            print(f"Error deleting documents from MongoDB: {e}")
            return 0

    def verify_deletion(self) -> bool:
        """Verify that no documents older than 24 hours remain."""
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        query = {
            "timestamp": {"$lt": cutoff_time}
        }
        remaining_docs = self.mongo_helper.find_many(query)
        return len(remaining_docs) == 0

    def process_upload_and_cleanup(self):
        """Main method to process documents older than 24h, upload to S3, and clean up MongoDB."""
        try:
            # Fetch documents older than 24 hours
            print("Fetching documents older than 24 hours...")
            documents = self.get_documents_older_than_24h()
            if not documents:
                print("No documents found older than 24 hours.")
                return
            
            # Group documents by hour
            print(f"Grouping {len(documents)} documents by hour...")
            hourly_groups = self.group_by_hour(documents)
            print(f"Found {len(hourly_groups)} hour groups")
            
            # Process each group
            total_processed = 0
            total_deleted = 0
            failed_uploads = []
            
            for key, group in hourly_groups.items():
                print(f"\nProcessing group: {key}")
                print(f"Documents in group: {len(group['documents'])}")
                
                # Upload to S3
                if self.upload_hour_group_to_s3(key, group):
                    total_processed += 1
                    # Delete from MongoDB only if S3 upload was successful
                    deleted_count = self.delete_processed_documents(group['document_ids'])
                    total_deleted += deleted_count
                else:
                    failed_uploads.append(key)
                    print(f"Failed to upload group: {key}")
            
            # Verify deletion
            all_deleted = self.verify_deletion()
            
            print(f"\nProcessing Summary:")
            print(f"- Total documents processed: {len(documents)}")
            print(f"- Hour groups processed: {len(hourly_groups)}")
            print(f"- Files uploaded to S3: {total_processed}")
            print(f"- Documents deleted from MongoDB: {total_deleted}")
            print(f"- All old documents deleted: {'Yes' if all_deleted else 'No'}")
            
            if failed_uploads:
                print(f"\nFailed uploads ({len(failed_uploads)}):")
                for failed_key in failed_uploads:
                    print(f"- {failed_key}")
            
        except Exception as e:
            print(f"Error in processing: {e}")
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
    processor.process_upload_and_cleanup()

if __name__ == "__main__":
    main()