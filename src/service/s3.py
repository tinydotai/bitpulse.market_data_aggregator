# service/s3.py
import os
import boto3
from typing import List, Union
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

class S3Helper:
    def __init__(self):
        """Initialize S3 client with credentials from environment variables."""
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.region_name = os.getenv('AWS_REGION', 'ap-south-1')
        self.bucket_name = os.getenv('AWS_BUCKET_NAME')
        
        if not all([self.aws_access_key_id, self.aws_secret_access_key, self.bucket_name]):
            raise ValueError("AWS credentials or bucket name not found in environment variables")
        
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )

    def upload_data(self, data: Union[str, bytes], s3_key: str) -> bool:
        """
        Upload data directly to S3 without creating a local file.
        
        Args:
            data: The data to upload (string or bytes)
            s3_key: The key (path) where the data will be stored in S3
        
        Returns:
            bool: True if upload was successful, False otherwise
        """
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=data
            )
            print(f"Successfully uploaded data to s3://{self.bucket_name}/{s3_key}")
            return True
            
        except ClientError as e:
            print(f"Error uploading data to S3: {e}")
            return False

    def download_file(self, s3_key: str) -> Union[str, None]:
        """
        Download a file from S3 bucket and return its contents.
        
        Args:
            s3_key: The key of the file in S3
        
        Returns:
            str: File contents if successful, None if failed
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            return response['Body'].read().decode('utf-8')
            
        except ClientError as e:
            print(f"Error downloading file from S3: {e}")
            return None

    def list_files(self, prefix: str = "") -> List[str]:
        """
        List all files in the S3 bucket with given prefix.
        
        Args:
            prefix: Filter results to files beginning with this prefix
        
        Returns:
            List[str]: List of file keys in the bucket
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
            
        except ClientError as e:
            print(f"Error listing files in S3: {e}")
            return []

    def delete_file(self, s3_key: str) -> bool:
        """
        Delete a file from S3 bucket.
        
        Args:
            s3_key: The key of the file to delete
        
        Returns:
            bool: True if deletion was successful, False otherwise
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            print(f"Successfully deleted s3://{self.bucket_name}/{s3_key}")
            return True
            
        except ClientError as e:
            print(f"Error deleting file from S3: {e}")
            return False