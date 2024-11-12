import os
import boto3
from typing import List
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

    def upload_file(self, file_path: str, s3_key: str = None) -> bool:
        """
        Upload a file to S3 bucket.
        
        Args:
            file_path: Local path to the file
            s3_key: The key (path) where the file will be stored in S3. 
                   If None, uses the filename as key.
        
        Returns:
            bool: True if upload was successful, False otherwise
        """
        try:
            if not os.path.exists(file_path):
                print(f"File not found: {file_path}")
                return False
            
            # If s3_key is not provided, use the filename
            if s3_key is None:
                s3_key = os.path.basename(file_path)
            
            self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
            print(f"Successfully uploaded {file_path} to s3://{self.bucket_name}/{s3_key}")
            return True
            
        except ClientError as e:
            print(f"Error uploading file to S3: {e}")
            return False

    def upload_files(self, directory: str, prefix: str = "") -> List[str]:
        """
        Upload all files from a directory to S3.
        
        Args:
            directory: Local directory containing files to upload
            prefix: Prefix to add to S3 keys (like a folder path)
        
        Returns:
            List[str]: List of successfully uploaded file keys
        """
        uploaded_files = []
        
        try:
            # Ensure directory exists
            if not os.path.exists(directory):
                print(f"Directory not found: {directory}")
                return uploaded_files

            # Walk through directory
            for root, _, files in os.walk(directory):
                for file in files:
                    local_path = os.path.join(root, file)
                    
                    # Create S3 key with prefix
                    relative_path = os.path.relpath(local_path, directory)
                    s3_key = os.path.join(prefix, relative_path).replace("\\", "/")
                    
                    # Upload file
                    if self.upload_file(local_path, s3_key):
                        uploaded_files.append(s3_key)
        
        except Exception as e:
            print(f"Error uploading files to S3: {e}")
        
        return uploaded_files

    def download_file(self, s3_key: str, local_path: str) -> bool:
        """
        Download a file from S3 bucket.
        
        Args:
            s3_key: The key of the file in S3
            local_path: Local path where the file should be saved
        
        Returns:
            bool: True if download was successful, False otherwise
        """
        try:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            print(f"Successfully downloaded s3://{self.bucket_name}/{s3_key} to {local_path}")
            return True
            
        except ClientError as e:
            print(f"Error downloading file from S3: {e}")
            return False

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