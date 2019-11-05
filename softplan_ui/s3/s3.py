
from botocore.exceptions import ClientError
import logging
from datetime import datetime

from softplan_ui.credentials import _REGION_NAME_S3, _BUCKET, boto_sess

class S3Bucket():
  
    def __init__(self):
        
        self.s3 = boto_sess.client('s3', region_name=_REGION_NAME_S3)
    
  
    def create_bucket_s3(self, bucket_name=_BUCKET, add_trigger=False, lambda_arn=None):
        """ Create an Amazon S3 bucket
    
        :param bucket_name: Unique string name
        :return: True if bucket is created, else False
        """
        try:            
            response = self.s3.list_buckets()
            map_tr = [True if a['Name'] == bucket_name else False for a in response['Buckets']]
            if True not in map_tr:
                self.s3.create_bucket(Bucket=bucket_name)
            else:
                return True
           
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def upload_file(self, file, bucket_name=_BUCKET, object_name=None, type_obj = 'text/csv'):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        
        The upload_fileobj method accepts a readable file-like object. The file object must be opened in binary mode, not text mode.
        """
        
        if object_name is None:
            object_name = file
        try:
            response = self.s3.put_object(Body=file, Bucket=bucket_name, Key=object_name, ContentType=type_obj)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def download_file(self, object_name, file_name=None,bucket_name=_BUCKET ):

        if object_name is None:
            object_name = file_name
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return response
    
    
   