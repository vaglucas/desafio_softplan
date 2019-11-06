
import unittest
import numpy as np
import json
from s3.s3 import S3Bucket
from datetime import datetime

class TestPortfolioMeth(unittest.TestCase):
    
    def __init__(self):
        self.bucket_name = 'bucket_test_{}'.format(str(datetime.now()))
    
    def test_creat_bucket(self):
        s3 = S3Bucket()
        response = s3.create_bucket_s3(self.bucket_name)
        self.assertTrue(response)
   
    def put_file(self):
        s3 = S3Bucket()
        out_file = open("tests/test.csv","r+")
        response = s3.upload_file(out_file, bucket_name=self.bucket_name, object_name='test.csv')
        self.assertTrue(response)
        
                
    def delete_bucket(self):
        s3 = S3Bucket()
        response = s3.delete_bucket_s3(self.bucket_name)
        self.assertTrue(response)
if __name__ == '__main__':
    unittest.main()
    
