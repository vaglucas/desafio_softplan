
from boto3.session import Session
import boto3
from decouple import RepositoryEnv, Config
import os

ambient = os.environ.get('ambient', "0")
config = Config(RepositoryEnv('config.env'))

cred = Config(RepositoryEnv('credentials.env'))
_S3API = cred.get('aws_access_key_id', None)
_S3SECRET = cred.get('aws_secret_access_key', None)

_BUCKET =  config.get('BUCKET')
_REGION_NAME_S3 = config.get('REGION_NAME_S3', 'us-west-1')

boto_sess = Session(region_name='us-west-1')
if _S3API:
    boto_sess = Session(
        region_name='us-west-1',
        aws_access_key_id=_S3API,
        aws_secret_access_key=_S3SECRET
    )