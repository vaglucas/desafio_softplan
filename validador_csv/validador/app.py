import json
import boto3
import unicodedata
from datetime import datetime
import re
import dynamo as db
import decimal

def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    try:
        s3 = boto3.client('s3',aws_access_key_id='AKIAIRJRT2UDAEZXYWJA',aws_secret_access_key='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')
        if event:
            for record in event['Records']:
                bucket_name = str(record['s3']['bucket']['name'])
                file_name = str(record['s3']['object']['key'])
                size = str(record['s3']['object']['size'])
                fileObj = s3.get_object(Bucket=bucket_name, Key=file_name)
                file_content = fileObj["Body"].read().splitlines()
                rows = 0
                g = db.GovernamentalExpenses()
                try:
                    if float(size)<1048576:#1024*1024
                        columns = []
                        info = []
                        
                        line = line.decode('utf-8-sig')
                        columns = line.strip().replace('"','').split(';')
                        columns = normalize_columns_name(columns[:len(columns)-1])
                    
                        g.save({'id':file_name,'date':str(datetime.now()), 'file_data':info, })
                        tag = {'TagSet':
                                   [{'Key': 'processed', 'Value': 'True'}]
                               }
                        response = s3.put_object_tagging(Bucket=bucket_name, Key=file_name,Tagging=tag)
                        
                except Exception as e:
                    tag = {'TagSet': [{'Key': 'processed', 'Value': 'False'}]}
                    response = s3.put_object_tagging(Bucket=bucket_name, Key=file_name, Tagging=tag)
                    print(e)
        return {
                "statusCode": 200,
                "body": json.dumps({"message": "File Upload {} row inserted {}  ".format(file_name,rows)}),
             }
    except Exception as e:
        print(e)
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Error "}),
        }

def normalize_columns_name(columns):
    return [unicodedata.normalize('NFD', x).encode('ascii', 'ignore').decode('utf-8').replace(')','').replace('(','').replace(' ','_').replace('/','').lower() for x in columns]

def normalize_line(line):
    return [to_number(x) if x is not '' else '-' for x in line.strip().replace('"','').split(';')]


def to_number(x):
    try:
        _x = x.strip().replace('.','').replace(',','.')
        if _x.split('.')[0].isnumeric() and '.' in _x:
            return decimal.Decimal(re.sub(r'[^\d.]', '', _x))
        if x.startswith('R$'):
            _x = x.strip().replace('.', '').replace(',', '.')
            return decimal.Decimal(re.sub(r'[^\d.]', '', _x))
        return x
    except:
        return x
