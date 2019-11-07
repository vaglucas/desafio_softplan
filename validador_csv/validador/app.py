import json
from datetime import datetime
from io import StringIO
import boto3
import numpy as np
import pandas as pd
import sys
from extraction import InitDataFrame, getErrorDesc
import csv

def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
        #api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    try:
        s3 = boto3.client('s3')
        if event:
            for record in event['Records']:
                bucket_name = str(record['s3']['bucket']['name'])
                file_name = str(record['s3']['object']['key'])
                size = str(record['s3']['object']['size'])
                fileObj = s3.get_object(Bucket=bucket_name, Key=file_name)
                file_content = fileObj["Body"].read()
                sep_file = get_sep(file_content.decode())
                try:
                    columns = []
                    info = []
                    df = pd.read_csv(StringIO(file_content.decode()), sep=sep_file)
                    ex = InitDataFrame(df)
                    ex._normalize_columns_name()
                    result_save = ex.data_frame_analytics(0.002, file_name)
                    tag = {'TagSet': [{'Key': 'validated', 'Value': 'True'}]}
                    response = s3.put_object_tagging(Bucket=bucket_name, Key=file_name, Tagging=tag)                    
                except Exception as e:
                    tag = {'TagSet': [{'Key': 'validated', 'Value': 'False'}]}
                    response = s3.put_object_tagging(Bucket=bucket_name, Key=file_name, Tagging=tag)
                    return {"statusCode": 500,"body": json.dumps({"message": "Error {}".format(getErrorDesc(sys.exc_info()))}),}
        print(result_save)
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "File Upload {} row inserted {}  ".format(file_name, 0)}),
            }
    except Exception as e:
        print(e)
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Error {}".format(getErrorDesc(sys.exc_info()))}),
            }


def get_sep(file_content):
    try:
        dialect = csv.Sniffer().sniff(file_content)
        return dialect.delimiter  
    except Exception as e :
        print(e.args)
        return ','