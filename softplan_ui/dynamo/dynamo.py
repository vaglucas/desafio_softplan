import decimal
import json
import numpy as np
import boto3
from boto3.dynamodb.conditions import Key
from boto3.session import Session
from credentials import _REGION_NAME_S3, _BUCKET, boto_sess



class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        if isinstance(o, np.integer):
            return int(o)
        elif isinstance(o, np.floating):
            return float(o)
        return super(DecimalEncoder, self).default(o)

class Dynamo():

    def __init__(self, table_name):
        self.client = boto3.client('dynamodb', region_name='eu-west-1')
        self.resurce = boto3.resource('dynamodb', region_name='eu-west-1')
        self.table_name = table_name
        self.db = boto_sess.resource('dynamodb', region_name='eu-west-1')
        self.creating = False
        self.get_table()

    def get_table(self):
        self.table = self.db.Table(self.table_name)

    def save(self, data):
        try:
            self.table.put_item(
                Item=data
                )
        except Exception as e:
            print(e)

    def get_info(self, id):
        try:
            response = self.table.query(KeyConditionExpression=Key('id').eq(id))
            return json.loads(json.dumps(response['Items'][0], cls=DecimalEncoder))
        except Exception as e:
            print(e)
            return None
        
    def scan(self):
        try:
            response = self.table.scan()
            return json.loads(json.dumps(response['Items'], cls=DecimalEncoder))
        except Exception as e:
            print(e)
            return None
        

