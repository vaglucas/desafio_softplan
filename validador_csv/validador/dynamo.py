import decimal
import json

import boto3
from boto3.dynamodb.conditions import Key
from boto3.session import Session

boto_sess = Session(
        region_name='eu-west-1',
        )


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class TableCreate():

    def __init__(self, table_name):
        self.client = boto3.client('dynamodb')
        self.resurce = boto3.resource('dynamodb')
        self.table_name = table_name
        self.db = boto_sess.resource('dynamodb', region_name='eu-west-1')
        self.creating = False
        self.create_table()
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

    def create_table(self):

        existing_tables = self.client.list_tables()['TableNames']
        if self.table_name not in existing_tables:
            response = self.resurce.create_table(
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'id',
                            'AttributeType': 'S',
                            }
                        ],
                    KeySchema=[
                        {
                            'AttributeName': 'id',
                            'KeyType': 'HASH',
                            }

                        ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5,
                        },
                    TableName=self.table_name,
                    )
            print(response.table_status)  # CREATING
            response.meta.client.get_waiter('table_exists').wait(TableName=self.table_name)
            self.creating = False
