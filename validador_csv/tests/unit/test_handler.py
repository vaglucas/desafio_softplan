import json
import pytest
from load_data import app


@pytest.fixture()
def apigw_event():
    """ Generates API GW Event"""

    return {
          "Records": [
            {
              "eventVersion": "2.0",
              "eventSource": "aws:s3",
              "awsRegion": "{region}",
              "eventTime": "1970-01-01T00:00:00Z",
              "eventName": "ObjectCreated:Put",
              "userIdentity": {
                "principalId": "EXAMPLE"
              },
              "requestParameters": {
                "sourceIPAddress": "127.0.0.1"
              },
              "responseElements": {
                "x-amz-request-id": "EXAMPLE123456789",
                "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
              },
              "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "testConfigRule",
                "bucket": {
                  "name": "dados-portal-transparencia",
                  "ownerIdentity": {
                    "principalId": "EXAMPLE"
                  },
                  "arn": "arn:{partition}:s3:::softplan-s3"
                },
                "object": {
                  "key": "total.csv",#nome do arquivo no S3 da amazon
                  "size": 4048,
                  "eTag": "f4993f407092a8d53663e51654e93410",#tag do arquivo
                  "sequencer": "0A1B2C3D4E5F678901"
                }
              }
            }
          ]
        }


def test_lambda_handler(apigw_event):

    ret = app.lambda_handler(apigw_event, "")
    data = json.loads(ret["body"])
    assert ret["statusCode"] == 200
    # assert "location" in data.dict_keys()
