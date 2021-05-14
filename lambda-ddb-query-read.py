import json
import csv
import boto3
from boto3.dynamodb.conditions import Key

def lambda_handler(event, context):
    region = 'ap-southeast-2'
    record_list = []
    table_name = "salary"
    primary_key = 'actor_id'
    
    try:
        dynamodb = boto3.resource('dynamodb', region_name= region)
        table = dynamodb.Table(table_name)
        resp = table.query(KeyConditionExpression=Key('actor_id').eq(1))

        print("The query returned the following items:")
        for item in resp['Items']:
            print('Actor ID: ', item["actor_id"], 'Firstname: ', item["firstname"], ' Surname: ', item["surname"] , ' Salary: ', item["salary"])
        
    except Exception as e:
        print (str(e))
        
    return {
        'statusCode': 200,
        'body': json.dumps('CSV to DDB Success')
    }

