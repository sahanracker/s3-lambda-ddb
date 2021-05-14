import json
import csv
import boto3

def lambda_handler(event, context):
    region = 'ap-southeast-2'
    record_list = []
    
    try:
        s3 = boto3.client('s3')
        
        dynamodb = boto3.client('dynamodb', region_name = region)
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        print('Bucket: ', bucket, 'Key:' ,key)
        
        csv_file = s3.get_object(Bucket = bucket, Key = key)
        
        record_list = csv_file['Body'].read().decode('utf-8').split('\n')
        
        csv_reader = csv.reader(record_list, delimiter=',', quotechar='"')
        #print(len(csv_reader))
        #print('Number of rows :', count)
        for row in csv_reader:
            print('Number of rows :', len(row))
            actor_id = row[0]
            firstname = row[1]
            surname= row[2]
            salary = row[3]
        
            print('Actor ID: ', actor_id, 'Firstname: ', firstname, ' Surname: ', surname)
            
            add_to_db = dynamodb.put_item(
                TableName = 'salary',
                Item = {
                    'actor_id' : {'N': str(actor_id)},
                    'firstname' : {'S': str(firstname)},
                    'surname' : {'S': str(surname)},
                    'salary' : {'N': str(salary)},
                })
            print('Successfully add items to DDB Table')
        
        
    except Exception as e:
        print (str(e))
        
    return {
        'statusCode': 200,
        'body': json.dumps('CSV to DDB Success')
    }
