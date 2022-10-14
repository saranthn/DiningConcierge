import json
import boto3

def lambda_handler(event, context):
  
    input = event['messages'][0]['unstructured']['text']
    
    client = boto3.client('lexv2-runtime')
    
    response = client.recognize_text(
        botId='J2XX77MMAR',
        botAliasId='TSTALIASID',
        localeId='en_US',
        sessionId="test_session",

        text=input)
        
    print(response['messages'][0]['content'])
    
    if response['ResponseMetadata']['HTTPStatusCode'] == 200 :
      return {
          'statusCode': 200,
          'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
          },
          "messages": [
            {
            "type": "unstructured",
            "unstructured": {
                "id": "string",
                "text": response['messages'][0]['content'],
                "timestamp": "string"
                }
            }
          ]
      }
    else:
      return {
        'statusCode': 200,
        'body': json.dumps('Timeout! Try Again')
    }



