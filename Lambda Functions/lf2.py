import boto3
import json
import random
import datetime
from requests_aws4auth import AWS4Auth
import requests
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

def get_message():
    # Create SQS client
    sqs = boto3.client('sqs')
    
    queue_url = 'https://sqs.us-east-1.amazonaws.com/704995242206/DiningChatQueue'
    
    # Get message from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['SentTimestamp'],
        MaxNumberOfMessages=5,
        MessageAttributeNames=['All'],
        VisibilityTimeout=10,
        WaitTimeSeconds=0
        )
        
    return response
    

# Gets Business Id's of restaurants of given input cuisine
def get_restaurantId_opensearch(cuisine):
    
    region = 'us-east-1'
    service = 'es'

    # Removed KEY
    credentials = boto3.Session(aws_access_key_id="", aws_secret_access_key="", region_name="us-east-1").get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    host = 'search-restaurants-hp47f7avhb2tdj25kwg3wdxpkq.us-east-1.es.amazonaws.com'
    
    index = 'restaurants'
    url = 'https://' + host + '/' + index + '/_search'
 
    query = {
        "size": 1300,
        "query": {
            "query_string": {
                "default_field": "cuisine",
                "query": cuisine
            }
        }
    }
    headers = { "Content-Type": "application/json" }
    response = requests.get(url,auth=awsauth, headers=headers, data=json.dumps(query))
    res = response.json()
    
    noOfHits = res['hits']['total']
    hits = res['hits']['hits']
    
    businessIds = []
    for hit in hits:
        businessIds.append(str(hit['_source']['Business ID']))
    return businessIds
    

# Get Restaurants from DynamoDB based on the Business Id's
def get_restaurant_dynamoDB(restaurantIds):
    res = []
    client = boto3.resource('dynamodb')
    table = client.Table('yelp-restaurants')
    for id in restaurantIds:
        response = table.get_item(Key={'Business ID': id})
        res.append(response)
    return res
    
    
def generate_msg_details(restaurantDetails, message):
    
    noOfPeople = message['MessageAttributes']['NumberOfPeople']['StringValue']
    date = message['MessageAttributes']['DiningDate']['StringValue']
    time = message['MessageAttributes']['DiningTime']['StringValue']
    cuisine = message['MessageAttributes']['Cuisine']['StringValue']
    
    comma = ', '
    restaurant_one_name = restaurantDetails[0]['Item']['name']
    restaurant_one_add = comma.join(restaurantDetails[0]['Item']['address'])
    restaurant_two_name = restaurantDetails[1]['Item']['name']
    restaurant_two_add = comma.join(restaurantDetails[1]['Item']['address'])
    restaurant_three_name = restaurantDetails[2]['Item']['name']
    restaurant_three_add = comma.join(restaurantDetails[2]['Item']['address'])
    
    msgFinal = '''    <p>Greetings! Here are the {0} restaurant suggestions for {1} people, at {2} on {3} : </p>
        <ul>
          <li>{4}, located at {5}</li>
          <li>{6}, located at {7}</li>
          <li>{8}, located at {9}</li>
        </ul>
        <p>  Enjoy your meal! </p>'''.format(cuisine, noOfPeople, time, date, restaurant_one_name, restaurant_one_add, restaurant_two_name, restaurant_two_add, restaurant_three_name, restaurant_three_add)
    
    return msgFinal
    

# Not used
def sendSMS(msgToSend,phoneNumber):
    client = boto3.client("sns")
    client.publish(PhoneNumber = phoneNumber,Message=msgToSend)
    
def sendEmail(msgToSend, email, subject):
    # Replace sender@example.com with your "From" address.
    # This address must be verified with Amazon SES.
    SENDER = "st3523@columbia.edu"
    
    # Replace recipient@example.com with a "To" address. If your account 
    # is still in the sandbox, this address must be verified.
    RECIPIENT = email
    
    # The subject line for the email.
    SUBJECT = subject
    
    # The email body for recipients with non-HTML email clients.
    BODY_TEXT = msgToSend
                
    # The HTML body of the email.
    BODY_HTML = """<html>
    <head></head>
    <body>
      <h3>Dining Concierge Suggestions</h1>
      <br>
      <hr>
      <p>{}</p>
    </body>
    </html>
                """        
                
    strValue = BODY_HTML.format(msgToSend)
    
    # The character encoding for the email.
    CHARSET = "UTF-8"
    
    # Create a new SES resource and specify a region.
    client = boto3.client('ses')
    
    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': strValue,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])
        
    
def deleteMsg(receipt_handle):
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/704995242206/DiningChatQueue'
    sqs.delete_message(QueueUrl=queue_url,
    ReceiptHandle=receipt_handle
    )
    

# Adds the current search of user in user_preference history table in dynamoDB
def add_current_search(email, cuisine, location):
    dynamodb = boto3.resource('dynamodb')
    #table name
    table = dynamodb.Table('user_preference')
    response = table.put_item(
       Item={
            'userId': email,
            'insertedAtTimestamp': str(datetime.datetime.now()),
            'location': location,
            'cuisine': cuisine
        }
    )
    print(response)
    
# Searches the user preferences history table in DynamoDB to retrieve the previous cuisine and location searched
def query_dynamodb_userHistory(email):
    res = []
    client = boto3.resource('dynamodb')
    table = client.Table('user_preference')
    response = table.query(
        KeyConditionExpression=Key('userId').eq(email)
    )
    return response
    
def handle_previous_search(email):
    response = query_dynamodb_userHistory(email)
    print("retrieve from db: ", response)
    location = ''
    cuisine = ''
    if response['Count'] != 0:
        location = response['Items'][-1]['location']
        cuisine = response['Items'][-1]['cuisine']
        
    return [location, cuisine]
    
def lambda_handler(event, context):
    
    # getting response from sqs queue
    sqsQueueResponse = get_message()

    if "Messages" in sqsQueueResponse.keys():
        for message in sqsQueueResponse['Messages']:
            cuisine = message['MessageAttributes']['Cuisine']['StringValue']
            restaurantIds = get_restaurantId_opensearch(cuisine)
            
            restaurantIds = random.sample(restaurantIds, 3)
            print(restaurantIds)
            restaurantDetails = get_restaurant_dynamoDB(restaurantIds)
            
            # Search for email in 
            
            msgToSend = generate_msg_details(restaurantDetails,message)
            print("Message: ", msgToSend)
            
            phoneNumber = message['MessageAttributes']['PhoneNo']['StringValue']
            if "+1" not in phoneNumber:
                phoneNumber  = '+1'+phoneNumber
            #sendSMS(msgToSend,phoneNumber)
            #now delete message from queue
            receipt_handle = message['ReceiptHandle']
            email = message['MessageAttributes']['Email']['StringValue']
            
            prev_data = handle_previous_search(email)
            pre_location = prev_data[0]
            pre_cuisine = prev_data[1]
            add_current_search(email, cuisine, message['MessageAttributes']['Location']['StringValue'])
            
            if len(pre_cuisine) != 0 and len(pre_location) != 0:
                pre_restaurantIds = get_restaurantId_opensearch(pre_cuisine)
                
                pre_restaurantIds = random.sample(pre_restaurantIds, 3)
                print(pre_restaurantIds)
                pre_restaurantDetails = get_restaurant_dynamoDB(pre_restaurantIds)
                
                # Search for email in 
                message['MessageAttributes']['Cuisine']['StringValue'] = pre_cuisine
                message['MessageAttributes']['Location']['StringValue'] = pre_location
                
                pre_msgToSend = generate_msg_details(pre_restaurantDetails,message)
                subject = "Your Dining Concierge Suggestions Based on Previous Search"
                sendEmail(pre_msgToSend, email, subject)
            
            subject = "Your Dining Concierge Suggestions for Your Search"
            
            sendEmail(msgToSend, email, subject)
            deleteMsg(receipt_handle)
