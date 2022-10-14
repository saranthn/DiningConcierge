import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def get_slot(intent_request, slotName):
    slots = intent_request['sessionState']['intent']['slots']
    if slots is not None and slotName in slots and slots[slotName] is not None:
        return slots[slotName]['value']['originalValue']
    else:
        return None 

def get_resolved_slot(intent_request, slotName):
    slots = intent_request['sessionState']['intent']['slots']
    if slots is not None and slotName in slots and slots[slotName] is not None:
        return slots[slotName]['value']['resolvedValues'][0]
    else:
        return None 

def elicit_slot(session_attributes, intent_name, intent, slot_to_elicit, message):
    intent['state'] = 'InProgress'
    return {
        'sessionState': {
            'dialogAction': {
                "slotToElicit": slot_to_elicit,
                'type': 'ElicitSlot',
            },
            'intent': intent,
            'sessionAttributes': session_attributes
        },
        'messages': [ message ] if message != None else None
    }
    
def delegate(session_attributes, intent):
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'Delegate',
            },
            'intent': intent,
            'sessionAttributes': session_attributes
        }
    }

def greeting(intent_request):
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'ElicitIntent'
            }
        },
        "messages": [
        {
            "contentType": "PlainText",
            "content": "Hi, How can I help you??",
        }
        ],
    }
    
def thankyou(intent_request):
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'ElicitIntent'
            }
        },
        "messages": [
        {
            "contentType": "PlainText",
            "content": "Thank you, have a good day!!"
        }
        ],
    }
    
def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')   
        
def isvalid_date(date):
    try:
        #dateutil.parser.parse(date)
        a = datetime.datetime.strptime(date, '%Y-%m-%d').date()
        return True
    except ValueError:
        return False
    
def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }
    
# Validates the slot values
def validate_dining_details(location, cuisine, dining_date, dining_time, number_of_people, phone_number, email):
    
    available_location = ['manhattan']
    if location is not None and location.lower() not in available_location:
        return build_validation_result(False,
                                       'Location',
                                       'We do not have suggestions for {}, would you like suggestions for Manhattan'.format(location))
    
    available_cuisine = ["indian", "italian", "chinese", "mexican", "thai", "japanese"]
    if cuisine is not None and cuisine.lower() not in available_cuisine:
        return build_validation_result(False,
                                       'Cuisine',
                                       'We do not have suggestions for {} cuisine, Please choose one of the following Cuisines: Indian, Italian, Chinese, Mexican, Thai, Japanese'.format(cuisine))
    
    if dining_date is not None:
        if not isvalid_date(dining_date):
            return build_validation_result(False, 'DiningDate', 'I did not understand that, what date would you like to book a reservation? Please enter in this format: yyyy-mm-dd')
        elif datetime.datetime.strptime(dining_date, '%Y-%m-%d').date() <= datetime.date.today():
            return build_validation_result(False, 'DiningDate', 'You can book restaurant from today onwards.  What day would you like to pick them up?')

    
    if dining_time is not None:
        if len(dining_time) != 5:
            return build_validation_result(False, 'DiningTime', 'Invalid Time, Please try again')

        hour, minute = dining_time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            return build_validation_result(False, 'DiningTime', 'Invalid Time, Please try again')

        if hour < 8 or hour > 22:
            return build_validation_result(False, 'DiningTime', 'Hotel business hours are from 8 AM. to 10 PM. Can you specify a time during this range?')
            
    if phone_number is not None:
        if len(phone_number) != 10:
            return build_validation_result(False, 'PhoneNo', 'Invalid Phone number. Please enter your phone number again')
    
    return build_validation_result(True, None, None)
    
def send_message(location, cuisine, dining_date, dining_time, number_of_people, phone_number, email):
    # Create SQS client
    sqs = boto3.client('sqs')
    
    queue_url = 'https://sqs.us-east-1.amazonaws.com/704995242206/DiningChatQueue'
    
    # Send message to SQS queue
    response = sqs.send_message(
        QueueUrl=queue_url,
        DelaySeconds=10,
        MessageAttributes={
            'Location': {
                'DataType': 'String',
                'StringValue': location
            },
            'Cuisine': {
                'DataType': 'String',
                'StringValue': cuisine
            },
            'DiningDate': {
                'DataType': 'String',
                'StringValue': dining_date
            },
            'DiningTime': {
                'DataType': 'String',
                'StringValue': dining_time
            },
            'NumberOfPeople': {
                'DataType': 'String',
                'StringValue': number_of_people
            },
            'PhoneNo': {
                'DataType': 'String',
                'StringValue': phone_number
            },
            'Email': {
                'DataType': 'String',
                'StringValue': email
            }
        },
        MessageBody=(
            'Dining suggestion slot values'
        )
    )
    
    print(response['MessageId'])
    
# Not used here
def save_previous_userdata(email, location, cuisine):
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
    
def dining_suggestions(intent_request):

    # Get all current slot values
    intent_name = intent_request['sessionState']['intent']['name']
    slots = intent_request['sessionState']['intent']['slots']
    location = get_slot(intent_request, 'Location')
    cuisine = get_slot(intent_request, 'Cuisine')
    dining_date = get_slot(intent_request, 'DiningDate')
    dining_time =  get_resolved_slot(intent_request, 'DiningTime')
    number_of_people = get_slot(intent_request, 'NumberOfPeople')
    phone_number = get_slot(intent_request, 'PhoneNo')
    email = get_slot(intent_request, 'Email')
    source = intent_request['invocationSource']
    
    print(location, cuisine, dining_date, dining_time, number_of_people, phone_number, email)

    if source == 'DialogCodeHook':

        validation_result = validate_dining_details(location, cuisine, dining_date, dining_time, number_of_people, phone_number, email)
        if not validation_result['isValid']:
            if slots is not None and validation_result['violatedSlot'] in slots and slots[validation_result['violatedSlot']] is not None:
                slots[validation_result['violatedSlot']]['value']['resolvedValues'] = None
            
            return elicit_slot(intent_request['sessionState']['sessionAttributes'],
                              intent_name,
                              intent_request['sessionState']['intent'],
                              validation_result['violatedSlot'],
                              validation_result['message'])

        return delegate(intent_request['sessionState']['sessionAttributes'], intent_request['sessionState']['intent'])
    
    # Send message to SQS
    send_message(location, cuisine, dining_date, dining_time, number_of_people, phone_number, email)
    
    
    intent_request['sessionState']['intent']['state'] = "Fulfilled"
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'Close'
            },
            'intent': intent_request['sessionState']['intent']
        },
        "messages": [
        {
            "contentType": "PlainText",
            "content": 'Thanks, your suggestions for city:{}, cuisine:{}, date: {}, time:{}, number of persons:{}, phone number:{}, email: {} will be sent in Email'.format(location, cuisine, dining_date, dining_time, number_of_people, phone_number, email)
        }
        ],
    }

def dispatch(intent_request):

    intent_name = intent_request['sessionState']['intent']['name']

    if intent_name == 'GreetingIntent':
        return greeting(intent_request)
    elif intent_name == 'ThankYouIntent':
        return thankyou(intent_request)
    elif intent_name == 'DiningSuggestionsIntent':
        return dining_suggestions(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')

def lambda_handler(event, context):
    print(event)
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)
