#!/usr/bin/env python
"""
Client which receives and processes the requests
"""
import os
import logging
import argparse
import urllib2
import boto3
from flask import Flask, request
from boto3.dynamodb.conditions import Key

# configure logging
logging.basicConfig(level=logging.INFO)

# environment vars
API_TOKEN = os.getenv("GD_API_TOKEN")
if API_TOKEN is None:
    raise Exception("Must define GD_API_TOKEN environment variable")
API_BASE = os.getenv("GD_API_BASE")
if API_BASE is None:
    raise Exception("Must define GD_API_BASE environment variable")
DYNAMO_TABLE = os.getenv("GD_DYNAMO_TABLE")
if DYNAMO_TABLE is None:
    raise Exception("Must define GD_DYNAMO_TABLE environment variable")

app = Flask(__name__)

dynamodb = boto3.resource('dynamodb', region_name = 'us-west-2')
table = dynamodb.Table(DYNAMO_TABLE)

# creating flask route for type argument
@app.route('/', methods=['GET', 'POST'])
def main_handler():
    """
    main routing for requests
    """
    if request.method == 'POST':
        return process_message(request.get_json())
    else:
        return get_message_stats()

def get_message_stats():
    """
    provides a status that players can check
    """
    # use DescribeTable to get number of items in DynamoDB table rather than
    # Scan as a Scan is very expensive and wille exhaust read capacity
    estimated_count = table.item_count
    return "There are ~{} messages in the DynamoDB table".format(estimated_count)

def process_message(msg):
    """
    processes the messages by combining parts
    """
    msg_id = msg['Id'] # The unique ID for this message
    msg_total = msg['TotalParts']
    part_number = msg['PartNumber'] # Which part of the message it is
    data = msg['Data'] # The data of the message

    # log
    logging.info("Processing message for msg_id={} with part_number={} and msg_total={} and data={}".format(msg_id, part_number, msg_total, data))

    try:
    	# store this part of the message in the dynamodb table
    	table.put_item(
       		Item={
       		    'messageid': msg_id,
        	    'part_number': part_number,
				'data': data
			},
			ConditionExpression='attribute_not_exists(messageid, part_number)')
	except ConditionExpression:
		logging.warning("Duplicate for messageid={}, part_number={}".format(msg_id, part_number))

    # try to get the parts of the message from the dynamodb table
    db_messages = table.query(KeyConditionExpression=Key('messageid').eq(msg_id))

    # if we have both parts, the message is complete
    if db_messages["Count"] == msg_total:
        # app.logger.debug("got a complete message for %s" % msg_id)
        logging.info("Have all parts for messageid={}".format(msg_id))
        # We can build the final message.
	result = ""
	for i in range(len(db_messages["Items"])):
        	result = result + db_messages["Items"][i]["data"]
        logging.debug("Assembled message: {}".format(result))
        # sending the response to the score calculator
        # format:
        #   url -> api_base/jFgwN4GvTB1D2QiQsQ8GHwQUbbIJBS6r7ko9RVthXCJqAiobMsLRmsuwZRQTlOEW
        #   headers -> x-gameday-token = API_token
        #   data -> EaXA2G8cVTj1LGuRgv8ZhaGMLpJN2IKBwC5eYzAPNlJwkN4Qu1DIaI3H1zyUdf1H5NITR
        url = API_BASE + '/' + msg_id
        logging.debug("Making request to {} with payload {}".format(url, result))
        req = urllib2.Request(url, data=result, headers={'x-gameday-token':API_TOKEN})
        resp = urllib2.urlopen(req)
        logging.debug("Response from server: {}".format(resp.read()))
        resp.close()

    return 'OK'

if __name__ == "__main__":
    # By default, we disable threading for "debugging" purposes.
    app.run(host="0.0.0.0", port="80", threaded=True)
                    