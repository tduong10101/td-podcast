import boto3
from boto3.dynamodb.conditions import Attr
import os
import logging

logging.basicConfig(level=logging.DEBUG)


def get_current_episode(event, context):
    dynamodb = boto3.resource('dynamodb', os.environ['REGION'])
    episode_table = dynamodb.Table(os.environ['EPISODE_TABLE'])
    current_episode = episode_table.scan(
        FilterExpression=Attr('status').eq('current')
    )['Items'][0]
    logging.debug([current_episode])
    return current_episode
