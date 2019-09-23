import boto3
from boto3.dynamodb.conditions import Attr, Key
import os
import logging
import random

logging.basicConfig(level=logging.DEBUG)


def gen_random_episode(event, context):
    # get tables
    dynamodb = boto3.resource('dynamodb', os.environ['REGION'])
    episode_table = dynamodb.Table(os.environ['EPISODE_TABLE'])
    # scan tables
    episode_pool = episode_table.scan(
        FilterExpression=Attr('status').not_exists(),
        ProjectionExpression="episode_url"
    )['Items']
    current_episode = episode_table.scan(
        FilterExpression=Attr('status').contains('current')
    )['Items']
    logging.debug(len(episode_pool))
    logging.debug(len(current_episode))
    random_number = random.randrange(0, len(episode_pool)-1)
    if len(current_episode) != 0:
        for e in current_episode:
            e['status'] = "completed"
            episode_table.put_item(Item=e)
    random_id = episode_pool[random_number]['episode_url']
    random_episode = episode_table.query(
        KeyConditionExpression=Key('episode_url').eq(random_id)
    )['Items'][0]
    random_episode['status'] = "current"
    episode_table.put_item(Item=random_episode)
    logging.debug(random_number)
    logging.debug(random_episode)
    return 0
