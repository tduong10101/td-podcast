import boto3
from boto3.dynamodb.conditions import Attr
import requests
import os
import logging

logging.basicConfig(level=logging.DEBUG)


def update_episode(event, context):
    def playerfm(list_url, station_name):
        temp_list = list()
        list_request = requests.get(list_url)
        list_content = list_request.json()
        new_ep_request = list_content['episodes']
        for new_ep in new_ep_request:
            temp_list.append({
                "station": station_name,
                "episode_url": new_ep['url'],
                "title": new_ep['title'],
                "duration": new_ep['duration'],
                "description": new_ep['description'],
                "home_url": new_ep['home']
            })
        return temp_list
    # get tables
    dynamodb = boto3.resource('dynamodb', os.environ['REGION'])
    episode_table = dynamodb.Table(os.environ['EPISODE_TABLE'])
    station_table = dynamodb.Table(os.environ['STATIONS_TABLE'])
    # scan tables
    stations = station_table.scan(
        FilterExpression=Attr('list_url').exists()
    )
    old_ep_list = episode_table.scan(
        ProjectionExpression="episode_url"
    )['Items']
    new_ep_list = list()
    # for each stations request get 50 most recent episodes
    for station in stations['Items']:
        spider_name = station['spider']
        new_ep_list.extend(locals()[spider_name](station['list_url'],
                                                 station['name']))
    # compare 2 lists, only compare the episode_url value
    logging.debug(new_ep_list)
    add_ep_list = list()
    logging.debug(old_ep_list)
    for new_ep in new_ep_list:
        if not any(i['episode_url'] == new_ep['episode_url']
                   for i in old_ep_list):
            add_ep_list.append(new_ep)
    logging.debug(add_ep_list)
    with episode_table.batch_writer() as batch:
        # add new episodes
        for put_ep in add_ep_list:
            batch.put_item(Item=put_ep)
    return 0


# os.environ['REGION'] = "us-east-1"
# os.environ['EPISODE_TABLE'] = "td-podcast-eps"
# os.environ['STATIONS_TABLE'] = "podcast_resources"
# update_episode("test", "test")
