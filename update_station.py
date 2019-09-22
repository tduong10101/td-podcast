from bs4 import BeautifulSoup
import boto3
from boto3 import client as boto3_client
from boto3.dynamodb.conditions import Attr
import requests
import re
import os
import logging
import json

logging.basicConfig(level=logging.DEBUG)


def update_station(event, context):
    def playerfm(url):
        crawl_url = requests.get(url)
        soup = BeautifulSoup(crawl_url.content, 'html.parser')
        section = soup.find("section", {
            "class": re.compile(r"series-episodes-list")})
        list_url = section['data-url']
        list_url = re.sub("&at=\\d+$", "", list_url)
        return list_url
    dynamodb = boto3.resource('dynamodb', os.environ['REGION'])
    table = dynamodb.Table(os.environ['STATIONS_TABLE'])
    stations = table.scan(
        FilterExpression=Attr('list_url').not_exists()
    )
    for station in stations['Items']:
        if 'list_url' not in station:
            spider_name = station['spider']
            list_url = locals()[spider_name](station['url'])
            station['list_url'] = list_url
            table.put_item(Item=station)
    msg = {"key": "new_invocation"}
    lambda_client = boto3_client('lambda')
    invoke_response = lambda_client.invoke(
                                    FunctionName="td-podcast-update-episode",
                                    InvocationType='Event',
                                    Payload=json.dumps(msg))
    return 0
