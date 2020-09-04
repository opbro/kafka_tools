#!/usr/bin/env python
from elasticsearch import Elasticsearch
import json
import uuid
import os
import time
import distutils.dir_util
import click


@click.command()
@click.argument('server')
@click.argument('index')
@click.option('--scheme', default='http')
@click.option('--port', default=9200)
@click.option('--username', default=None)
@click.option('--password', default=None)
def main(server, index, scheme, port, username, password):
    if username and password:
        http_auth = (username, password)
    else:
        http_auth = None
    client = Elasticsearch([server], http_auth=http_auth, verify_certs=False,scheme=scheme, port=port)
    index_subname = index.replace('-', '_').replace('*', '')
    distutils.dir_util.mkpath(os.path.join("data", index_subname))
    data = client.search(
        index=index,
        scroll = "2m",
        size = 10000,
        body={}
    )
    sid = data['_scroll_id']
    scroll_size = len(data['hits']['hits'])
    hits = list()
    counter = 0
    total_records = 0
    while scroll_size > 0:
        "Scrolling..."
        # Before scroll, process current batch of hits
        hits = data['hits']['hits']
        save_path = os.path.join("data", index_subname, "hits_{}.json".format(counter))
        with open(save_path, 'w') as _file:
            json.dump(hits,_file)
        total_records += len(hits)
        print("Downloaded {} Records".format(total_records))
        counter += 1
        data = client.scroll(scroll_id=sid, scroll='2m')

        # Update the scroll ID
        sid = data['_scroll_id']

        # Get the number of results that returned in the last scroll
        scroll_size = len(data['hits']['hits'])


if __name__ == "__main__":
    main()