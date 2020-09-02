#!/usr/bin/env python

from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition
import json
import sys
import distutils.dir_util
import click
import os

@click.command()
@click.argument('server')
@click.argument('topic')
@click.option('--part', default=0)
@click.option('--port', default=9092)
def main(server, topic, part, port):
    server_port = "{}:{}".format(server,port)
    consumer = KafkaConsumer(bootstrap_servers=[server_port],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    consumer.assign([TopicPartition(topic, part)])
    events = list()
    distutils.dir_util.mkpath(os.path.join("data", server)
    try:
        for event in consumer:
            events.append({
                "topic": event.topic,
                "partition": event.partition,
                "offset": event.offset,
                "key": event.key,
                "value": event.value,
                "headers": {str(k):str(v) for k,v in event.headers},
                "checksum": event.checksum
            })
            if event.offset % 100 == 0:
                print(event.offset)
    except KeyboardInterrupt:
        print("Saving Data")
        with open('{}/{}_{}.json'.format(server,topic, part), 'w') as _file:
            json.dump(events, _file, indent=4)

if __name__ == "__main__":
    main()