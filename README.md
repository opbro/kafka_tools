# tools


## Export Index from a Elasticsearch cluster

I use this to pull data for offline use on an elastic search cluster
```
$ ./export_index.py --help
Usage: export_index.py [OPTIONS] SERVER INDEX

Options:
  --scheme TEXT
  --port INTEGER
  --username TEXT
  --password TEXT
  --help           Show this message and exit.
```

## Export Topic from a Kafka Cluster

```
$ ./export_topic.py  --help
Usage: export_topic.py [OPTIONS] SERVER TOPIC

Options:
  --part INTEGER
  --port INTEGER
  --help          Show this message and exit.
```
