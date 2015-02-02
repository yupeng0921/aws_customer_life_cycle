#! /usr/bin/env python

# this script is only used for clean test data
# be careful to use it!

import os
import os.path
import yaml
import boto
from boto.dynamodb2.table import Table

with open('%s/conf.yaml' % os.path.split(os.path.realpath(__file__))[0], 'r') as f:
    conf = yaml.safe_load(f)

metadata_db_name = conf['metadata_db_name']
region = conf['region']

conn = boto.dynamodb2.connect_to_region(region)
metadata_table = Table(metadata_db_name, connection=conn)
metadatas = metadata_table.scan()

for metadata in metadatas:
    metadata.delete()
