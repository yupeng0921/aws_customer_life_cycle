#! /usr/bin/env python
# -*- coding: UTF-8 -*-

import os
import yaml
import json
import logging
import types
import boto.sqs

conf_file_path = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'conf.yaml')
with open('%s/conf.yaml' % os.path.split(os.path.realpath(__file__))[0], 'r') as f:
    conf = yaml.safe_load(f)

# region = conf['region']
region = 'us-west-2'
complaint_queue_name = conf['complaint_queue_name']
max_complaint_count = conf['max_complaint_count']

complaint_list = []

conn = boto.sqs.connect_to_region("us-west-2")
complaint_queue = conn.get_queue(complaint_queue_name)
complaint_queue.set_message_class(boto.sqs.message.MHMessage)

def exact_addr_from_body(body):
    message_content=body['"Message"'][:-1]
    m1 = json.loads(message_content)
    m2 = json.loads(m1)
    source = m2['mail']['source']
    dests = []
    for item in m2['complaint']['complainedRecipients']:
        dests.append(item['emailAddress'])
    return (source, dests)

def get_complaint(source_address):
    ret_list = []
    unused_list = []
    rs = complaint_queue.get_messages(num_messages=10)
    while rs and len(ret_list) < max_complaint_count:
        for message in rs:
            body = message.get_body()
            if type(body) is not types.DictType:
                logging.warning('invalid message body: %s' % body)
                unused_list.append((message, 0))
                continue
            try:
                source, dests = exact_addr_from_body(body)
            except Exception, e:
                logging.warning('parse body failed %s %s ' % (body, unicode(e)))
                unused_list.append((message, 0))
                continue
            if source == source_address:
                for dest in dests:
                    ret_list.append(dest)
                complaint_list.append(message)
            else:
                unused_list.append((message, 0))
        rs = complaint_queue.get_messages(num_messages=10)
    if unused_list:
        complaint_queue.change_message_visibility_batch(unused_list)
    return ret_list

def delete_complaint_messages():
    complaint_queue.delete_message_batch(complaint_list)

if __name__ == '__main__':
    dests = get_complaint('penyu@amazon.com')
    print(dests)
    delete_complaint_messages()
