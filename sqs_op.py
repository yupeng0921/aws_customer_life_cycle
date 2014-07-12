#! /usr/bin/env python

import os
import yaml
import json
import logging
import types
import boto.sqs

conf_file_path = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'conf.yaml')
with open('%s/conf.yaml' % os.path.split(os.path.realpath(__file__))[0], 'r') as f:
    conf = yaml.safe_load(f)

region = conf['region']
complaint_queue_name = conf['complaint_queue_name']
max_complaint_count = conf['max_complaint_count']
bounce_queue_name = conf['bounce_queue_name']
max_bounce_count = conf['max_bounce_count']

def Queue():
    used = []
    def __init__(self, queue_name, region, max_count, num_messages=10):
        conn = boto.sqs.connect_to_region(region)
        queue = conn.get_queue(queue_name)
        queue.set_message_class(boto.sqs.message.MHMessage)
        self.queue = queue
        self.max_count = max_count
        self.num_messages = num_messages
    def get_dests(source_address):
        ret_list = []
        unused_list = []
        rs = self.queue.get_messages(num_messages=self.num_messages)
        while rs and len(ret_list) < self.max_count:
            for message in rs:
                body = message.get_body()
                if type(body) is not types.DictType:
                    logging.warning('invalid message body: %s' % body)
                    unused_list.append((message, 0))
                    continue
                try:
                    source, dests = self._exact_addr_from_body(body)
                except Exception, e:
                    logging.warning('parse body failed %s %s' % (body, unicode(e)))
                    unused_list.append((message, 0))
                    continue
                if source == source_address:
                    for diest in dests:
                        ret_list.append(dest)
                    self.used.append(message)
                else:
                    unused_list.append((message, 0))
            rs = self.queue.get_messages(num_messages=self.num_messages)
        if unused_list:
            self.queue.change_message_visibility_batch(unused_list)
        return ret_list
    def _exact_addr_from_body(self, body):
        raise Exception('child should overwrite this method')
    def delete_message(self):
        if self.used:
            self.queue.delete_message_batch(self.used)

class ComplaintQueue(Queue):
    def _exact_addr_from_body(self, body):
        message_content=body['"Message"'][:-1]
        m1 = json.loads(message_content)
        m2 = json.loads(m1)
        source = m2['mail']['source']
        dests = []
        for item in m2['complaint']['complainedRecipients']:
            dests.append(item['emailAddress'])
        return (source, dests)

class BounceQueue(Queue):
    def _exact_addr_from_body(self, body):
        def _exact_addr_from_body(self, body):
        message_content=body['"Message"'][:-1]
        m1 = json.loads(message_content)
        m2 = json.loads(m1)
        source = m2['mail']['source']
        dests = []
        for item in m2['bounce']['bounceRecipients']:
            dests.append(item['emailAddress'])
        return (source, dests)

complaint_queue = ComplaintQueue(complaint_queue_name, region, max_complaint_count)
bounce_queue = BounceQueue(bounce_queue_name, region, max_bounce_count)
