#!/usr/bin/env python

import unittest
import os
import yaml
from mock import Mock, patch
import sqs_op
import boto.sqs

class SqsOpTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('boto.sqs')
    def test_get_bounce(self, boto_sqs):
        source_address = 'test@company.com'
        fake_bounce_body = {}
        message = Mock()
        message.get_body = Mock(return_value=fake_bounce_body)
        messages = [message]
        queue = Mock()
        queue.get_messages = Mock(side_effect=[messages, []])
        conn = Mock()
        conn.get_queue = Mock(return_value=queue)
        boto_sqs.connect_to_region = Mock(return_value=conn)
        dests = sqs_op.bounce_queue.get_dests(source_address)

    @patch('boto.sqs')
    def test_get_complaint(self, boto_sqs):
        source_address = 'test@company.com'
        fake_bounce_body = {}
        message = Mock()
        message.get_body = Mock(return_value=fake_bounce_body)
        messages = [message]
        queue = Mock()
        queue.get_messages = Mock(side_effect=[messages, []])
        conn = Mock()
        conn.get_queue = Mock(return_value=queue)
        boto_sqs.connect_to_region = Mock(return_value=conn)
        dests = sqs_op.complaint_queue.get_dests(source_address)
