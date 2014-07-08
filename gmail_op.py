#! /usr/bin/env python

import email
import imaplib
import os
import yaml

with open('%s/conf.yaml' % os.path.split(os.path.realpath(__file__))[0], 'r') as f:
    conf = yaml.safe_load(f)
test_gmail_conf = conf['test_gmail_conf']

with open(test_gmail_conf, 'r') as f:
    conf = yaml.safe_load(f)

username = conf['username']
password = conf['password']
folder = conf['folder']

def get_emails():
    m = imaplib.IMAP4_SSL("imap.gmail.com")
    m.login(username, password)
    m.select(folder)
    resp, items = m.search(None, "ALL")
    items = items[0].split()
    result = []
    for emailid in items:
        resp, data = m.fetch(emailid, "(RFC822)")
        email_body = data[0][1]
        mail = email.message_from_string(email_body)
        mailtype = mail.get_content_maintype()
        if mailtype != 'text':
            raise Exception('unsupport mailtype: %s %s' % (emailid, mailtype))
        for part in mail.walk():
            subject = email.Header.decode_header(part["Subject"])
            content = part.get_payload(decode=True).decode('utf-8')
            subject = email.Header.decode_header(part["Subject"])
            if subject[0][1]:
                subject = unicode(subject[0][0],subject[0][1])
            else:
                subject = unicode(subject[0][0])
            result.append({'subject': subject, 'content': content})
            break
    return result

def delete_emails():
    m = imaplib.IMAP4_SSL("imap.gmail.com")
    m.login(username, password)
    m.select(folder)
    resp, items = m.search(None, "ALL")
    items = items[0].split()
    for item in items:
        m.store(item, '+X-GM-LABELS', '\\Trash')

if __name__ == '__main__':
    results = get_emails()
    count = 0
    for result in results:
        count += 1
        print('-'*10 + 'start ' + str(count) + '-'*10)
        subject = result['subject']
        content = result['content']
        msg = 'subject: %s\ncontent: %s\n' % (subject, content)
        print(msg)
        print('-'*10 + 'stop ' + str(count) + '-'*10)
    delete_emails()
