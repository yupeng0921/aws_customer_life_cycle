#! /usr/bin/env python

import pexpect

ssl = pexpect.spawn('openssl req -new -key server.key -out server.csr', timeout=5)
ssl.expect('Country Name')
ssl.sendline('.')
ssl.expect('State or Province Name')
ssl.sendline('.')
ssl.expect('Locality Name')
ssl.sendline('.')
ssl.expect('Organization Name')
ssl.sendline('.')
ssl.expect('Organizational Unit Name')
ssl.sendline('.')
ssl.expect('Common Name')
ssl.sendline('example.com')
ssl.expect('Email Address')
ssl.sendline('.')
ssl.expect('A challenge password')
ssl.sendline('')
ssl.expect('An optional company name')
ssl.sendline('')
ssl.interact()
