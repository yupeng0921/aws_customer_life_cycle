#! /bin/bash

# echo $* > /tmp/setup.out

data_db_name=$1
metadata_db_name=$2
region=$3

sed -i "s/replace_by_data_db_name/$data_db_name/g" conf.yaml
sed -i "s/replace_by_metadata_db_name/$metadata_db_name/g" conf.yaml
sed -i "s/replace_by_region/$region/g" conf.yaml

yum install -y nginx
yum install -y python-pip
yum install -y gcc
yum install -y python-devel
pip install flask
pip install uwsgi
pip install pexpect
pip install python-crontab

cp nginx.conf /etc/nginx/nginx.conf

openssl genrsa -out server.key 1024
python ./generate_csr.py
cp server.key server.key.org
openssl rsa -in server.key.org -out server.key
openssl x509 -req -days 3650 -in server.csr -signkey server.key -out server.crt

ssl_conf_dir="/usr/local/nginx/conf"
mkdir -p $ssl_conf_dir
cp server.crt $ssl_conf_dir
cp server.key $ssl_conf_dir

touch /tmp/stdin
touch /tmp/stdout
touch /tmp/stderr
service nginx start
chkconfig nginx on

cmd="/opt/aws_customer_life_cycle-master/life_cycle_daemon.py start"
echo "$cmd" >> /etc/rc.local
echo "$cmd" > /tmp/run.sh
bash /tmp/run.sh

cmd="cd /opt/aws_customer_life_cycle-master; uwsgi --socket 127.0.0.1:3031 --wsgi-file server.py --callable app --processes 2 --threads 4 --stats 127.0.0.1:9191 -d /opt/aws_customer_life_cycle-master/uwsgi.log"

echo "$cmd" >> /etc/rc.local
echo "$cmd" > /tmp/run.sh
bash /tmp/run.sh


