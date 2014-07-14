#! /bin/bash

echo $* > /tmp/setup.out

data_db_name=$1
metadata_db_name=$2
complaint_queue_name=$3
bounce_queue_name=$4
region=$5
username="$6"
password="$7"

sed -i "s/replace_by_data_db_name/$data_db_name/g" conf.yaml
sed -i "s/replace_by_metadata_db_name/$metadata_db_name/g" conf.yaml
sed -i "s/replace_by_complaint_queue_name/$complaint_queue_name/g" conf.yaml
sed -i "s/replace_by_bounce_queue_name/$bounce_queue_name/g" conf.yaml
sed -i "s/replace_by_region/$region/g" conf.yaml

default_login_file_place="/tmp/login_file"
# sed -i "s/replace_by_login_file/\"$default_login_file_place\"/g" conf.yaml

secret_key=`date '+%N'`
sed -i "s/replace_by_secret_key/$secret_key/g" login_file.yaml
sed -i "s/replace_by_username/$username/g" login_file.yaml
sed -i "s/replace_by_password/$password/g" login_file.yaml

mv login_file.yaml $default_login_file_place

yum install -y nginx
yum install -y python-pip
yum install -y gcc
yum install -y python-devel
pip install flask
pip install uwsgi
pip install pexpect
pip install python-crontab
pip install flask-login

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
mkdir upload
mkdir job

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

