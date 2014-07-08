#! /bin/bash

if [ $# -ne 1 ]; then
	echo "input and only input a stack name"
	exit 1
fi

stack_name="$1"
key_name="yupeng"
region="cn-north-1"
password="123"

aws cloudformation create-stack --stack-name $stack_name --template-body file://life_cycle.json --parameters ParameterKey="KeyName",ParameterValue="$key_name" ParameterKey="Password",ParameterValue="$password" --region $region
