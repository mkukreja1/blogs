#!/bin/bash
stty -echo
sh start_api.sh 
aws ec2 delete-security-group --group-name "RDS Security Group" > /dev/null
aws kinesis create-stream --stream-name hotelStream --shard-count 1  > /dev/null            
RDS_GROUP=`aws ec2 create-security-group --description sg-rds --group-name "RDS Security Group" | grep GroupId | sed 's/"GroupId"://' | sed 's/"//g' | sed 's/,//g'`;echo $RDS_GROUP > /dev/null
aws ec2 authorize-security-group-ingress --group-id $RDS_GROUP  --protocol tcp --port 3306 --cidr 0.0.0.0/0 > /dev/null
aws rds create-db-instance --db-instance-identifier hotelcdc --db-instance-class db.t2.micro --engine mysql --region us-east-1 --output text --master-username admin --master-user-password admin123 --allocated-storage 20 --vpc-security-group-ids $RDS_GROUP --db-parameter-group-name default.mysql8.0 --option-group-name default:mysql-8-0 --engine-version 8.0.27 > /dev/null
RDS_ENDPOINT=`aws rds describe-db-instances --db-instance-identifier hotelcdc | grep "Address" | sed 's/.*://'   | sed 's/"//g'    | sed 's/,//g'`;echo $RDS_ENDPOINT
until [ "echo $RDS_ENDPOINT" == "" ]
do
	if [ -z "$RDS_ENDPOINT" ]
    then
	  RDS_ENDPOINT=`aws rds describe-db-instances --db-instance-identifier hotelcdc | grep "Address" | sed 's/.*://'   | sed 's/"//g'    | sed 's/,//g'`
	  echo "RDS instance is not created as yet. Sleeping 60 seconds, will check again."
	  sleep 60
	else
      break
	fi
done
echo "All done. The RDS instance has been sucessfully created. Type MySQL Endpoint: $RDS_ENDPOINT, Username: admin, Password: admin123 Please take note of these details since you will need to use them in the Databricks notebook."
stty echo

