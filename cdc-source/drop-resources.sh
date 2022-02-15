aws kinesis delete-stream --stream-name hotelStream
aws ec2 delete-security-group --group-name "RDS Security Group"
aws rds delete-db-instance --db-instance-identifier hotelcdc --skip-final-snapshot             