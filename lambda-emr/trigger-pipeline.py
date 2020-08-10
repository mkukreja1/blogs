from __future__ import division

import boto3
import json
import logging
import base64

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        key_split=key.split("/")
        JOB_DATE=key_split[-2]
        
    client = boto3.client("emr")        
    cluster_id = client.run_job_flow(
        Name='Transient '+JOB_DATE,
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole',
        ReleaseLabel='emr-6.0.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'MASTER',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'CORE',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                }
            ],
        },
        Applications=[{
            'Name': 'Spark'
        }],
        BootstrapActions=[{
            'Name': 'Install',
            'ScriptBootstrapAction': {
                'Path': 's3://aws-analytics-course/job/energy/emr.sh'
            }
        }],
        Steps=[{
            'Name': 'RunCuration',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    "spark-submit", "--deploy-mode", "cluster",
                    's3://aws-analytics-course/job/energy/renewable-curation.py',
					'--JOB_DATE', JOB_DATE
                ]
            }
        }],
    )
    return "Started new cluster {}".format(cluster_id)