#!/bin/bash
#Record an EC2 interruption if the instance is spot

LOGFILE=${1:-"logfile.txt"}
CLOUDFILE=${2:-"s3://bgtibanna/somejob.lost"}

export INSTANCE_ID=$(ec2metadata --instance-id |cut -d' ' -f2)
export INSTANCE_REGION=$(ec2metadata --availability-zone | sed 's/[a-z]$//')
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity| grep Account | sed 's/[^0-9]//g')


TOKEN=`curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600"`
while sleep 10; do
    HTTP_CODE=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" -s -w %{http_code} -o /dev/null http://169.254.169.254/latest/meta-data/spot/instance-action)

    if [[ "$HTTP_CODE" -eq 401 ]] ; then
        echo 'Refreshing Authentication Token'
        TOKEN=`curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 30"`
    elif [[ "$HTTP_CODE" -eq 200 ]] ; then
        # Insert Your Code to Handle Interruption Here
	echo WARNING Instance is being terminated by AWS Spot |tee -a $LOGFILE
	echo WARNING Instance will be lostt |tee -a $LOGFILE
	echo $INSTANCE_ID > c.lost;aws s3 cp c.lost $CLOUDFILE	
    else
        echo 'Instance is alive and well' > instance.notice
    fi

done
