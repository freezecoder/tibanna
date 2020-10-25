#!/bin/bash

LOGFILE=${1:-"logfile.txt"}

TOKEN=`curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600"`

while sleep 10; do
    HTTP_CODE=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" -s -w %{http_code} -o /dev/null http://169.254.169.254/latest/meta-data/spot/instance-action)

    if [[ "$HTTP_CODE" -eq 401 ]] ; then
        echo 'Refreshing Authentication Token'
        TOKEN=`curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 30"`
    elif [[ "$HTTP_CODE" -eq 200 ]] ; then
        # Insert Your Code to Handle Interruption Here
	echo Instance is being terminated by AWS Spot |tee -a $LOGFILE
	echo Instance is lost |tee -a $LOGFILE
    else
        echo 'Instance is alive Not Interrupted' > instance.notice
    fi

done
