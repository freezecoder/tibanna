#!/usr/bin/python
import json
import sys
import boto3
import re
import time
import random
import string
import os
import subprocess
import argparse
import os.path
import watchtower, logging
from datetime import datetime
logging.basicConfig(level=logging.INFO)



## Do updates to dynamodb and job status from running vm,requires apt IAM Roles for Dynamodb

source_directory = '/data1/out/'
region='us-east-1'
session= boto3.Session(region_name = region)
s3 = boto3.client('s3',region_name=region)

BUCKETNAME=""
JOBTBL="bgm-jobs"
LOG_GROUP="zeegenomics"
LOG_STREAM="defaultstream"
JOBID="somejobid"
OUTBUCKET="bgtibanna"

#acquire the log details from env. variables
try:
	LOG_GROUP=os.environ["LOG_GROUP"]
	LOG_STREAM=os.environ["LOG_STREAM"]
except Exception as e:
	print("Sorry: did not find env variables LOG_GROUP, LOG_STREAM and JOBID")
	print(str(e))
	pass

#Add job id 
try:
	JOBID=os.environ["JOBID"]
except:
	print("Could not find JOBID env var")
	pass

#Add BUCKET NAME
try:
	BUCKETNAME=os.environ["OUTBUCKET"]
except:
	print("OUTBUCKET environment variable not defined")
	print("Setting BUCKETNAME to empty string")
	pass



from pynamodb.models import Model
import uuid
from pynamodb.attributes import ( UnicodeAttribute, NumberAttribute, UnicodeSetAttribute, UTCDateTimeAttribute)
class Bgmofiles(Model):
    """
    AWS Cloud file storage bucket
    A DynamoDB Output files from cloud
    """
    class Meta:
        table_name = "bgm-ofiles"
        region = 'us-east-1'
    name = UnicodeAttribute(hash_key=True,default="somefile"+str(uuid.uuid4()))
    jobid = UnicodeAttribute(hash_key=False,default=uuid.uuid4())
    size = NumberAttribute(null=True,default=0)
    bucket=UnicodeAttribute(null=True,default=BUCKETNAME)
    created = UTCDateTimeAttribute(null=True,default=datetime.now())
    modified = UTCDateTimeAttribute(null=True,default=datetime.now())


#Create logger
def make_logger(name=LOG_GROUP,stream=LOG_STREAM):
  logger = logging.getLogger(__name__)
  ch = logging.StreamHandler()
  ch.setLevel(logging.ERROR)

  logger.addHandler(ch)
  if stream is None:
    logger.addHandler(watchtower.CloudWatchLogHandler(log_group=name,boto3_session=session))
  else:
    logger.addHandler(watchtower.CloudWatchLogHandler(log_group=name,stream_name=stream,boto3_session=session))
  return(logger)


""" Update the job table with instance Id  """
def update_instanceid(jobid=JOBID,tblname=JOBTBL,col='instanceid',value='i-xxxxxxxxxxxxx'):
    expr="set "+str(col)+"= :r"
    table = boto3.resource('dynamodb',region_name=region).Table(tblname)
    response = table.update_item(
        Key={
            'jobid': jobid,
        },
        UpdateExpression=expr,
        ExpressionAttributeValues={
            ':r': value,
        },
        ReturnValues="UPDATED_NEW"
    )


""" Update the job table  """
def update_jobs(jobid=JOBID,tblname=JOBTBL,col='description',value='A new description from me'):
    expr="set "+str(col)+"= :r"
    table = boto3.resource('dynamodb',region_name=region).Table(tblname)
    response = table.update_item(
        Key={
            'jobid': jobid,
        },
        UpdateExpression=expr,
        ExpressionAttributeValues={
            ':r': value,
        },
        ReturnValues="UPDATED_NEW"
    )

""" Convenience for setting job status """
def set_job(jobid,status):
    update_jobs(jobid=jobid,col='jstatus',value=status)

""" Update  job timestamp """
def update_jobtime(jobid):
    print('Updating job {jobid} timestamp'.format(jobid=jobid))
    update_jobs(jobid=jobid,col='modified',value=str(datetime.now()))



def parse_command(logfile):
    """
    parse commands from the log file and returns the commands as a list
    of command line lists, each corresponding to a step run.
    """
    command_list = []
    command = []
    in_command = False
    with open(logfile, 'r') as f:
        for line in f:
            line = line.strip('\n')
            if line.startswith('[job') and line.endswith('docker \\'):
                in_command = True
            if in_command:
                command.append(line.strip('\\'))
                if not line.endswith('\\'):
                    in_command = False
                    command_list.append(command)
                    command = []
    return(command_list)


def add_cloud_from_s3(bucket=BUCKETNAME,key="somekey",jobid="somejobid",suffix=None):
    print("Adding Data from cloud to db table")
    s3 = boto3.resource('s3')
    bucketobj = s3.Bucket(bucket)
    print("Fetching files from s3://"+str(bucket)+"/"+str(key))
    try:
      for obj in bucketobj.objects.filter(Prefix=key):
        print("S3:  Adding "+ bucket + " " + str(obj.key) + " to " + jobid + " to bgm-ofiles files database")
        of=Bgmofiles(obj.key)
        of.bucket=bucket
        of.jobid=jobid
        of.size=int(obj.size)
        of.created=obj.last_modified
        of.modified=obj.last_modified
        of.save()
    except Exception as e:
        print("Failed")
        print(e)



def upload_to_s3(s3, source, bucket, target):
    if os.path.isdir(source):
        print("source " + source + " is a directory")
        source = source.rstrip('/')
        for root, dirs, files in os.walk(source):
            for f in files:
                source_f = os.path.join(root, f)
                if root == source:
                    target_f = os.path.join(target, f)
                else:
                    target_subdir = re.sub('^' + source + '/', '', root)
                    target_f = os.path.join(target, target_subdir, f)
                print("source_f=" + source_f)
                print("target_f=" + target_f)
                s3.upload_file(source_f, bucket, target_f)
            # for d in dirs:
            #     source_d = os.path.join(root, d)
            #     target_d = os.path.join(target, re.sub(source + '/', '', root), d)
            #     upload_to_s3(s3, source_d, bucket, target_d)
    else:
        print("source " + source + " is a not a directory")
        s3.upload_file(source, bucket, target)


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="")
  parser.add_argument("-cmd", help="Task to run. message,touch,status,addfiles",default="message",required=True)
  parser.add_argument("-message", help="Message text",required=False,default="Hi there")
  parser.add_argument("-status", help="task to perform (message|touch|status|instance)",required=False,default="running")
  parser.add_argument("-instance", help="Instance",required=False,default="ix2")
  args = parser.parse_args()
  cmd=args.cmd
  status=args.status
  message=args.message
  instance=args.instance
  logger=make_logger()
  if cmd=="message":
    print("sending message: "+ str(message))
    logger.info(message)
    #logger.critical("Critical messsage")
  elif cmd=="touch":
    logger.info("Updating "+str(JOBID))
    update_jobtime(JOBID)
  elif cmd=="status":
    logger.info("Setting job status to "+status)
    print("Setting job status to "+status)
    set_job(JOBID,status)
    update_jobtime(JOBID)
  elif cmd=="instance":
    logger.info("Setting job "+str(JOBID)+" instance id  to "+ instance)
    update_instanceid(jobid=JOBID,value=instance)
  elif cmd=="addfiles":
    print("addfiles mode:")
    print("Adding S3 Output files - if they exist ")
    k=str(JOBID)+".workflow"
    print(k)
    add_cloud_from_s3(bucket=BUCKETNAME,key=k,jobid=JOBID)

   
    
    
    
   
  

