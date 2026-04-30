# -*- coding: utf-8 -*-
import boto3
import json
import copy
import time as time_module
from .cw_utils import TibannaResource
from datetime import datetime, timedelta
from dateutil.tz import tzutc
from .utils import (
    printlog,
    does_key_exist,
    read_s3
)
from .awsem import (
    AwsemPostRunJson
)
from .exceptions import (
    StillRunningException,
    EC2StartingException,
    AWSEMJobErrorException,
    EC2UnintendedTerminationException,
    EC2IdleException,
    MetricRetrievalException
)

RESPONSE_JSON_CONTENT_INCLUSION_LIMIT = 30000  # strictly it is 32,768 but just to be safe.

CLOUDDOCKERJOBS_LOG_GROUP = "clouddockerjobs"
BGMCLOUDDOCKERS_TABLE = "bgm-jobs"


def check_task(input_json):
    return CheckTask(input_json).run()


class CheckTask(object):
    TibannaResource = TibannaResource

    def __init__(self, input_json):
        self.input_json = copy.deepcopy(input_json)

    def run(self):
        input_json_copy = self.input_json
    
        # s3 bucket that stores the output
        bucket_name = input_json_copy['config']['log_bucket']
    
        # info about the jobby job
        jobid = input_json_copy['jobid']
        job_started = "%s.job_started" % jobid
        job_success = "%s.success" % jobid
        job_error = "%s.error" % jobid
    
        public_postrun_json = input_json_copy['config'].get('public_postrun_json', False)
    
        # check to see ensure this job has started else fail
        if not does_key_exist(bucket_name, job_started):
            raise EC2StartingException("Failed to find jobid %s, ec2 is probably still booting" % jobid)

        # Check if job is actively running via CloudWatch Logs and update bgm-jobs to 'running'
        self.poll_cloudwatch_and_update_running(jobid, bucket_name, job_success, input_json_copy, public_postrun_json)
    
        # check to see if job has error, report if so
        if does_key_exist(bucket_name, job_error):
            try:
                self.handle_postrun_json(bucket_name, jobid, input_json_copy, public_read=public_postrun_json)
            except Exception as e:
                printlog("error handling postrun json %s" % str(e))
            errmsg = "Job encountered an error check log using tibanna log --job-id=%s [--sfn=stepfunction]" % jobid
            raise AWSEMJobErrorException(errmsg)
    
        # check to see if job has completed
        if does_key_exist(bucket_name, job_success):
            try:
                self.handle_postrun_json(bucket_name, jobid, input_json_copy, public_read=public_postrun_json)
            except Exception as e:
                # Metrics collection can fail (e.g. no instance_id in test jobs) — job is still complete
                printlog("handle_postrun_json failed (non-critical): %s" % str(e))
            print("completed successfully")
            return input_json_copy
    
        # checking if instance is terminated for no reason
        instance_id = input_json_copy['config'].get('instance_id', '')
        if instance_id:  # skip test for instance_id by not giving it to input_json_copy
            try:
                res = boto3.client('ec2').describe_instances(InstanceIds=[instance_id])
            except Exception as e:
                if 'InvalidInstanceID.NotFound' in str(e):
                    raise EC2UnintendedTerminationException("EC2 is no longer found for job %s - please rerun." % jobid)
                else:
                    raise e
            if not res['Reservations']:
                raise EC2UnintendedTerminationException("EC2 is no longer found for job %s - please rerun." % jobid)
            else:
                ec2_state = res['Reservations'][0]['Instances'][0]['State']['Name']
                if ec2_state in ['stopped', 'shutting-down', 'terminated']:
                    errmsg = "EC2 is terminated unintendedly for job %s - please rerun." % jobid
                    printlog(errmsg)
                    raise EC2UnintendedTerminationException(errmsg)
    
            # check CPU utilization for the past hour
            filesystem = '/dev/nvme1n1'  # doesn't matter for cpu utilization
            end = datetime.now(tzutc())
            start = end - timedelta(hours=1)
            jobstart_time = boto3.client('s3').get_object(Bucket=bucket_name, Key=job_started).get('LastModified')
            if jobstart_time + timedelta(hours=1) < end:
                try:
                    cw_res = self.TibannaResource(instance_id, filesystem, start, end).as_dict()
                except Exception as e:
                    raise MetricRetrievalException(e)
                if 'max_cpu_utilization_percent' in cw_res:
                    if not cw_res['max_cpu_utilization_percent'] or cw_res['max_cpu_utilization_percent'] < 1.0:
                        # the instance wasn't terminated - otherwise it would have been captured in the previous error.
                        if not cw_res['max_ebs_read_bytes'] or cw_res['max_ebs_read_bytes'] < 1000:  # minimum 1kb
                        # in case the instance is copying files using <1% cpu for more than 1hr, do not terminate it.
                            try:
                                boto3.client('ec2').terminate_instances(InstanceIds=[instance_id])
                                errmsg = "Nothing has been running for the past hour for job %s " + \
                                         "(CPU utilization %s and EBS read %s bytes)." % \
                                         (jobid, str(cw_res['max_cpu_utilization_percent']), str(cw_res['max_ebs_read_bytes']))
                                raise EC2IdleException(errmsg)
                            except Exception as e:
                                errmsg = "Nothing has been running for the past hour for job %s," + \
                                         "but cannot terminate the instance (cpu utilization (%s) : %s" % \
                                         jobid, str(cw_res['max_cpu_utilization_percent']), str(e)
                                printlog(errmsg)
                                raise EC2IdleException(errmsg)
    
        # if none of the above
        raise StillRunningException("job %s still running" % jobid)

    def poll_cloudwatch_and_update_running(self, jobid, bucket_name, job_success, input_json_copy, public_postrun_json):
        """Poll CloudWatch Logs until the job's biodocker log stream shows events,
        then update bgm-jobs status to 'running'. Retries with exponential backoff.

        Log streams are named: biodocker_{jobid}_ip-{instance_ip}
        We use filter-log-events on the clouddockerjobs log group to find the stream
        without needing the instance IP.

        Retries: starts at 30s wait, doubles each attempt up to 120s, max ~4 min total.

        Also checks for .success during polling to handle short-running jobs that
        complete while the Lambda is waiting on CloudWatch retries.
        """
        cw = boto3.client('logs')
        log_group = CLOUDDOCKERJOBS_LOG_GROUP

        # Exponential backoff: [30, 60, 120, 120, 120, ...] — up to ~330s total wait
        # Lambda timeout is 300s, so keep total wait under 270s to leave headroom
        delays = [30, 60, 120, 120, 120]
        max_total_wait = 270  # seconds

        waited = 0
        for attempt, delay in enumerate(delays):
            if waited + delay > max_total_wait:
                delay = max_total_wait - waited
            if delay <= 0:
                break

            printlog("poll_cloudwatch attempt %d: waiting %ds for job %s log events" % (attempt + 1, delay, jobid))
            time_module.sleep(delay)
            waited += delay

            try:
                # Find log streams matching biodocker_{jobid}_*
                streams_resp = cw.describe_log_streams(
                    logGroupName=log_group,
                    logStreamNamePrefix="biodocker_%s" % jobid
                )
                matching_streams = [
                    s for s in streams_resp.get('logStreams', [])
                    if s['logStreamName'].startswith("biodocker_%s" % jobid)
                ]
            except Exception as e:
                printlog("poll_cloudwatch: describe_log_streams error for job %s: %s" % (jobid, e))
                continue

            if not matching_streams:
                printlog("poll_cloudwatch attempt %d: no log stream found yet for job %s" % (attempt + 1, jobid))
                continue

            # Get the most recent stream (there should only be one per job)
            stream = max(matching_streams, key=lambda s: s.get('lastIngestionTime', 0))
            stream_name = stream['logStreamName']

            try:
                events_resp = cw.get_log_events(
                    logGroupName=log_group,
                    logStreamName=stream_name,
                    limit=1,
                    startFromHead=False
                )
                events = events_resp.get('events', [])
            except Exception as e:
                printlog("poll_cloudwatch: get_log_events error for stream %s: %s" % (stream_name, e))
                continue

            if events:
                printlog("poll_cloudwatch: stream %s found with %d events — updating bgm-jobs to running" % (stream_name, len(events)))
                self._update_bgm_jobs_running(jobid)
                return  # success — job is confirmed running

            printlog("poll_cloudwatch attempt %d: stream %s exists but no events yet" % (attempt + 1, stream_name))

            # Early exit: check if job already completed while we were polling
            if does_key_exist(bucket_name, job_success):
                printlog("poll_cloudwatch: .success appeared during polling — handling completion")
                try:
                    self.handle_postrun_json(bucket_name, jobid, input_json_copy, public_read=public_postrun_json)
                except Exception as e:
                    printlog("handle_postrun_json failed (non-critical): %s" % str(e))
                print("completed successfully")
                return input_json_copy

        printlog("poll_cloudwatch: max retries reached for job %s — will retry on next check_task invocation" % jobid)

    def _update_bgm_jobs_running(self, jobid):
        """Update bgm-jobs status and jstatus to 'running' and set modified to now."""
        try:
            now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000000Z")
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table(BGMCLOUDDOCKERS_TABLE)
            table.update_item(
                Key={'jobid': jobid},
                UpdateExpression="SET #s = :s, #js = :js, #m = :m",
                ExpressionAttributeNames={
                    '#s': 'status',
                    '#js': 'jstatus',
                    '#m': 'modified'
                },
                ExpressionAttributeValues={
                    ':s': 'running',
                    ':js': 'running',
                    ':m': now
                }
            )
            printlog("bgm-jobs updated: jobid=%s status=running modified=%s" % (jobid, now))
        except Exception as e:
            printlog("poll_cloudwatch: failed to update bgm-jobs for job %s: %s" % (jobid, e))

    def handle_postrun_json(self, bucket_name, jobid, input_json, public_read=False):
        postrunjson = "%s.postrun.json" % jobid
        if not does_key_exist(bucket_name, postrunjson):
            postrunjson_location = "https://s3.amazonaws.com/%s/%s" % (bucket_name, postrunjson)
            raise Exception("Postrun json not found at %s" % postrunjson_location)
        postrunjsoncontent = json.loads(read_s3(bucket_name, postrunjson))
        prj = AwsemPostRunJson(**postrunjsoncontent)
        prj.Job.update(instance_id=input_json['config'].get('instance_id', ''))
        self.handle_metrics(prj)
        printlog("inside funtion handle_postrun_json")
        printlog("content=\n" + json.dumps(prj.as_dict(), indent=4))
        # upload postrun json file back to s3
        acl = 'public-read' if public_read else 'private'
        try:
            boto3.client('s3').put_object(Bucket=bucket_name, Key=postrunjson, ACL=acl,
                                          Body=json.dumps(prj.as_dict(), indent=4).encode())
        except Exception as e:
            boto3.client('s3').put_object(Bucket=bucket_name, Key=postrunjson, ACL='private',
                                          Body=json.dumps(prj.as_dict(), indent=4).encode())
        except Exception as e:
            raise "error in updating postrunjson %s" % str(e)
        # add postrun json to the input json
        self.add_postrun_json(prj, input_json, RESPONSE_JSON_CONTENT_INCLUSION_LIMIT)
    
    def add_postrun_json(self, prj, input_json, limit):
        prjd = prj.as_dict()
        if len(str(prjd)) + len(str(input_json)) < limit:
            input_json['postrunjson'] = prjd
        else:
            del prjd['commands']
            if len(str(prjd)) + len(str(input_json)) < limit:
                prjd['log'] = 'postrun json not included due to data size limit'
                input_json['postrunjson'] = prjd
            else:
                input_json['postrunjson'] = {'log': 'postrun json not included due to data size limit'}
    
    def handle_metrics(self, prj):
        try:
            resources = self.TibannaResource(prj.Job.instance_id,
                                             prj.Job.filesystem,
                                             prj.Job.start_time_as_str,
                                             prj.Job.end_time_as_str or datetime.now())
        except Exception as e:
            raise MetricRetrievalException("error getting metrics: %s" % str(e))
        prj.Job.update(Metrics=resources.as_dict())
        resources.plot_metrics(prj.config.instance_type, directory='/tmp/tibanna_metrics/')
        resources.upload(bucket=prj.config.log_bucket, prefix=prj.Job.JOBID + '.metrics/')
