# -*- coding: utf-8 -*-

from core import ec2_utils
from core.utils import powerup, check_dependency
import os


def metadata_only(event):
    event.update({'jobid': 'metadata_only'})
    return event


@powerup('run_task_awsem', metadata_only)
def handler(event, context):
    '''
    config:
    # required
      instance_type: EC2 instance type
      ebs_size: EBS storage size in GB
      ebs_type: EBS storage type (available values: gp2, io1, st1, sc1, standard (default: io1)
      ebs_iops: EBS storage IOPS
      password: password for ssh connection for user ec2-user
      EBS_optimized: Use this flag if the instance type is EBS-optimized (default: EBS-optimized)
      shutdown_min: Number of minutes before shutdown after the jobs are finished. (default now)
      log_bucket: bucket for collecting logs (started, postrun, success, error, log)
    # required for wdl
      language: 'cwl_v1','cwl_draft3', or 'wdl'
    # optional
      public_postrun_json (optional): whether postrun json should be made public (default false)

    args:
    # required (i.e. field must exist, value may be null):
      app_name: name of the app
      app_version: version of the app
      output_S3_bucket: bucket name and subdirectory for output files and logs
      input_files: input files in json format (parametername: {'bucket_name':bucketname, 'object_key':filename})
      secondary_files: secondary files in json format (parametername: {'bucket_name':bucketnname, 'object_ke':filename})
      input_parameters: input parameters in json format (parametername:value)
    # required for cwl
      cwl_main_filename: main cwl file name
      cwl_child_filenames: names of the other cwl files used by main cwl file, delimiated by comma
      cwl_directory_url: the url and subdirectories for the main cwl file
      cwl_version: the version of cwl (either 'draft3' or 'v1')
    # required for wdl
      wdl_filename: wdl file name
      wdl_directory_url: the url of the wdl file
    # optional
      dependency: {'exec_arn': [exec_arns]}
    '''

    # read default variables in config
    CONFIG_FIELD = "config"
    CONFIG_KEYS = ["EBS_optimized", "shutdown_min", "instance_type", "ebs_size", "key_name",
                   "ebs_type", "ebs_iops", "json_bucket", "password", "log_bucket"]
    ARGS_FIELD = "args"
    ARGS_KEYS = ["app_name", "app_version", "input_files", "output_S3_bucket",
                 "input_parameters", "secondary_files", "output_target", "secondary_output_target"]
    ARGS_KEYS_CWL = ["cwl_main_filename", "cwl_child_filenames", "cwl_directory_url"]
    ARGS_KEYS_WDL = ["wdl_filename", "wdl_directory_url"]

    cfg = event.get(CONFIG_FIELD)
    for k in CONFIG_KEYS:
        assert k in cfg, "%s not in config_field" % k

    args = event.get(ARGS_FIELD)
    for k in ARGS_KEYS:
        assert k in args, "%s not in args field" % k
    if 'language' in cfg and cfg['language'] == 'wdl':
        for k in ARGS_KEYS_WDL:
            assert k in args, "%s not in args field" % k
    else:
        for k in ARGS_KEYS_CWL:
            assert k in args, "%s not in args field" % k

    if 'dependency' in args:
        check_dependency(**args['dependency'])

    # args: parameters needed by the instance to run a workflow
    # cfg: parameters needed to launch an instance
    cfg['job_tag'] = args.get('app_name')
    cfg['userdata_dir'] = '/tmp/userdata'

    # local directory in which the json file will be first created.
    cfg['json_dir'] = '/tmp/json'

    # postrun json should be made public?
    if 'public_postrun_json' not in cfg:
        cfg['public_postrun_json'] = False
        # 4dn will use 'true' --> this will automatically be added by start_run_awsem

    # script url
    cfg['script_url'] = 'https://raw.githubusercontent.com/' + \
        os.environ.get('TIBANNA_REPO_NAME') + '/' + \
        os.environ.get('TIBANNA_REPO_BRANCH') + '/awsf/'

    # AMI and script directory according to cwl version
    if 'language' in cfg and cfg['language'] == 'wdl':
        cfg['ami_id'] = os.environ.get('AMI_ID_WDL')
    else:
        if args['cwl_version'] == 'v1':
            cfg['ami_id'] = os.environ.get('AMI_ID_CWL_V1')
            cfg['language'] = 'cwl_v1'
        else:
            cfg['ami_id'] = os.environ.get('AMI_ID_CWL_DRAFT3')
            cfg['language'] = 'cwl_draft3'
        if args.get('singularity', False):
            cfg['singularity'] = True

    ec2_utils.update_config(cfg, args['app_name'],
                            args['input_files'], args['input_parameters'])

    # create json and copy to s3
    jobid = ec2_utils.create_json(event)

    # profile
    if os.environ.get('TIBANNA_PROFILE_ACCESS_KEY', None) and \
            os.environ.get('TIBANNA_PROFILE_SECRET_KEY', None):
        profile = {'access_key': os.environ.get('TIBANNA_PROFILE_ACCESS_KEY'),
                   'secret_key': os.environ.get('TIBANNA_PROFILE_SECRET_KEY')}
    else:
        profile = None

    # launch instance and execute workflow
    launch_instance_log = ec2_utils.launch_instance(cfg, jobid, profile=profile)

    if 'jobid' not in event:
        event.update({'jobid': jobid})
    event.update(launch_instance_log)
    return(event)
