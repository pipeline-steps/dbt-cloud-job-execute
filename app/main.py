import argparse
import enum
import json
import os
import sys
import time

import requests

CONFIG_ARG = "config_path"
DEFAULT_CONFIG = "/etc/config/config.json"


# These are documented on the dbt Cloud API docs
class DbtJobRunStatus(enum.IntEnum):
    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    ERROR = 20
    CANCELLED = 30

def _trigger_job(account_id, job_id, api_key, job_body) -> int:
    res = requests.post(
        url=f"https://emea.dbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/",
        headers={'Authorization': f"Token {api_key}"},
        json=job_body
    )

    try:
        res.raise_for_status()
    except:
        print(f"API token (last four): ...{api_key[-4:]}")
        raise

    response_payload = res.json()
    return response_payload['data']['id']


def _get_job_run_status(account_id, api_key, job_run_id):
    res = requests.get(
        url=f"https://emea.dbt.com/api/v2/accounts/{account_id}/runs/{job_run_id}/",
        headers={'Authorization': f"Token {api_key}"},
    )

    res.raise_for_status()
    response_payload = res.json()
    return response_payload['data']['status']


def run(account_id, job_id, api_key, command):
    info = f'run {os.environ.get("RUN")} of pipeline {os.environ.get("PIPELINE_NAME")}:{os.environ.get("PIPELINE_VERSION")} in namespace {os.environ.get("NAMESPACE")}'
    job_body = {
        'cause': "Triggered by "+info,
        "steps_override": [ command ]
    }
    job_run_id = _trigger_job(account_id, job_id, api_key, job_body)
    print(f"job_run_id = {job_run_id}")
    while True:
        time.sleep(5)
        status = _get_job_run_status(account_id, api_key, job_run_id)
        print(DbtJobRunStatus(status))
        if status == DbtJobRunStatus.SUCCESS:
            break
        elif status == DbtJobRunStatus.ERROR or status == DbtJobRunStatus.CANCELLED:
            raise Exception("Failure!")


print(f"Commandline arguments: {sys.argv}")
parser = argparse.ArgumentParser()
parser.add_argument(f"--{CONFIG_ARG}", default=DEFAULT_CONFIG)
args = vars(parser.parse_args())
config_file = args.get(CONFIG_ARG)
print(f"Reading config from {config_file}")
with open(config_file) as f:
    config = json.load(f)
print(f"Configuration:\n {config}")
account_id = config["account_id"]
job_id = config["job_id"]
api_key = os.environ.get("API_KEY")
command = config["command"]
print(f"Starting job {job_id} in account {account_id}")
try:
    run(account_id, job_id, api_key, command)
    print("DBT job executed successfully")
except Exception as e:
    print(f"Error executing DBT job: {e}")
    sys.exit(1)
