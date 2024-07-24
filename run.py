import json
import os
import time
from pprint import pprint
from time import sleep
from typing import Callable, TypedDict, TypeVar

from boto3 import client

from lib import query as q

STACK_NAME = os.getenv("STACK_NAME", "")
ENDPOINT_URL = os.getenv("ENDPOINT_URL")

cfn = client("cloudformation", endpoint_url=ENDPOINT_URL)
dms = client("dms", endpoint_url=ENDPOINT_URL)
kinesis = client("kinesis", endpoint_url=ENDPOINT_URL)
s3 = client("s3", endpoint_url=ENDPOINT_URL)
secretsmanager = client("secretsmanager", endpoint_url=ENDPOINT_URL)

retries = 100 if not ENDPOINT_URL else 10
retry_sleep = 5 if not ENDPOINT_URL else 1


class CfnOutput(TypedDict):
    fullLoadTask: str
    cdcTask: str
    kinesisStream: str


def get_cfn_output() -> CfnOutput:
    stacks = cfn.describe_stacks()["Stacks"]
    stack = None
    for s in stacks:
        if s["StackName"] == STACK_NAME:
            stack = s
            break
    if not stack:
        raise Exception(f"Stack {STACK_NAME} not found")

    outputs = stack["Outputs"]
    cfn_output = CfnOutput()
    for output in outputs:
        cfn_output[output["OutputKey"]] = output["OutputValue"]
    return cfn_output


T = TypeVar("T")


def retry(
    function: Callable[..., T], retries=retries, sleep=retry_sleep, **kwargs
) -> T:
    raise_error = None
    retries = int(retries)
    for i in range(0, retries + 1):
        try:
            return function(**kwargs)
        except Exception as error:
            raise_error = error
            time.sleep(sleep)
    raise raise_error

class S3Credentials(TypedDict):
    bucket_name: str
    bucket_folder: str
    change_data: str

def get_s3_credentials(secret_arn: str) -> S3Credentials:
    secret_value = secretsmanager.get_secret_value(SecretId=secret_arn)
    credentials = S3Credentials(**json.loads(secret_value["SecretString"]))
    return credentials

def upload_data_to_s3(file_name: str, data: str):
    bucket_folder = s3_credentials["bucket_folder"]
    s3.put_object(Bucket=s3_credentials["bucket_name"], Key=f"{bucket_folder}/{file_name}", Body=data)


def start_task(task: str):
    response = dms.start_replication_task(
        ReplicationTaskArn=task, StartReplicationTaskType="start-replication"
    )
    status = response["ReplicationTask"].get("Status")
    print(f"Replication Task {task} status: {status}")


def stop_task(task: str):
    response = dms.stop_replication_task(ReplicationTaskArn=task)
    status = response["ReplicationTask"].get("Status")
    print(f"\nReplication Task {task} status: {status}")


def wait_for_task_status(task: str, expected_status: str):
    print(f"Waiting for task status {expected_status}")

    def _wait_for_status():
        status = dms.describe_replication_tasks(
            Filters=[{"Name": "replication-task-arn", "Values": [task]}],
            WithoutSettings=True,
        )["ReplicationTasks"][0].get("Status")
        print(f"{task=} {status=}")
        assert status == expected_status

    retry(_wait_for_status)


def wait_for_kinesis(stream: str, expected_count: int, threshold_timestamp: int):
    print("\n\tKinesis events\n")
    print("Fetching Kinesis event")

    shard_id = kinesis.describe_stream(StreamARN=stream)["StreamDescription"]["Shards"][
        0
    ]["ShardId"]
    shard_iterator = kinesis.get_shard_iterator(
        StreamARN=stream,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON",
    )
    shard_iter = shard_iterator["ShardIterator"]
    all_records = []
    while shard_iter is not None:
        res = kinesis.get_records(ShardIterator=shard_iter, Limit=50)
        shard_iter = res["NextShardIterator"]
        records = res["Records"]
        for r in records:
            if r["ApproximateArrivalTimestamp"].timestamp() > threshold_timestamp:
                all_records.append(r)
        if len(all_records) >= expected_count:
            break
        print(f"Found {len(all_records)}, {expected_count=}")
        sleep(retry_sleep)
    print(f"Received: {len(all_records)} events")
    pprint(
        [
            {**json.loads(record["Data"]), "partition_key": record["PartitionKey"]}
            for record in all_records
        ]
    )


def describe_table_statistics(task_arn: str):
    res = dms.describe_table_statistics(
        ReplicationTaskArn=task_arn,
    )
    res["TableStatistics"] = sorted(
        res["TableStatistics"], key=lambda x: (x["SchemaName"], x["TableName"])
    )
    return res


def execute_full_load(cfn_output: CfnOutput):
    # Full load Flow
    threshold_timestamp = int(time.time())
    task = cfn_output["fullLoadTask"]
    stream = cfn_output["kinesisStream"]

    print("*" * 12)
    print("STARTING FULL LOAD FLOW")
    print("*" * 12)

    print("\tUploading data to S3")
    upload_data_to_s3("hr/employee/LOAD001.csv", q.SOURCE_CSV_EMPLOYEE_SAMPLE_DATA)
    upload_data_to_s3("hr/department/LOAD002.csv", q.SOURCE_CSV_DEPARTMENT_SAMPLE_DATA)
    upload_data_to_s3("hr/project/LOAD003.csv", q.SOURCE_CSV_PROJECT_SAMPLE_DATA)

    print("\n****Full Load Task****\n")
    print("\n\tStarting Full load task")
    start_task(task)
    wait_for_task_status(task, "stopped")
    wait_for_kinesis(stream, 16, threshold_timestamp)
    print("\n****End of Full Load Task****\n")

    print("\n****Table Statistics****\n")
    print("\tTable Statistics tasks")
    pprint(describe_table_statistics(task))


def execute_cdc(cfn_output: CfnOutput):
    # CDC Flow
    task = cfn_output["cdcTask"]
    stream = cfn_output["kinesisStream"]
    print("")
    print("*" * 12)
    print("STARTING CDC FLOW")
    print("*" * 12)

    threshold_timestamp = int(time.time())
    print("Starting CDC task")
    start_task(task)
    wait_for_task_status(task, "running")

    print("\n****Uploading CDC data to S3****\n")
    change_data = s3_credentials["change_data"]
    upload_data_to_s3(f"{change_data}/cdc0000000001.csv", q.CDC_FILE_SAMPLE_DATA_1)
    upload_data_to_s3(f"{change_data}/cdc0000000002.csv", q.CDC_FILE_SAMPLE_DATA_2)

    print("\n****CDC events****\n")
    wait_for_kinesis(stream, 15, threshold_timestamp)
    print("\n****End of CDC events****\n")

    print("\n****Table Statistics****\n")
    print("\tTable Statistics tasks")
    pprint(describe_table_statistics(task))

    stop_task(task)
    wait_for_task_status(task, "stopped")


if __name__ == "__main__":
    cfn_output = get_cfn_output()

    s3_credentials = get_s3_credentials(cfn_output["s3Secret"])

    execute_full_load(cfn_output)
    execute_cdc(cfn_output)
