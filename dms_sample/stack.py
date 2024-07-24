import json
import os
from typing import Iterable

import aws_cdk as cdk
from aws_cdk import SecretValue, Stack
from aws_cdk import aws_dms as dms
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kinesis as kinesis
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct

BUCKET_NAME = os.getenv("BUCKET_NAME", "")
BUCKET_FOLDER = os.getenv("BUCKET_FOLDER", "")
CHANGE_DATA = os.getenv("CHANGE_DATA", "")

class DmsSampleStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # VPC configuration
        vpc = ec2.Vpc(
            self,
            "dms-sample",
            vpc_name="dmsSample",
            create_internet_gateway=True,
            enable_dns_hostnames=True,
            enable_dns_support=True,
            nat_gateways=0,
        )
        security_group = create_security_group(self, vpc)

        # Assume Role for DMS resources
        dms_assume_role = iam.Role(
            self,
            "dms-assume-role",
            assumed_by=iam.ServicePrincipal("dms.amazonaws.com").grant_principal,
        )

        # S3 Bucket
        bucket = s3.Bucket(
            self,
            "SourceBucket",
            bucket_name=BUCKET_NAME,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )
        bucket.grant_read_write(dms_assume_role)

        # Secret for S3 source endpoint
        s3_secret = create_s3_secret(self, bucket)

        # Source Endpoints for Full Load and CDC
        full_load_source_endpoint = create_s3_source_endpoint(self, bucket, dms_assume_role, "full-load-s3", full_load=True)
        cdc_source_endpoint = create_s3_source_endpoint(self, bucket, dms_assume_role, "cdc-s3", full_load=False)

        # Creation of the Kinesis Stream
        kinesis_stream = create_kinesis_stream(self, dms_assume_role)
        target_endpoint = create_kinesis_target_endpoint(self, kinesis_stream, dms_assume_role)

        # Creating a replication instance
        replication_instance = create_replication_instance(self, vpc, security_group)

        table_mappings = {
                "rules": [
                    {
                        "rule-type": "selection",
                        "rule-id": "1",
                        "rule-name": "all-tables",
                        "object-locator": {
                            "schema-name": "%",
                            "table-name": "%",
                        },
                        "rule-action": "include",
                    }
                ]
            }

        # CDC Task
        cdc_task = create_replication_task(
            self,
            "cdc-task",
            replication_instance=replication_instance,
            source=cdc_source_endpoint,
            target=target_endpoint,
            migration_type="cdc",
            table_mappings=table_mappings,
        )

        # Full Load Task
        full_load_task = create_replication_task(
            self,
            "full-load-task",
            replication_instance=replication_instance,
            source=full_load_source_endpoint,
            target=target_endpoint,
            migration_type="full-load",
            table_mappings=table_mappings,
        )

        cdk.CfnOutput(self, "cdcTask", value=cdc_task.ref)
        cdk.CfnOutput(self, "fullLoadTask", value=full_load_task.ref)
        cdk.CfnOutput(self, "kinesisStream", value=kinesis_stream.stream_arn)
        cdk.CfnOutput(self, "sourceBucket", value=bucket.bucket_name)
        cdk.CfnOutput(self, "s3Secret", value=s3_secret.secret_full_arn)


# Helper functions

def create_kinesis_target_endpoint(stack: Stack, target: kinesis.Stream, dms_assume_role: iam.Role) -> dms.CfnEndpoint:
    return dms.CfnEndpoint(
        stack,
        "target",
        endpoint_type="target",
        engine_name="kinesis",
        kinesis_settings=dms.CfnEndpoint.KinesisSettingsProperty(
            stream_arn=target.stream_arn,
            message_format="json",
            service_access_role_arn=dms_assume_role.role_arn,
            include_control_details=True,
            include_null_and_empty=True,
            include_partition_value=True,
            include_table_alter_operations=True,
            include_transaction_details=True,
            partition_include_schema_table=True,
        ),
    )

def create_s3_source_endpoint(stack: Stack, bucket: s3.Bucket, dms_assume_role: iam.Role, endpoint_id: str, full_load: bool) -> dms.CfnEndpoint:
    table_structure = {
        "TableCount": "3",
        "Tables": [
            {
                "TableName": "employee",
                "TablePath": "hr/employee/",
                "TableOwner": "hr",
                "TableColumns": [
                    {"ColumnName": "Id", "ColumnType": "INT8", "ColumnNullable": "false", "ColumnIsPk": "true"},
                    {"ColumnName": "LastName", "ColumnType": "STRING", "ColumnLength": "20"},
                    {"ColumnName": "FirstName", "ColumnType": "STRING", "ColumnLength": "30"},
                    {"ColumnName": "HireDate", "ColumnType": "DATETIME"},
                    {"ColumnName": "OfficeLocation", "ColumnType": "STRING", "ColumnLength": "20"},
                ],
                "TableColumnsTotal": "5",
            },
            {
                "TableName": "department",
                "TablePath": "hr/department/",
                "TableOwner": "hr",
                "TableColumns": [
                    {"ColumnName": "Id", "ColumnType": "INT8", "ColumnNullable": "false", "ColumnIsPk": "true"},
                    {"ColumnName": "DepartmentName", "ColumnType": "STRING", "ColumnLength": "50"},
                ],
                "TableColumnsTotal": "2",
            },
            {
                "TableName": "project",
                "TablePath": "hr/project/",
                "TableOwner": "hr",
                "TableColumns": [
                    {"ColumnName": "Id", "ColumnType": "INT8", "ColumnNullable": "false", "ColumnIsPk": "true"},
                    {"ColumnName": "ProjectName", "ColumnType": "STRING", "ColumnLength": "50"},
                    {"ColumnName": "ProjectDescription", "ColumnType": "STRING", "ColumnLength": "100"},
                ],
                "TableColumnsTotal": "3",
            },
        ],
    }

    return dms.CfnEndpoint(
        stack,
        endpoint_id,
        endpoint_type="source",
        engine_name="s3",
        s3_settings=dms.CfnEndpoint.S3SettingsProperty(
            bucket_name=bucket.bucket_name,
            external_table_definition=json.dumps(table_structure),
            bucket_folder=BUCKET_FOLDER,
            service_access_role_arn=dms_assume_role.role_arn,
            cdc_path=CHANGE_DATA if not full_load else None,
        ),
    )

def create_replication_instance(stack: Stack, vpc: ec2.Vpc, security_group: ec2.SecurityGroup) -> dms.CfnReplicationInstance:
    subnets = vpc.public_subnets
    # Role definitions
    assume_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "dms.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    cdk.aws_iam.CfnRole(
        stack,
        "DmsVpcRole",
        managed_policy_arns=[
            "arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole",
        ],
        assume_role_policy_document=assume_role_policy_document,
        role_name="dms-vpc-role",  # this exact name needs to be set
    )

    replication_subnet_group = dms.CfnReplicationSubnetGroup(
        stack,
        "ReplSubnetGroup",
        replication_subnet_group_description="Replication Subnet Group for DMS",
        subnet_ids=[subnet.subnet_id for subnet in subnets],
    )

    return dms.CfnReplicationInstance(
        stack,
        "replication-instance",
        replication_instance_class="dms.t2.micro",
        allocated_storage=5,
        replication_subnet_group_identifier=replication_subnet_group.ref,
        allow_major_version_upgrade=False,
        auto_minor_version_upgrade=False,
        multi_az=False,
        publicly_accessible=True,
        vpc_security_group_ids=[security_group.security_group_id],
        availability_zone=subnets[0].availability_zone,
    )

def create_replication_task(
    stack: Stack,
    id: str,
    replication_instance: dms.CfnReplicationInstance,
    source: dms.CfnEndpoint,
    target: dms.CfnEndpoint,
    migration_type: str = "cdc",
    table_mappings: dict = None,
    replication_task_settings: dict = None,
) -> dms.CfnReplicationTask:
    if not table_mappings:
        table_mappings = {
            "rules": [
                {
                    "rule-type": "selection",
                    "rule-id": "1",
                    "rule-name": "rule1",
                    "object-locator": {"schema-name": "%", "table-name": "%"},
                    "rule-action": "include",
                }
            ]
        }
    if not replication_task_settings:
        replication_task_settings = {"Logging": {"EnableLogging": True}}

    return dms.CfnReplicationTask(
        stack,
        id,
        replication_task_identifier=id,
        migration_type=migration_type,
        replication_instance_arn=replication_instance.ref,
        source_endpoint_arn=source.ref,
        target_endpoint_arn=target.ref,
        table_mappings=json.dumps(table_mappings),
        replication_task_settings=json.dumps(replication_task_settings),
    )

def create_kinesis_stream(stack: Stack, dms_assume_role: iam.Role) -> kinesis.Stream:
    target_stream = kinesis.Stream(
        stack, "TargetStream", shard_count=1, retention_period=cdk.Duration.hours(24)
    )
    target_stream.grant_read_write(dms_assume_role)
    target_stream.apply_removal_policy(cdk.RemovalPolicy.DESTROY)
    return target_stream

def create_security_group(stack: Stack, vpc: ec2.Vpc) -> ec2.SecurityGroup:
    return ec2.SecurityGroup(
        stack,
        "sg",
        vpc=vpc,
        description="Security group for DMS sample",
        allow_all_outbound=True,
    )

# Secrets Manager functions


def create_s3_secret(stack: Stack, bucket: s3.Bucket) -> secretsmanager.Secret:
    return secretsmanager.Secret(
        stack,
        "s3-access-secret",
        secret_object_value={
            "bucket_name": SecretValue.unsafe_plain_text(bucket.bucket_name),
            "bucket_folder": SecretValue.unsafe_plain_text(os.getenv("BUCKET_FOLDER")),
            "change_data": SecretValue.unsafe_plain_text(os.getenv("CHANGE_DATA")),
        },
    )
