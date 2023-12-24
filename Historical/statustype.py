import json
import os
import json
import boto3
import botocore
import botocore.session as bc
from botocore.client import Config
from typing import NamedTuple

print('Loading function')

secret_name = "redshift!tpcdi-benchmark-admin"  # getting SecretId from Environment varibales
session = boto3.session.Session()
region = session.region_name

# Initializing Botocore client
bc_session = bc.get_session()

session = boto3.Session(
    botocore_session=bc_session,
    region_name=region
)
client_redshift = session.client("redshift-data")
s3_client = session.client("s3")


class StatusType(NamedTuple):
    ST_ID: str
    ST_NAME: str


def extract() -> list[StatusType]:
    response = s3_client.get_object(
        Bucket='tpcdi-benchmark-data',
        Key='Batch1/StatusType.txt',
    )
    csv_string = response['Body'].read().decode('utf-8')
    print(repr(csv_string))
    return [StatusType(*line.split("|")) for line in csv_string.split("\r\n") if line]


def transform(raw_rows):
    return raw_rows


def load(rows: list[StatusType]):
    create_table_stmt = """
    CREATE TABLE IF NOT EXISTS public.statustype (
        st_id character varying(256) ENCODE lzo,
        st_name character varying(65535) ENCODE lzo
    ) DISTSTYLE AUTO;
    """

    insert_stmt = """
    INSERT INTO public.statustype VALUES %s;
    """ % str(tuple(tuple([*st]) for st in rows))

    print(insert_stmt)
    try:
        client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=create_table_stmt)

        response = client_redshift.execute_statement(
            Database='dev', WorkgroupName='tpcdi-benchmark', Sql=insert_stmt)

        print("API successfully executed")

    except botocore.exceptions.ConnectionError as e:
        print("API executed after reestablishing the connection")
        return str(result)

    except Exception as e:
        raise Exception(e)
    return response


def lambda_handler(event, context):
    raw_rows = extract()
    rows = transform(raw_rows)
    response = load(rows)

    return str(response)