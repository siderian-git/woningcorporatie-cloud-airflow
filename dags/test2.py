from plugins.ingest.fabric import FabricIngestEngine
from plugins.ingest.postgresql import PostgresqlIngestEngine
from plugins.ingest.write import WriteEngine
from plugins.ingest.config import EMPIRE_CONNECTION_URL,LAKEHOUSE_URL,LAKEHOUSE_S3_ACCESS_KEY,LAKEHOUSE_S3_SECRET_KEY,LAKEHOUSE_S3_BUCKET,POSTGRESQL_CONNECTION_URL

import json

import pendulum

from airflow.sdk import dag, task
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["test2"],
)
def test2():

    @task()
    def get_table(schema_name = "staging", table_name = "eenheid"):

        odbc_conn_string = EMPIRE_CONNECTION_URL
        if odbc_conn_string is None:
            print("Please set the EMPIRE_CONNECTION_URL environment variable to run the test.")
            return

        fabric_ingest_engine = FabricIngestEngine(
            odbc_conn_string=odbc_conn_string,
        )
        source_write_engine = WriteEngine(
            s3_access_key=LAKEHOUSE_S3_ACCESS_KEY,
            s3_secret_key=LAKEHOUSE_S3_SECRET_KEY,
            s3_endpoint=LAKEHOUSE_URL,
            bucket=LAKEHOUSE_S3_BUCKET,
        )
        iceberg_write_engine = WriteEngine(
            hive_uri="thrift://localhost:9083",
            s3_access_key=LAKEHOUSE_S3_ACCESS_KEY,
            s3_secret_key=LAKEHOUSE_S3_SECRET_KEY,
            s3_endpoint=LAKEHOUSE_URL,
        )

        # ingest_table("empire", "[staging].[eenheid]", "id")
        layer_name = "source"
        source_name = "empire"
        schema_name = "staging"

        print("Testing ingest: ",table_name)
        arrow_table = fabric_ingest_engine.ingest_partitioned(
            source=source_name,
            schema=schema_name,
            table=table_name, 
            partitions=6
        )
        if arrow_table is not None:
            # print("Testing write to source layer (arrow dataset)...")
            # source_write_engine.write_arrow_dataset(
            #     layer = layer_name,
            #     source = source_name, 
            #     schema = schema_name,
            #     table_name = table_name, 
            #     arrow_table=arrow_table
            # )
            print("Testing write to iceberg...")
            namespace = "staging"
            iceberg_write_engine.write_to_iceberg(
                namespace=namespace,
                table_name=table_name,
                arrow_table=arrow_table,
            )
    @task()
    def get_postgresql_table(schema_name = "public", table_name = "eenheid"):
        postgresql_ingest_engine = PostgresqlIngestEngine(
            connection_url=POSTGRESQL_CONNECTION_URL,
            batch_size=100000
        )
        iceberg_write_engine = WriteEngine(
            hive_uri="thrift://localhost:9083",
            s3_access_key=LAKEHOUSE_S3_ACCESS_KEY,
            s3_secret_key=LAKEHOUSE_S3_SECRET_KEY,
            s3_endpoint=LAKEHOUSE_URL,
        )

        # ingest_table("empire", "[staging].[eenheid]", "id")
        layer_name = "source"
        source_name = "vera"
        schema_name = "staging"

        print("Testing ingest: ",table_name)
        arrow_table = postgresql_ingest_engine.ingest_partitioned(
            source=source_name,
            schema=schema_name,
            table=table_name,
            partitions=6
        )
        if arrow_table is not None:
            print("Testing write to iceberg...")
            namespace = "staging"
            iceberg_write_engine.write_to_iceberg(
                namespace=namespace,
                table_name=table_name,
                arrow_table=arrow_table,
            )

    list_of_tables = [
        # {"schema": "staging", "table": "eenheid"},
        # {"schema": "staging", "table": "adres"},
        # {"schema": "staging", "table": "gemeente"}, # Leeg
        # {"schema": "staging", "table": "buurt"},
        # {"schema": "staging", "table": "wijk"}, # Leeg
        # {"schema": "staging", "table": "eenheid_cluster"},
        # {"schema": "staging", "table": "cluster"},
        {"schema": "staging", "table": "grootboekmutatie"},
        {"schema": "staging", "table": "grootboekrekening"},
    ]

    for table_info in list_of_tables:
        get_table(schema_name=table_info["schema"], table_name=table_info["table"])

test2()
