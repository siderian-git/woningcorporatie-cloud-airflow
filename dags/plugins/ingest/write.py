import warnings
from datetime import date
import time
# Pyiceberg imports
from pyiceberg.catalog.hive import HiveCatalog,UNPARTITIONED_PARTITION_SPEC
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.schema import assign_fresh_schema_ids
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.expressions import EqualTo
from pyiceberg.table import Table,ALWAYS_TRUE
import pyarrow as pa
# Pyarrow
import pyarrow.dataset as ds
import pyarrow.fs as fs

class WriteEngine:

    def __init__(
        self,
        hive_uri: str = "thrift://hive-metastore.hive.svc.cluster.local:9083", # Local after portforward: "thrift://localhost:9083"
        s3_endpoint: str = None,
        s3_access_key: str = None,
        s3_secret_key: str = None,
        bucket: str = "lakehouse",
        max_rows_per_file: int = 500000,
    ):
        self.hive_uri = hive_uri
        self.s3_endpoint = s3_endpoint
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.bucket = bucket
        self.max_rows_per_file = max_rows_per_file
        self.s3 = None
        if s3_endpoint is not None and s3_access_key is not None  and s3_secret_key is not None :
            self.s3 = fs.S3FileSystem(
                endpoint_override=s3_endpoint,
                access_key=s3_access_key,
                secret_key=s3_secret_key,
            )



    # -------------------------
    # Write to Iceberg table
    # -------------------------

    def write_to_iceberg(
        self,
        namespace,
        table_name,
        batch_iter,
        partition_field: str | None = None,
    ):
        def _cast_null_columns(table: pa.Table, *, null_fallback: pa.DataType = pa.string()) -> pa.Table:
            schema = table.schema
            null_fields = [f for f in schema if pa.types.is_null(f.type)]
            if not null_fields:
                return table

            out = table
            for f in null_fields:
                idx = out.schema.get_field_index(f.name)
                arr = pa.array([None] * out.num_rows, type=null_fallback)
                out = out.set_column(idx, f.name, arr)
            return out


        catalog = HiveCatalog(
            name="iceberg",
            uri=self.hive_uri,
            **{
                "s3.endpoint": self.s3_endpoint,
                "s3.access-key-id": self.s3_access_key,
                "s3.secret-access-key": self.s3_secret_key,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
                "s3.region": "us-east-1",
            },
        )

        identifier = f"{namespace}.{table_name}"

        # Ensure namespace exists
        try:
            catalog.create_namespace(namespace)
        except Exception:
            pass

        # Drop old table (full load pattern)
        try:
            catalog.purge_table(identifier)
            print(f"Purged existing table {identifier}")
        except Exception:
            pass

        first_batch = next(batch_iter, None)
        if first_batch is None:
            print("No data to ingest")
            return

        # Lock the Arrow schema we will append with (based on first batch)
        locked_arrow_schema = first_batch.schema

        first_table = _cast_null_columns(pa.Table.from_batches([first_batch]))

        schema_without_ids = _pyarrow_to_schema_without_ids(first_table.schema)
        schema_with_ids = assign_fresh_schema_ids(schema_without_ids)

        partition_spec = UNPARTITIONED_PARTITION_SPEC

        if partition_field:
            field_id = schema_with_ids.find_field(partition_field).field_id
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=field_id,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name=partition_field,
                )
            )

        table = catalog.create_table(
            identifier,
            schema=schema_with_ids,
            partition_spec=partition_spec,
        )

        print(f"Created Iceberg table {identifier}")

        # Append first chunk
        table.append(first_table)

        # Append remaining batches one-by-one (streaming without RecordBatchReader)
        appended = first_table.num_rows
        for batch in batch_iter:
            if batch.schema != locked_arrow_schema:
                batch = batch.cast(locked_arrow_schema)

            chunk = _cast_null_columns(pa.Table.from_batches([batch]))
            table.append(chunk)
            appended += chunk.num_rows

        print(f"Finished streaming append to Iceberg (rows appended: {appended})")
            
    # -------------------------
    # Write to S3 dataset
    # -------------------------

    def write_arrow_dataset(self, layer : str = "source", source: str = None, schema : str = None, table_name : str = None, arrow_table : pa.Table = None):
        if arrow_table is None:
            print("No data to write.")
            return
    
        base_dir = f"{self.bucket}/{layer}/{source}/{schema}/{table_name}/"
        
        file_options = ds.ParquetFileFormat().make_write_options(compression='snappy')

        # Ensure Arrow constraint: max_rows_per_group <= max_rows_per_file
        max_rows_per_file = int(self.max_rows_per_file)
        max_rows_per_group = max_rows_per_file  # one row-group per file (safe default)

        total_rows = arrow_table.num_rows
        total_bytes = arrow_table.nbytes  # uncompressed, in-memory size (approx)
        print(f"Writing dataset to S3: {base_dir} with {total_rows} rows...")

        t0 = time.perf_counter()
        ds.write_dataset(
            arrow_table,
            base_dir=base_dir,
            filesystem=self.s3,
            format="parquet",
            max_rows_per_file=max_rows_per_file,
            max_rows_per_group=max_rows_per_group,
            file_options=file_options,
            existing_data_behavior="overwrite_or_ignore",
            create_dir=False,
            use_threads=True,
        )
        dt = time.perf_counter() - t0

        rows_per_s = (total_rows / dt) if dt > 0 else float("inf")
        mib_per_s = ((total_bytes / (1024 * 1024)) / dt) if dt > 0 else float("inf")
        print(f"Wrote dataset in {dt:.3f}s ({rows_per_s:,.0f} rows/s, ~{mib_per_s:,.1f} MiB/s raw)")

    def test_create_namespace(self,namespace):

        TARGET_NAMESPACE = namespace

        catalog = HiveCatalog(
            name="iceberg",
            uri=self.hive_uri, # "thrift://hive-metastore.hive.svc.cluster.local:9083",
            **{
                "s3.endpoint": self.s3_endpoint,
                "s3.access-key-id": self.s3_access_key,
                "s3.secret-access-key": self.s3_secret_key,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
                "s3.region": "us-east-1",  # required by some S3-compatible storage, even if not used
            }
        )

        catalog.create_namespace(TARGET_NAMESPACE)

    def write_arrow_batches(self, layer, source, schema, table_name, batch_iter):

        base_dir = f"{self.bucket}/{layer}/{source}/{schema}/{table_name}/"

        file_options = ds.ParquetFileFormat().make_write_options(compression="snappy")

        writer = None

        for batch in batch_iter:

            table = pa.Table.from_batches([batch])

            ds.write_dataset(
                table,
                base_dir=base_dir,
                filesystem=self.s3,
                format="parquet",
                max_rows_per_file=self.max_rows_per_file,
                file_options=file_options,
                existing_data_behavior="overwrite_or_ignore",
                create_dir=False,
                use_threads=True,
            )            