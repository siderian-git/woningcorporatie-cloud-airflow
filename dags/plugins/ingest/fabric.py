import pyodbc
import pyarrow as pa
import math


class FabricIngestEngine:

    def __init__(
        self,
        odbc_conn_string: str,
        batch_size: int = 50000,
    ):
        self.conn_string = odbc_conn_string
        self.batch_size = batch_size

    # -------------------------
    # Connection
    # -------------------------

    def _connect(self):
        print("Connecting to MS SQL Server with ODBC connection string: ", self.conn_string)
        return pyodbc.connect(self.conn_string)

    # -------------------------
    # Metadata helpers
    # -------------------------

    def get_pk_column(self, schema, table):
        check_schema = f" AND TABLE_SCHEMA = '{schema}'" if schema is not None else ""
        query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
        {check_schema}
        AND TABLE_NAME = '{table}'
        """

        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query)
            row = cur.fetchone()
            return row[0] if row else None

    def get_min_max_pk(self, schema, table, pk):
        query = f"SELECT MIN({pk}), MAX({pk}) FROM {table}"
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query)
            return cur.fetchone()

    # -------------------------
    # Core streaming extract
    # -------------------------

    def _promote_decimal_fields(self, schema: pa.Schema, *, precision: int = 38) -> pa.Schema:
        fields = []
        for f in schema:
            if pa.types.is_decimal(f.type):
                # Keep scale, widen precision
                fields.append(pa.field(f.name, pa.decimal128(precision, f.type.scale), nullable=f.nullable, metadata=f.metadata))
            else:
                fields.append(f)
        return pa.schema(fields, metadata=schema.metadata)

    def _promote_null_fields(self, locked: pa.Schema, incoming: pa.Schema, *, null_fallback: pa.DataType = pa.string()) -> pa.Schema:
        """
        If the locked schema has columns typed as null (because first batch was all NULL),
        promote them to the incoming type (or a fallback) so later casts succeed.
        """
        incoming_by_name = {f.name: f for f in incoming}
        out_fields = []
        changed = False

        for f in locked:
            if pa.types.is_null(f.type):
                inf = incoming_by_name.get(f.name)
                new_type = inf.type if inf is not None and not pa.types.is_null(inf.type) else null_fallback
                out_fields.append(pa.field(f.name, new_type, nullable=True, metadata=f.metadata))
                changed = True
            else:
                out_fields.append(f)

        return pa.schema(out_fields, metadata=locked.metadata) if changed else locked

    def _stream_query_to_arrow(self, query):
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(query)

        columns = [col[0] for col in cursor.description]

        batches: list[pa.RecordBatch] = []
        target_schema: pa.Schema | None = None

        try:
            while True:
                rows = cursor.fetchmany(self.batch_size)
                if not rows:
                    break

                pyrows = [dict(zip(columns, r)) for r in rows]
                batch = pa.RecordBatch.from_pylist(pyrows)

                if target_schema is None:
                    # Start with the first batch schema, but immediately widen decimals to avoid later overflow.
                    target_schema = self._promote_decimal_fields(batch.schema, precision=38)
                    # Also avoid locking in null-typed columns.
                    target_schema = self._promote_null_fields(target_schema, batch.schema, null_fallback=pa.string())
                    batch = batch.cast(target_schema)
                else:
                    # If locked schema has null columns, promote to incoming types (or fallback)
                    target_schema = self._promote_null_fields(target_schema, batch.schema, null_fallback=pa.string())
                    # Always keep decimals wide enough
                    target_schema = self._promote_decimal_fields(target_schema, precision=38)

                    try:
                        batch = batch.cast(target_schema)
                    except pa.ArrowInvalid as e:
                        if "Decimal value does not fit in precision" in str(e):
                            target_schema = self._promote_decimal_fields(target_schema, precision=38)
                            batch = batch.cast(target_schema)
                        else:
                            raise

                batches.append(batch)
        finally:
            cursor.close()
            conn.close()

        if not batches:
            return None

        # Early batches may have been cast to an older schema; unify all to final schema.
        if target_schema is not None:
            batches = [b.cast(target_schema) if b.schema != target_schema else b for b in batches]

        return pa.Table.from_batches(batches, schema=target_schema)
    
    # -------------------------
    # Public API
    # -------------------------

    def ingest_full_table(self, source, schema, table):

        # fix schema and table names for MS SQL Server -> include brackets if not already present
        if schema is not None:
            if not schema.startswith('['):
                schema = f'[{schema}]'
            if not schema.endswith(']'):
                schema = f'{schema}]'
        if not table.startswith('['):
            table = f'[{table}]'
        if not table.endswith(']'):
            table = f'{table}]'
        full_table = f"{schema}.{table}" if schema is not None else table

        query = f"SELECT * FROM {full_table}"
        arrow_table = self._stream_query_to_arrow(query)

        if arrow_table:
            return arrow_table
        else:
            return None


    def ingest_partitioned(self, source, schema, table, partitions=4):
        pk = self.get_pk_column(schema,table)

        if not pk:
            # fallback
            print(f"No primary key found for table {table}, ingesting full table without partitioning.")
            arrow_table = self.ingest_full_table(source, schema, table)
            return arrow_table

        # fix schema and table names for MS SQL Server -> include brackets if not already present
        if schema is not None:
            if not schema.startswith('['):
                schema = f'[{schema}]'
            if not schema.endswith(']'):
                schema = f'{schema}]'
        if not table.startswith('['):
            table = f'[{table}]'
        if not table.endswith(']'):
            table = f'{table}]'
        full_table = f"{schema}.{table}" if schema is not None else table

        if not pk.startswith('['):
            pk = f'[{pk}]'
        if not pk.endswith(']'):
            pk = f'{pk}]'

        min_id, max_id = self.get_min_max_pk(table, pk)

        if min_id is None:
            return

        step = math.ceil((max_id - min_id) / partitions)

        print(f"Partitioning table {full_table} on PK {pk} with range {min_id} - {max_id} into {partitions} partitions (step size: {step})")
        for i in range(partitions):
            start = min_id + i * step
            end = min(start + step - 1, max_id)

            query = f"""
            SELECT *
            FROM {full_table}
            WHERE {pk} BETWEEN {start} AND {end}
            """

            print(f"Ingesting partition {i+1}/{partitions} for table {full_table}: {pk} BETWEEN {start} AND {end}")
            arrow_table = self._stream_query_to_arrow(query)

        if arrow_table:
            return arrow_table
        else:
            return None