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

                # Build rows as dicts in a stable column order
                pyrows = [dict(zip(columns, r)) for r in rows]
                batch = pa.RecordBatch.from_pylist(pyrows)

                if target_schema is None:
                    # Lock schema based on first batch to avoid per-batch inference drift
                    target_schema = batch.schema
                else:
                    # Align to the locked schema (handles int<->float, null columns, etc.)
                    batch = batch.cast(target_schema)

                batches.append(batch)
        finally:
            cursor.close()
            conn.close()

        if not batches:
            return None

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

        for i in range(partitions):
            start = min_id + i * step
            end = min(start + step - 1, max_id)

            query = f"""
            SELECT *
            FROM {full_table}
            WHERE {pk} BETWEEN {start} AND {end}
            """

            arrow_table = self._stream_query_to_arrow(query)

        if arrow_table:
            return arrow_table
        else:
            return None