import math
from typing import Optional

import psycopg2
import pyarrow as pa


class PostgresqlIngestEngine:
    def __init__(
        self,
        connection_url: str,
        batch_size: int = 50000,
    ):
        self.conn_string = connection_url
        self.batch_size = batch_size

    # -------------------------
    # Connection
    # -------------------------

    def _connect(self):
        # psycopg3 connection string: "postgresql://user:pass@host:5432/dbname"
        return psycopg2.connect(self.conn_string)

    # -------------------------
    # Metadata helpers
    # -------------------------

    def get_pk_column(self, schema: Optional[str], table: str) -> Optional[str]:
        # Returns first PK column (composite PKs return the first column by position)
        query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_name = %s
          AND tc.table_schema = COALESCE(%s, tc.table_schema)
        ORDER BY kcu.ordinal_position
        LIMIT 1
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (table, schema))
                row = cur.fetchone()
                return row[0] if row else None

    def get_min_max_pk(self, schema: Optional[str], table: str, pk: str):
        full_table = f'"{schema}"."{table}"' if schema else f'"{table}"'
        # pk is an identifier; quote it
        query = f'SELECT MIN("{pk}"), MAX("{pk}") FROM {full_table}'
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchone()

    # -------------------------
    # Core streaming extract
    # -------------------------

    def _stream_query_to_arrow(self, query: str, params=None):
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(query, params or ())

        columns = [desc.name for desc in cursor.description]

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
                    target_schema = batch.schema
                else:
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

    def ingest_full_table(self, source, schema: Optional[str], table: str):
        full_table = f'"{schema}"."{table}"' if schema else f'"{table}"'
        query = f"SELECT * FROM {full_table}"
        arrow_table = self._stream_query_to_arrow(query)
        return arrow_table if arrow_table else None

    def ingest_partitioned(self, source, schema: Optional[str], table: str, partitions: int = 4):
        pk = self.get_pk_column(schema, table)
        if not pk:
            return self.ingest_full_table(source, schema, table)

        min_id, max_id = self.get_min_max_pk(schema, table, pk)
        if min_id is None:
            return None

        step = math.ceil((max_id - min_id) / partitions) if partitions > 0 else (max_id - min_id)

        full_table = f'"{schema}"."{table}"' if schema else f'"{table}"'
        last_arrow_table = None

        for i in range(partitions):
            start = min_id + i * step
            end = min(start + step - 1, max_id)

            query = f'SELECT * FROM {full_table} WHERE "{pk}" BETWEEN %s AND %s'
            last_arrow_table = self._stream_query_to_arrow(query, (start, end))

        return last_arrow_table if last_arrow_table else None