from __future__ import annotations

from trino.dbapi import connect

from settings import Settings


class TrinoSqlExecutor:
    name = "trino"

    def execute(self, sql: str, *, settings: Settings):
        t = settings.trino
        try:
            conn = connect(
                host=t.host,
                port=t.port,
                user=t.user,
                http_scheme=t.http_scheme,
                catalog=t.catalog,
                schema=t.schema_name,
            )
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in (cur.description or [])]
            return cols, rows
        except Exception as e:
            raise Exception(f"Trino execution failed: {e}") from e