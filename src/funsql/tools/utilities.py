"""This module hosts commonly used utility routines with FunSQL. 
"""

import sqlite3 as sql
from typing import Any

__all__ = ["TableRelations"]


class TableRelations:
    """Keep a cache of all table relationships. Has to be filled in manually."""

    conn: sql.Connection

    def __init__(self):
        self.conn = sql.connect(":memory")
        with self.conn:
            self.conn.execute("DROP TABLE IF EXISTS relations;")
            self.conn.execute(
                "CREATE TABLE relations (TabA TEXT, ColA TEXT, TabB TEXT, ColB TEXT);"
            )

    def add(self, tabA: str, colA: str, tabB: str, colB: str) -> None:
        with self.conn:
            pair = [(tabA, colA, tabB, colB), (tabB, colB, tabA, colA)]
            for values in pair:
                self.conn.execute("INSERT INTO relations VALUES (?, ?, ?, ?);", values)

    def get(self, tabA: str, tabB: str) -> list[Any]:
        return self.conn.execute(
            "SELECT ColA, ColB FROM relations WHERE TabA = ? AND TabB = ?;",
            (tabA, tabB),
        ).fetchall()
