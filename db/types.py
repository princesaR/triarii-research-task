"""Column types shared by both databases."""

from sqlalchemy import BigInteger, Integer

# BIGINT on Postgres, INTEGER on SQLite (only INTEGER PRIMARY KEY autoincrements there).
BigId = BigInteger().with_variant(Integer, "sqlite")
