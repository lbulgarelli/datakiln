"""
SQL dialects.
"""
from sqlglot.dialects.postgres import Postgres
from sqlglot.tokens import TokenType


class ExtendedPostgres(Postgres):
    """PostgreSQL with the syntax that SQLGlot does not read. An aggregate is
    created like a function, with options in place of a body."""

    class Tokenizer(Postgres.Tokenizer):
        """Tokenizer that reads AGGREGATE as it reads FUNCTION."""

        KEYWORDS = {**Postgres.Tokenizer.KEYWORDS,
                    "AGGREGATE": TokenType.FUNCTION}


# SQLAlchemy dialect names and the same dialects in SQLGlot, by name or class
DIALECT_MAP = {
    "postgresql": ExtendedPostgres,
    "mssql": "tsql"
}
