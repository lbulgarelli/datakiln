"""
SQL script metadata.
"""
from pathlib import Path
from typing import Optional, Set

import sqlglot
from sqlglot import exp
from sqlglot.tokens import Token, TokenType


class ScriptMeta:
    # pylint: disable=too-few-public-methods
    """SQL script and the database objects it creates and uses."""

    # Statement kinds that create an object other statements can reference
    # (materialized views are parsed as views)
    _creates = {"TABLE", "VIEW", "FUNCTION", "PROCEDURE"}

    # Function and procedure bodies are text, so the objects they use are
    # found by looking for schema-qualified names in their tokens
    _body = {"FUNCTION", "PROCEDURE"}

    def __init__(self, path: Path, sql: str, dialect: str = "postgres"):
        """
        :param path: Path of the script.
        :type path: Path

        :param sql: Content of the script.
        :type sql: str

        :param dialect: SQLGlot dialect of the script.
        :type dialect: str
        """
        self.path = Path(path)
        self.sql = sql
        self.dialect = dialect
        self.full_name: Optional[str] = None
        self.requires: Set[str] = set()

        for statement in sqlglot.parse(sql, read=dialect):
            if statement is not None:
                self._inspect(statement)
        self.requires.discard(self.full_name)

    @classmethod
    def from_file(cls, path: Path, directory: Path) -> 'ScriptMeta':
        """Load script metadata from a SQL file.

        :param path: Path of the script.
        :type path: Path

        :param directory: Directory the script path is relative to.
        :type directory: Path

        :return: Script metadata.
        :rtype: ScriptMeta
        """
        with open(path, "r", encoding="utf-8") as file:
            return cls(path.relative_to(directory), file.read())

    @staticmethod
    def _full_name(table: exp.Table) -> str:
        """Returns schema.table, or an empty string if unqualified.

        Functions called in a FROM clause are tables whose name is a call.
        """
        name = table.this.name
        return f"{table.db}.{name}" if table.db and name else ""

    @staticmethod
    def _is_name(token: Token) -> bool:
        """Whether a token is an identifier, quoted or not."""
        if token.token_type == TokenType.IDENTIFIER:
            return True
        return (
            token.text.isidentifier()
            and not token.token_type.name.endswith("STRING")
        )

    def _qualified_names(self, text: str) -> Set[str]:
        """Returns the schema.name pairs found in a piece of SQL text.

        Comments and string literals are not tokens of names, so they are
        skipped.
        """
        names = set()
        tokens = sqlglot.tokenize(text, read=self.dialect)
        for left, dot, right in zip(tokens, tokens[1:], tokens[2:]):
            if (
                dot.token_type == TokenType.DOT
                and self._is_name(left)
                and self._is_name(right)
            ):
                names.add(f"{left.text}.{right.text}")
        return names

    def _inspect(self, statement: exp.Expression):
        """Collect the objects created and referenced by a statement."""
        # Dropping an object does not require it to exist
        if isinstance(statement, exp.Drop):
            return

        created = None
        if isinstance(statement, exp.Create):
            kind = (statement.kind or "").upper()
            if kind in self._creates:
                # The table may be wrapped, e.g. by its column definitions
                if self.full_name:
                    raise ValueError(
                        f"Script '{self.path}' creates more than one object."
                    )
                created = statement.this.find(exp.Table)
                self.full_name = self._full_name(created) or None

                body = statement.args.get("expression")
                if kind in self._body and isinstance(
                    body, (exp.Heredoc, exp.Literal)
                ):
                    self.requires |= self._qualified_names(body.name)

        # Names defined by WITH clauses are not database objects
        ctes = {cte.alias for cte in statement.find_all(exp.CTE)}
        for table in statement.find_all(exp.Table):
            if table is created or (not table.db and table.name in ctes):
                continue
            if name := self._full_name(table):
                self.requires.add(name)

        # Schema-qualified function calls
        for call in statement.find_all(exp.Dot):
            if (
                isinstance(call.this, exp.Identifier)
                and isinstance(call.expression, exp.Anonymous)
            ):
                self.requires.add(f"{call.this.name}.{call.expression.name}")
