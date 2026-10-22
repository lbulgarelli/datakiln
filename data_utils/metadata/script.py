"""
SQL script metadata.
"""
import re
from pathlib import Path
from typing import List, Set

import sqlglot
from decouple import config
from more_itertools import sliding_window
from sqlglot import exp
from sqlglot.errors import ParseError
from sqlglot.parser import Parser
from sqlglot.tokens import TokenType

from data_utils.metadata.dialect import DIALECT_MAP


PUBLIC = "public"
FUNCTION_SCHEMA = (
    "functions and procedures cannot specify a schema, they use the default "
    "one."
)


class ScriptError(ValueError):
    """Error in a script."""

    def __init__(self, path: Path, message: str):
        super().__init__(f"Script '{path}': {message}")


class ScriptMeta:
    # pylint: disable=too-few-public-methods
    """SQL script and the database objects it creates and uses."""

    _functions = {"FUNCTION", "PROCEDURE"}

    # Statement kinds that create an object other statements can reference
    # (materialized views are parsed as views)
    _creates = {"TABLE", "VIEW", "INDEX", "TYPE", "SEQUENCE"} | _functions

    # Tokens after which sqlglot makes the rest of the statement one string
    _commands = {TokenType.COMMAND, TokenType.EXECUTE}

    def __init__(self, path: Path, sql: str):
        """
        :param path: Path of the script.
        :type path: Path

        :param sql: Content of the script.
        :type sql: str
        """
        # The dialect of the database is set in the environment (DB_DIALECT)
        dialect = config("DB_DIALECT", "postgresql")

        self.path = Path(path)
        self.sql = sql
        self.dialect = DIALECT_MAP.get(dialect, dialect)
        self.full_name = ""
        self.requires: Set[str] = set()

        for statement in self._parse(sql):
            if statement is not None:
                self._inspect(statement)

        if not self.full_name:
            raise ScriptError(self.path, "it must create one object.")
        self.requires.discard(self.full_name)

    @classmethod
    def from_sql(cls, path: Path, directory: Path) -> 'ScriptMeta':
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

    def _parse(self, sql: str) -> List[exp.Expression]:
        """Parse SQL, reporting the syntax that sqlglot cannot parse.

        The SQL is read in lowercase, so that all the names are, since the
        database folds unquoted names. It is only read, not executed.
        """
        try:
            return sqlglot.parse(sql.lower(), read=self.dialect)
        except ParseError as error:
            message = "it has unsupported syntax."
            raise ScriptError(self.path, message) from error

    def _body_names(self, text: str) -> Set[str]:
        """Returns the objects used in the body of a function: schema.name
        pairs and the names of called functions.

        Comments and string literals are not tokens of names, so they are
        skipped. Built-in functions and words that look like calls, such as
        the statements of a procedural language, are returned too and match
        no object.
        """
        names = set()
        tokens = sqlglot.tokenize(text, read=self.dialect)

        for left, center, right in sliding_window(tokens, 3):
            match (left.token_type, center.token_type, right.token_type):
                # A dot before the name of a call means it names a schema
                case (TokenType.DOT, TokenType.VAR, TokenType.L_PAREN):
                    raise ScriptError(self.path, FUNCTION_SCHEMA)
                case (TokenType.VAR, TokenType.L_PAREN, _):
                    names.add(left.text)
                # What follows CALL, DECLARE... or EXECUTE is one string token
                case (cmd, TokenType.STRING, _) if cmd in self._commands:
                    names |= self._body_names(center.text)
                case (l, TokenType.DOT, r) if {l, r} <= Parser.ID_VAR_TOKENS:
                    names.add(f"{left.text}.{right.text}")
        return names

    def _create(self, statement: exp.Create, kind: str):
        """Register the object created by a statement."""
        if self.full_name:
            raise ScriptError(self.path, "it creates more than one object.")

        if kind == "INDEX":
            index = statement.this
            if not index.name:
                raise ScriptError(self.path, "indexes must be named.")
            self.full_name = f"{index.args['table'].db}.{index.name}"
            return

        if kind in self._functions:
            # Functions are created in the default schema
            function = statement.this.find(exp.Table)
            if function.db:
                raise ScriptError(self.path, FUNCTION_SCHEMA)

            # Overloads are told apart by the types of their parameters, but
            # not of the OUT ones, which are results
            types = []
            for param in statement.this.expressions:
                if isinstance(param, exp.ColumnDef):
                    mode = param.find(exp.InOutColumnConstraint)
                    if mode and mode.sql() == "OUT":
                        continue
                    param = param.args["kind"]
                types.append(param.sql(self.dialect).lower())
            self.full_name = f"{function.name}({', '.join(types)})"
            return

        # The table may be wrapped, e.g. by its column definitions
        table = statement.this.find(exp.Table)
        self.full_name = f"{table.db}.{table.name}"

    def _references(self, statement: exp.Create):
        """Collect the tables and functions referenced by a statement."""
        ctes = {cte.alias for cte in statement.find_all(exp.CTE)}
        for table in statement.find_all(exp.Table):
            # A function called in FROM has no name, and a name without a
            # schema may be defined by a WITH clause
            if not table.name or (not table.db and table.name in ctes):
                continue

            if table.db in ("", PUBLIC):
                raise ScriptError(self.path, "it needs a non-public schema.")
            self.requires.add(f"{table.db}.{table.name}")

        # Calls to functions, which are named without a schema. Built-in
        # functions are known to sqlglot, so only user functions are unknown.
        for call in statement.find_all(exp.Anonymous):
            parent = call.parent
            if isinstance(parent, exp.Dot) or getattr(parent, "db", ""):
                raise ScriptError(self.path, FUNCTION_SCHEMA)
            self.requires.add(call.name)

    def _inspect(self, statement: exp.Expression):
        """Collect the objects created and referenced by a statement."""
        # Dropping an object does not require it to exist
        if isinstance(statement, exp.Drop):
            return

        # A definition that sqlglot cannot parse is kept as text
        if isinstance(statement, exp.Command):
            # The signature ends at the last ")" before the first AS, and the
            # expression is the literal that follows AS, up to its closing
            # $ or ', which leaves out any option that comes after it. What is
            # between them has the return type and the options.
            pattern = r"(.*?\))([^)]*?)\bAS\b\s*(.*[$'])"
            found = re.match(pattern, statement.expression, re.I | re.S)
            if not found:
                raise ScriptError(self.path, "it has unsupported syntax.")

            # What is left between them is what sqlglot cannot parse
            signature, between, expression = found.groups()
            self.requires |= self._body_names(between)
            sql = f"CREATE {signature} AS {expression}"
            statement = self._parse(sql)[0]

            # If it still cannot be parsed, the rest was not what stopped it
            if isinstance(statement, exp.Command):
                raise ScriptError(self.path, "it has unsupported syntax.")

        # Nothing but the creation of an object is allowed
        if not isinstance(statement, exp.Create):
            raise ScriptError(self.path, "it must create one object.")

        # Only some of the objects that can be created are tracked
        if statement.kind not in self._creates:
            kind = statement.kind.lower()
            raise ScriptError(self.path, f"{kind}s are not supported.")

        self._create(statement, statement.kind)

        # The type of a parameter or of the result can be a table
        for datatype in statement.find_all(exp.DataType):
            kind = datatype.args.get("kind")
            if isinstance(kind, exp.Dot):
                self.requires.add(f"{kind.this.name}.{kind.expression.name}")

        # The body of a function is quoted text ($$...$$ or a string literal),
        # which sqlglot does not parse, so it is read by its tokens. Anything
        # else is a tree, which is walked.
        body = statement.args.get("expression")
        if isinstance(body, (exp.Heredoc, exp.Literal)):
            self.requires |= self._body_names(body.name)
        elif statement.kind in self._functions:
            raise ScriptError(self.path, "it has unsupported syntax.")
        else:
            self._references(statement)
