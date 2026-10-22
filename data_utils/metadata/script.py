"""
SQL script metadata.
"""
from pathlib import Path
from typing import Set

import sqlglot
from more_itertools import sliding_window
from sqlglot import exp
from sqlglot.parser import Parser
from sqlglot.tokens import TokenType


PUBLIC = "public"

# Token types that can be the name of an object
NAMES = Parser.ID_VAR_TOKENS
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
    _creates = {"TABLE", "VIEW", "INDEX"} | _functions

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
        self.full_name = ""
        self.requires: Set[str] = set()

        for statement in sqlglot.parse(sql, read=dialect):
            if statement is not None:
                self._inspect(statement)

        if not self.full_name:
            raise ScriptError(self.path, "it must create one object.")
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
                case (_, TokenType.VAR, TokenType.L_PAREN):
                    names.add(center.text)
                case (first, TokenType.DOT, last) if {first, last} <= NAMES:
                    names.add(f"{left.text}.{right.text}")
        return names

    def _create(self, statement: exp.Create, kind: str):
        """Register the object created by a statement.

        :return: The table node that names the created object, which is not
                 a dependency of the script, or None for an index, since the
                 table of an index is a dependency.
        :rtype: exp.Table
        """
        if self.full_name:
            raise ScriptError(self.path, "it creates more than one object.")

        if kind == "INDEX":
            index = statement.this
            if not index.name:
                raise ScriptError(self.path, "indexes must be named.")
            self.full_name = f"{index.args['table'].db}.{index.name}"
            return None

        if kind in self._functions:
            # Functions are created in the default schema
            function = statement.this.find(exp.Table)
            if function.db:
                raise ScriptError(self.path, FUNCTION_SCHEMA)

            # Overloads are told apart by the types of their parameters
            types = []
            for param in statement.this.expressions:
                if isinstance(param, exp.ColumnDef):
                    param = param.args["kind"]
                types.append(param.sql(self.dialect).lower())
            self.full_name = f"{function.name}({', '.join(types)})"

            return function

        # The table may be wrapped, e.g. by its column definitions
        created = statement.this.find(exp.Table)
        self.full_name = f"{created.db}.{created.name}"
        return created

    def _references(self, statement: exp.Create, created: exp.Table):
        """Collect the tables and functions referenced by a statement.

        :param created: Table node that names the created object.
        :type created: exp.Table
        """
        # Names defined by WITH clauses are not database objects
        ctes = {cte.alias for cte in statement.find_all(exp.CTE)}
        for table in statement.find_all(exp.Table):
            # Functions are not tables, whether created or called in FROM
            if (
                isinstance(table.parent, exp.UserDefinedFunction)
                or isinstance(table.this, exp.Func)
                or (not table.db and table.name in ctes)
            ):
                continue

            if table.db.lower() in ("", PUBLIC):
                raise ScriptError(
                    self.path,
                    f"objects must be named with a schema other than {PUBLIC}."
                )
            if table is not created:
                self.requires.add(f"{table.db}.{table.name}")

        # Calls to functions, which are named without a schema. Built-in
        # functions are known to sqlglot, so only user functions are unknown.
        for call in statement.find_all(exp.Anonymous):
            if isinstance(call.parent, exp.Dot) or (
                isinstance(call.parent, exp.Table) and call.parent.db
            ):
                raise ScriptError(self.path, FUNCTION_SCHEMA)
            self.requires.add(call.name)

    def _inspect(self, statement: exp.Expression):
        """Collect the objects created and referenced by a statement."""
        # Dropping an object does not require it to exist
        if isinstance(statement, exp.Drop):
            return

        # Nothing but the creation of an object is allowed
        if not isinstance(statement, exp.Create):
            raise ScriptError(self.path, "it must create one object.")

        # Only some of the objects that can be created are tracked
        if statement.kind not in self._creates:
            kind = statement.kind.lower()
            raise ScriptError(self.path, f"{kind}s are not supported.")

        created = self._create(statement, statement.kind)

        # The body of a function is quoted text ($$...$$ or a string literal),
        # which sqlglot does not parse, so it is read by its tokens. Anything
        # else is a tree, which is walked.
        body = statement.args.get("expression")
        if isinstance(body, (exp.Heredoc, exp.Literal)):
            self.requires |= self._body_names(body.name)
        else:
            self._references(statement, created)
