"""
Compile hooks of the extraction pipeline for the dialects of the databases.

A Values clause, which carries the seeds of a cohort, is sent to each database
as one JSON parameter, which the database reads back as rows.
"""
import json
from copy import copy
from textwrap import dedent, indent

from sqlalchemy import Float, String, Text, UnicodeText, bindparam, event
from sqlalchemy.engine import Engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.selectable import Values
from sqlalchemy.types import UserDefinedType


@compiles(Float, "oracle")
def compile_oracle_float(type_, compiler, **kwargs):
    # pylint: disable=unused-argument
    """Compile generic SQLAlchemy Float for Oracle dialect.

    Pandas to_sql maps float64 columns to Float(precision=53).
    Oracle FLOAT types use binary precision instead of decimal precision,
    causing SQLAlchemy 2.0+ to raise an ArgumentError. Defaulting to
    plain 'FLOAT' allows Oracle to use its default binary precision (126).
    """
    return "FLOAT"


class _OracleCLOB(UserDefinedType):
    """Minimal SQLAlchemy type that binds a Python str as Oracle DB_TYPE_CLOB.

    Text() and oracle.CLOB() both resolve to DB_TYPE_NVARCHAR via
    setinputsizes, which still has a size limit. This type returns
    DB_TYPE_CLOB directly so oracledb sends the value as a true LOB,
    allowing the JSON seed list to exceed Oracle's VARCHAR2 4000-byte
    cap without ORA-01460.
    """
    cache_ok = True

    def get_col_spec(self, **kwargs):  # pylint: disable=unused-argument
        """
        Get the column specification for Oracle.

        :param kwargs: Keyword arguments.
        :type kwargs: dict

        :return: Column specification.
        :rtype: str
        """
        return "CLOB"

    def get_dbapi_type(self, dbapi):
        """
        Get the database API type for Oracle.

        :param dbapi: Database API.
        :type dbapi: oracledb

        :return: Database API type.
        :rtype: oracledb.DB_TYPE_CLOB
        """
        return dbapi.DB_TYPE_CLOB

    def _with_collation(self, collation, collation_schema=None):
        """A LOB has no collation, which TypeEngine declares as abstract."""
        raise NotImplementedError("This type does not support collation.")


@event.listens_for(Engine, "do_setinputsizes")
def bind_oracle_seeds_as_lobs(inputsizes, cursor, _statement, parameters,
                              context):
    """Bind the JSON of the seeds in Oracle as a temporary LOB.

    Oracle refuses a string longer than 32767 bytes (ORA-01460), whatever
    the type declared to setinputsizes. A LOB has no limit, and it is made
    here, where the connection is.
    """
    # Only Oracle has the limit
    if context.dialect.name != "oracle":
        return

    # Each parameter of the seeds gets a LOB with its text
    converted = set()
    for bind_param in inputsizes:
        if isinstance(bind_param.type, _OracleCLOB):
            name = context.compiled.bind_names[bind_param]
            # Reused predicates can compile distinct bind objects under
            # one parameter name. Its text must become a LOB only once.
            if name in converted:
                continue
            for params in parameters:
                params[name] = cursor.connection.createlob(
                    inputsizes[bind_param], params[name]
                )
            converted.add(name)


class JsonValues:
    """Compile a Values clause to a query that reads its rows from one JSON
    parameter. A dialect sets the type of the parameter, the definition of
    each column and the query.

    :cvar bind_type: Type of the parameter that carries the JSON.
    :cvar literal_execute: Whether the JSON is in the text of the statement,
                           and not a parameter.
    :cvar template: Query that reads the rows, with the placeholders
                    ``{placeholder}`` of the parameter and ``{columns}`` of
                    the definitions of the columns.
    """
    bind_type = Text
    literal_execute = False
    template = ""

    def column(self, col_idx, col, compiler):
        """Definition of a column of the Values clause in the query.

        :param col_idx: Position of the column in the JSON array of a row.
        :type col_idx: int

        :param col: Column of the Values clause.
        :type col: ColumnClause

        :param compiler: Compiler of the statement.
        :type compiler: SQLCompiler

        :return: SQL definition of the column.
        :rtype: str
        """
        raise NotImplementedError

    @staticmethod
    def type_sql(col_type, compiler):
        """SQL of a type in the dialect."""
        return compiler.dialect.type_compiler_instance.process(col_type)

    def __call__(self, element, compiler, **kwargs):
        # The rows of the Values clause, as a JSON array of arrays
        data = element._data  # pylint: disable=protected-access
        raw_data = data[0] if data else []
        json_str = json.dumps(raw_data, default=str)

        # The JSON is one parameter, whatever the number of rows
        bind_name = f"seed_list_{id(element)}"
        bind_param = bindparam(
            bind_name, value=json_str, type_=self.bind_type(),
            literal_execute=self.literal_execute
        )
        bind_placeholder = compiler.process(bind_param, **kwargs)

        # Each column is read from its position in the array of a row
        columns_spec = []
        for col_idx, col in enumerate(element.columns):
            columns_spec.append(self.column(col_idx, col, compiler))

        # The query of the dialect, which takes the place of the VALUES
        query = self.template.format(
            placeholder=bind_placeholder,
            columns=", ".join(columns_spec)
        )
        return (
            f"(\n{indent(query, '  ')}\n) "
            f"{compiler.preparer.quote(element.name)}"
        )


class OracleValues(JsonValues):
    """Oracle reads the JSON with JSON_TABLE."""
    bind_type = _OracleCLOB
    template = dedent("""
    SELECT jt.*
    FROM JSON_TABLE({placeholder}, '$[*]'
    COLUMNS ({columns})) jt
    """).strip()

    def column(self, col_idx, col, compiler):
        # A string without length has none in Oracle, where JSON_TABLE
        # needs one
        if isinstance(col.type, String) and not col.type.length:
            col_type = "VARCHAR2(4000)"
        else:
            col_type = self.type_sql(col.type, compiler)
        return f"{col.name} {col_type} PATH '$[{col_idx}]'"


class MssqlValues(JsonValues):
    """SQL Server reads the JSON with OPENJSON.

    The JSON is in the text of the statement, since the driver of FreeTDS
    refuses a long string as a parameter (HY090).
    """
    bind_type = UnicodeText
    literal_execute = True
    template = dedent("""
    SELECT jt.*
    FROM OPENJSON({placeholder})
    WITH ({columns}) jt
    """).strip()

    def column(self, col_idx, col, compiler):
        # The collation of the column of the table is not valid in OPENJSON
        col_type = copy(col.type)
        if hasattr(col_type, "collation"):
            col_type.collation = None
        return (
            f"{col.name} {self.type_sql(col_type, compiler)} '$[{col_idx}]'"
        )


class PostgresqlValues(JsonValues):
    """PostgreSQL reads the JSON with jsonb_array_elements."""
    template = dedent("""
    SELECT {columns}
    FROM jsonb_array_elements(CAST({placeholder} AS JSONB)) jt
    """).strip()

    def column(self, col_idx, col, compiler):
        # jsonb_array_elements gives one row for each element, and ->> reads
        # the position of the column as text, which is cast to its type
        return (
            f"CAST(jt.value ->> {col_idx} AS "
            f"{self.type_sql(col.type, compiler)}) AS {col.name}"
        )


# A Values clause is compiled by the class of the dialect of the engine
compiles(Values, "oracle")(OracleValues())
compiles(Values, "mssql")(MssqlValues())
compiles(Values, "postgresql")(PostgresqlValues())
