"""
Compile hooks of the extraction pipeline for the dialects of the databases.

A Values clause, which carries the seeds of a cohort, is sent to each database
as one JSON parameter, which the database reads back as rows.
"""
import json
from textwrap import dedent

from sqlalchemy import Float, String, bindparam
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


@compiles(Values, "oracle")
def compile_oracle_values(element, compiler, **kwargs):
    """Translate a Values clause to Oracle JSON_TABLE query."""
    columns = element.columns
    raw_data = element._data[0] if element._data else []  # pylint: disable=protected-access  # noqa: E501
    json_str = json.dumps(raw_data)

    bind_name = f"seed_list_{id(element)}"
    bind_param = bindparam(bind_name, value=json_str, type_=_OracleCLOB())
    bind_placeholder = compiler.process(bind_param, **kwargs)

    columns_spec = []
    for col_idx, col in enumerate(columns):
        if isinstance(col.type, String) and not col.type.length:
            col_type = "VARCHAR2(4000)"
        else:
            col_type = compiler.dialect.type_compiler.process(col.type)
        columns_spec.append(f"{col.name} {col_type} PATH '$[{col_idx}]'")

    return dedent(f"""
    (
      SELECT jt.*
      FROM JSON_TABLE({bind_placeholder}, '$[*]'
      COLUMNS ({', '.join(columns_spec)})) jt
    ) {compiler.preparer.quote(element.name)}
    """).strip()
