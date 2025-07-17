from textwrap import dedent


class ColumnMeta():
    """Metadata for column."""

    def __init__(self, name, dtype, description=None, required=False,
                 rename=None, transform=False):
        # pylint: disable=too-many-arguments, too-many-positional-arguments
        """
        :param name: Name of the column.
        :type name: str

        :param dtype: Type of the column for each dialect.
        :type dtype: dict

        :param description: Description of the column.
        :type description: str

        :param required: If True, column can't be NULL.
        :type required: bool

        :param rename: Name to be used in the database.
        :type rename: str

        :param rename: Specifies if the column is derived/created in the
                       transform step.
                       (This means the column is not present and will not be
                       retrieved from the source).
        :type rename: bool

        :return: Column metadata.
        :rtype: ColumnMeta
        """
        self.name = name
        self.dtype = dtype
        self.rename = rename or name
        self.description = description
        self.required = required
        self.transform = transform

    def __str__(self):
        return self.rename

    def __iter__(self):
        for item in self.__dict__.items():
            if item[0].startswith("_"):
                continue
            yield item

    def resolve_na(self, conn, schema, name, na_values):
        """Enforce column data type.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :param schema: Table schema.
        :type schema: str

        :param name: Table name.
        :type name: str

        :param na_values: Values to consider as NULL.
        :type columns: set[str]

        :return: None
        """
        stmt = dedent("""
        UPDATE __{schema}.{table}
        SET {name} = NULL
        WHERE {name} IN (
            '{na_values}'
        )""").format(
            schema=schema,
            table=name,
            name=self.rename,
            na_values="'\n  , '".join(na_values)
        )
        conn.execute(stmt)

    def enforce_type(self, conn, schema, name):
        """Resolve NULL values

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :param schema: Table schema.
        :type schema: str

        :param name: Table name.
        :type name: str

        :return: None
        """
        # Get database dialect from connection
        dialect = conn.bind.dialect.name

        stmt = dedent(f"""
        ALTER TABLE {schema}.{name}
        ALTER COLUMN {self.rename} TYPE {self.dtype[dialect]}
        USING {self.rename}::{self.dtype[dialect]}
        """)
        conn.execute(stmt)

    def enforce_required(self, conn, schema, name):
        """Enforce required column values.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :param schema: Table schema.
        :type schema: str

        :param name: Table name.
        :type name: str

        :return: None
        """
        if not self.required:
            return

        stmt = dedent(f"""
        ALTER TABLE {schema}.{name}
        ALTER COLUMN {self.rename} SET NOT NULL
        """)
        conn.execute(stmt)

    def comment(self, conn, schema, name):
        """Add column description as a column comment.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :param schema: Table schema.
        :type schema: str

        :param name: Table name.
        :type name: str

        :return: None
        """
        if self.description is None:
            return

        stmt = dedent(f"""
        COMMENT ON COLUMN {schema}.{name}.{self.rename} IS '{self.description}'
        """)
        conn.execute(stmt)
