"""
Column metadata.
"""
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

    def typed(self, dialect):
        """Name of column followed by dialect-specific type.

        :param dialect: Dialect of the database.
        :type dialect: str

        :return: Typed column name.
        :rtype: str
        """
        typed = f"{self.rename} {self.dtype[dialect]}"

        if self.required:
            typed += " NOT NULL"

        return typed

    def resolve_na(self, session, schema, name, na_values, **kwargs):
        """Enforce column data type.

        :param session: Database Session.
        :type session: datakiln.metadata.database.Session

        :param schema: Table schema.
        :type schema: str

        :param name: Table name.
        :type name: str

        :param na_values: Values to consider as NULL.
        :type columns: set[str]

        :return: None
        """
        stmt = dedent("""
        UPDATE _{schema}.{table}
        SET {name} = NULL
        WHERE {name} IN (
            '{na_values}'
        )""").format(
            schema=schema,
            table=name,
            name=self.rename,
            na_values="'\n  , '".join(na_values)
        )
        session.execute(stmt, **kwargs)

    def comment(self, session, schema, name, **kwargs):
        """Add column description as a column comment.

        :param session: Database Session.
        :type session: datakiln.metadata.database.Session

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
        session.execute(stmt, **kwargs)
