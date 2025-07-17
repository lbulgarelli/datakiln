from textwrap import dedent

from dataclasses import dataclass


@dataclass
class ForeignKeyReference:
    """Metadata for table referenced in foreign key.

    :param schema: Schema of foreign table.
    :type schema: str

    :param table: Table referenced by foreign key.
    :type table: str

    :param columns: Columns referenced by foreign key.
    :type columns: str
    """
    schema: str
    table: str
    columns: list


class ForeignKeyMeta():
    """Metadata for foreign key."""

    def __init__(self, table, columns, reference):
        """
        :param table: Table referencing the foreign key.
        :type table: TableMeta

        :param columns: Columns referencing the foreign key.
        :type columns: str

        :param reference: Metadata for referenced table and columns.
        :type reference: dict

        :return: Foreign key metadata.
        :rtype: ForeignKeyMeta
        """
        self.table = table
        self.columns = columns
        self.reference = ForeignKeyReference(**reference)

    def create(self, conn):
        """Create statement.

        :param conn: SQLAlchemy connection or session.
        :type conn: sqlalchemy.Engine

        :return: None
        """
        stmt = dedent("""
        ALTER TABLE {table.schema}.{table.name}
        ADD FOREIGN KEY ({t_columns})
        REFERENCES {reference.schema}.{reference.table} ({r_columns})
        """).format(
            table=self.table,
            t_columns=", ".join(self.columns),
            reference=self.reference,
            r_columns=", ".join(self.reference.columns)
        )
        conn.execute(stmt)

    def _conditions(self):
        """Generate comparisons to match each foreign key column.

        :return: Comparisons for each foreign key column.
        :rtype: list[str]
        """
        conditions = []

        # Generate one comparison statement for each foreign key column
        for column, o_column in zip(self.columns, self.reference.columns):
            conditions.append(
                f"  AND {self.table.name}.{column} = "
                f"{self.reference.table}.{o_column}::TEXT"
            )

        # Remove AND clause from first comparison
        conditions[0] = conditions[0][5:]

        return conditions

    def join(self):
        """Generate JOIN statement for the foreign key.

        :return: JOIN statement.
        :rtype: list[str]
        """
        join = [f"LEFT JOIN {self.reference.schema}.{self.reference.table}"]
        join.extend(self._conditions())

        # Add ON clause to first comparison
        join[1] = "  ON" + join[1]

        return join

    def is_reference_null(self):
        """Check if foreign column is null.

        :return: Comparisons for each foreign key column.
        :rtype: list[str]
        """
        conditions = []

        # Generate one comparison statement for each foreign key column
        for column in self.reference.columns:
            conditions.append(f"AND {self.reference.table}.{column} IS NULL")

        return conditions

