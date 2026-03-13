"""
Foreign key metadata.
"""
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

    def __init__(self, table, columns, reference, dtypes=None):
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
        self.dtypes = dtypes

        # Set default dtypes
        if dtypes is None:
            self.dtypes = dict.fromkeys(["default"] * len(columns), "TEXT")

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

    def _conditions(self, dialect="default"):
        """Generate comparisons to match each foreign key column.

        :return: Comparisons for each foreign key column.
        :rtype: list[str]

        :param dialect: Dialect of the database.
        :type dialect: str
        """
        conditions = []

        # Zip column pair together along with data type
        column_pair = zip(
            self.columns,
            self.reference.columns,
            self.dtypes
        )

        # Generate one comparison statement for each foreign key column
        for column, reference, dtype in column_pair:
            conditions.append(
                f"  AND {self.table.name}.{column}::{dtype[dialect]} = "
                f"{self.reference.table}.{reference}"
            )

        # Remove AND clause from first comparison
        conditions[0] = conditions[0][5:]

        return conditions

    def join(self, dialect="default"):
        """Generate JOIN statement for the foreign key.

        :return: JOIN statement.
        :rtype: list[str]

        :param dialect: Dialect of the database.
        :type dialect: str
        """
        join = [f"LEFT JOIN {self.reference.schema}.{self.reference.table}"]
        join.extend(self._conditions(dialect=dialect))

        # Add ON clause to first comparison
        join[1] = "  ON" + join[1]

        return join

    def distinct_reference(self, dialect="default"):
        """Filter foreign key values that do not exist in the reference tables,
        allowing null values.

        :return: Comparisons for each foreign key column.
        :rtype: list[str]

        :param dialect: Dialect of the database.
        :type dialect: str
        """
        conditions = ["OR (", "  1 = 1"]

        # Zip column pair together along with data type
        column_pair = zip(
            self.columns,
            self.reference.columns,
            self.dtypes
        )

        # Generate one comparison statement for each foreign key column
        for column, reference, dtype in column_pair:
            conditions.append(
                f"  AND {self.reference.table}.{reference} IS DISTINCT FROM "
                f"{self.table.name}.{column}::{dtype[dialect]}"
            )

        return conditions + [")"]
