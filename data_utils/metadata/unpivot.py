import re
from textwrap import dedent
from collections import defaultdict

from data_utils.metadata.column import ColumnMeta


class UnpivotColumnMeta(ColumnMeta):
    """Metadata for columns that are unpivoting targets."""
    def __init__(self, unpivot, regex=None, order=1000, **kwargs):
        r"""
        :param unpivot: Metadata of this column's unpivot.
        :type unpivot: UnpivotMeta

        :param regex: Pattern for target column.
        :type regex: str

        :param order: Position of the column in the target table.
        :type order: int

        :param \**kwargs: `ColumnMeta` attributes.
        :type \**kwargs: dict

        :return: Unpivot column metadata.
        :rtype: UnpivotColumnMeta
        """
        self.unpivot = unpivot
        self.regex = regex
        self.order = order

        super().__init__(**kwargs)

    def value(self, column):
        """Value for the column to be unpivoted.

        :param column: Name of the column to be unpivoted.
        :type column: dict

        :return: Unpivot value.
        :rtype: str
        """
        match = re.search(self.regex, column)
        if self.regex and not match:
            return "NULL"
        return f"'{''.join(match.groups())}'"


class UnpivotValueMeta(ColumnMeta):
    """Metadata for columns that will store unpivoted values."""
    def __init__(self, unpivot, columns, **kwargs):
        r"""
        :param unpivot: Metadata of this column's unpivot.
        :type unpivot: UnpivotMeta

        :param columns: Columns storing the values.
        :type columns: list[str]

        :return: Column metadata.
        :rtype: ColumnMeta
        """
        self.unpivot = unpivot
        super().__init__(**kwargs)

        self.columns = []
        for column in columns:
            column_meta = ColumnMeta(
                dtype={
                    "postgresql": "TEXT"
                },
                name=column
            )
            self.columns.append(column_meta)


class UnpivotMeta():
    """Metadata for unpivoting columns."""
    def __init__(self, alias, target_columns, target_values, **kwargs):
        r"""
        :param alias: Alias for the LATERAL join.
        :type alias: str

        :param target_columns: Metadata for target columns.
        :type target_columns: list[str]

        :param target_values: Metadata for target value column.
        :type target_values: dict

        :param \**kwargs: `UnpivotColumnMeta` attributes.
        :type \**kwargs: dict

        :return: Unpivot metadata.
        :rtype: UnpivotMeta
        """
        self.alias = alias

        # Target columns
        self.target_columns = []
        for target in target_columns:
            target_meta = UnpivotColumnMeta(self, **target, **kwargs)
            self.target_columns.append(target_meta)

        # Target value
        self.target_values = []
        for value in target_values:
            value_meta = UnpivotValueMeta(self, **value, **kwargs)
            self.target_values.append(value_meta)

    def columns(self, unpivot=True):
        """Unpivot columns.

        :return: Pivot columns.
        :rtype: list[ColumnMeta]
        """
        # List target columns
        if unpivot:
            return self.target_columns + self.target_values

        # List original columns
        columns = []
        for value in self.target_values:
            columns.extend(value.columns)
        return columns

    def values(self):
        """SQL with VALUES to use in LATERAL JOIN.

        :return: Values.
        :rtype: list[str]
        """
        # Generate expressions
        exp_dict = defaultdict(list)
        for idx, value in enumerate(self.target_values):
            value_columns = []
            # Retrieve name for columns being pivoted
            for pivot in value.columns:
                value_columns.append(pivot.name)

            values = []
            # Extract values from columns
            for target in self.target_columns:
                values.append(list(map(target.value, value_columns)))

            # Add columns to list for each value found
            expression = dict(zip(zip(*values), value_columns))
            keys = exp_dict.keys() | expression.keys()
            for key in keys:
                value = expression.get(key, 'NULL')
                # Prepend NULL if value not seen before
                if idx and key not in exp_dict:
                    count = len(list(exp_dict.items())[0]) + 1
                    exp_dict[key].extend(['NULL'] * count)
                exp_dict[key].append(value)

        # Parse unpivoting columns to string
        expressions = []
        for vals, v_cols in exp_dict.items():
            expression = list(vals) + v_cols
            expression = f"({', '.join(expression)})"
            expressions.append(expression)
        return expressions

    def lateral(self):
        """Returns the LATERAL JOIN to unpivot a table.

        :return: LATERAL JOIN expression.
        :rtype: list[str]
        """
        return dedent("""
        LATERAL ( VALUES
            {values}
        ) AS {alias} (
            {columns}
        )
        """).format(
            values="\n  , ".join(self.values()),
            alias=self.alias,
            columns="\n  , ".join(map(str, self.columns()))
        )[1:]
