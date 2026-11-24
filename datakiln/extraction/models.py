"""
Configuration domain models and task graphs for DBMS extraction pipeline.
"""
import json
import re
from collections.abc import Iterable, Iterator
from functools import cached_property
from itertools import repeat
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    MetaData,
    Table,
    text,
)

from datakiln.metadata.graph import DependencyGraph

# Generate a unique run UUID shared across extraction components
UUID = str(uuid4()).rsplit("-", maxsplit=1)[-1]


def alias_column(column: str, default: str) -> str:
    """Impute table alias for column reference.

    :param column: Column name or template string.
    :type column: str

    :param default: Default table alias.
    :type default: str

    :return: Aliased column name.
    :rtype: str
    """
    if "." in column:
        return column
    if value := re.search(r'\{(.*?)\}', column):
        return value.group(1)
    return f"{default}.{column}"


def get_table(name: str, alias: str | None = None) -> Any:
    """Helper to create a Table object, resolving schema-qualification.

    :param name: Table name (potentially schema-qualified).
    :type name: str

    :param alias: Optional table alias name.
    :type alias: str, optional

    :return: SA Table or Alias object.
    :rtype: Table | Alias
    """
    if "." in name:
        schema, table = name.split(".", 1)
        tbl = Table(table, MetaData(), schema=schema)
    else:
        tbl = Table(name, MetaData())
    return tbl.alias(alias) if alias else tbl


class ReferenceConfig:
    # pylint: disable=too-few-public-methods
    """Target reference table configuration.

    :param columns: Reference column names.
    :type columns: list[str]

    :param schema: Reference database schema.
    :type schema: str, optional

    :param table: Reference table name.
    :type table: str, optional

    :param operators: Comparison operators.
    :type operators: list[str], optional

    :param alias: Table alias.
    :type alias: str, optional
    """

    def __init__(
        self,
        columns: list[str],
        schema: str | None = None,
        table: str | None = None,
        operators: list[str] | None = None,
        alias: str | None = "ref"
    ):
        # pylint: disable=too-many-arguments, too-many-positional-arguments
        self.schema = schema
        self.table = table
        self.operators = operators or repeat("=")
        self.alias = alias or "ref"
        self.columns = [alias_column(c, self.alias) for c in columns]

    @property
    def full_name(self) -> str:
        """Returns the full table name (schema.table)."""
        if self.schema and self.table:
            return f"{self.schema}.{self.table}"
        return self.table or ""


class ColumnPair:
    """Base class for a single condition in a JoinConfig pair list."""

    def __init__(self, left: str, right: str):
        self.left = left
        self.right = right

    @staticmethod
    def bare(col: str) -> str:
        """Strip the alias prefix from a qualified column name.

        :param col: Qualified or bare column name (e.g. ``'alias.column'``).
        :type col: str

        :return: The bare column name after the last dot.
        :rtype: str
        """
        return col.split(".")[-1]

    @staticmethod
    def extract_columns(expr: str) -> list[str]:
        """Extract qualified column references in expression order.

        :param expr: SQL expression, such as ``'src.value + ref.offset'``.
        :type expr: str

        :return: References including their aliases, such as ``'ref.offset'``.
        :rtype: list[str]
        """
        pattern = (
            r'(?<![\w.])'
            r'([a-zA-Z_][\w$#]*\.[a-zA-Z0-9_$#]+)'
            r'(?![\w.])'
        )
        columns = []
        for match in re.finditer(pattern, expr):
            columns.append(match.group(1))
        return columns

    @classmethod
    def classify(
        cls,
        left: str,
        right: str,
        cohort_alias: str
    ) -> "ColumnPair":
        """Classify a processed column pair by inspecting its syntax.

        :param left: Processed left-side column.
        :type left: str

        :param right: Processed right-side column.
        :type right: str

        :param cohort_alias: Alias of the root reference (local cohort table).
        :type cohort_alias: str

        :return: Appropriate :class:`ColumnPair` subclass instance.
        :rtype: ColumnPair
        """
        # Handle constant filters (i.e., columns without an alias)
        if "." not in left:
            return ConstantFilter(left, right)
        if "." not in right:
            # normalise: value always left
            return ConstantFilter(right, left)  # pylint: disable=arguments-out-of-order # noqa: E501

        # Handle seed columns (i.e., columns from the root cohort table)
        if left.startswith(f"{cohort_alias}."):
            return SeedPair(left, right)

        # Handle cross columns (i.e., columns from other reference tables)
        return CrossPair(left, right)

    def to_onclause(self, cohort_alias: str, ref_alias: str) -> str:
        """Return the SQL ON-clause fragment for this pair.

        :param cohort_alias: Alias of the local cohort table.
        :type cohort_alias: str

        :param ref_alias: Alias of the remote join table.
        :type ref_alias: str

        :return: SQL equality fragment for use in a JOIN ON clause.
        :rtype: str
        """
        raise NotImplementedError


class SeedPair(ColumnPair):
    """Links a local cohort column to a remote join-table column.

    At query time the local cohort column is matched against the remote table
    via a VALUES construct, passing seed IDs across database boundaries.
    """

    @property
    def local(self) -> str:
        """Bare cohort column name: 'alias.col' -> 'col'."""
        return self.bare(self.left)

    @property
    def remote(self) -> str:
        """Bare remote column name: 'alias.col' -> 'col'."""
        return self.bare(self.right)

    def to_onclause(self, cohort_alias: str, ref_alias: str) -> str:
        """:inheritdoc:"""
        return f"{cohort_alias}.{self.local} = {ref_alias}.{self.remote}"


class ConstantFilter(ColumnPair):
    """A constant equality constraint on a remote column.

    Applied as a WHERE predicate in StreamStrategy and as an ON-clause fragment
    in TempTableStrategy. Normalised so the constant value is always stored in
    ``left``.
    """

    @property
    def value(self) -> str:
        """The literal constant: e.g. '15'."""
        return self.left

    @property
    def remote(self) -> str:
        """Bare remote column name: 'alias.col' -> 'col'."""
        return self.bare(self.right)

    def to_onclause(self, cohort_alias: str, ref_alias: str) -> str:
        """:inheritdoc:"""
        return f"{ref_alias}.{self.remote} = {self.value}"


class CrossPair(ColumnPair):
    """An equality condition between two remote aliased columns.

    Applied as part of the JOIN ON clause when joining remote tables to one
    another (e.g. ``hx.pat_id = enc.pat_id``).
    """

    def to_onclause(self, cohort_alias: str, ref_alias: str) -> str:
        """:inheritdoc:"""
        return f"{self.left} = {self.right}"


class JoinConfig:
    # pylint: disable=too-few-public-methods
    """Intermediate join configuration for reference chains.

    :param columns: Source columns for the join.
    :type columns: list[str]

    :param reference: Reference table configuration.
    :type reference: dict[str, Any]
    """

    def __init__(
        self,
        columns: list[str],
        reference: dict[str, Any],
        reference_alias: str = "ref"
    ):
        self.reference_alias = reference_alias
        self.reference = ReferenceConfig(**reference)

        self.pairs = []
        for left, ref_col in zip(columns, self.reference.columns):
            self.pairs.append(ColumnPair.classify(
                alias_column(left, reference_alias),
                ref_col,
                reference_alias,
            ))

    @property
    def columns(self) -> list[str]:
        """Left-side column references, other than the constants."""
        columns = []
        for pair in self.pairs:
            if not isinstance(pair, ConstantFilter):
                columns.append(pair.left)
        return columns

    def seed_pairs(self) -> Iterator["SeedPair"]:
        """Pairs that link a local cohort column to a remote column."""
        return (p for p in self.pairs if isinstance(p, SeedPair))

    def const_filters(self) -> Iterator["ConstantFilter"]:
        """Pairs that apply a constant equality constraint."""
        return (p for p in self.pairs if isinstance(p, ConstantFilter))

    def cross_pairs(self) -> Iterator["CrossPair"]:
        """Pairs that join two remote aliased columns."""
        return (p for p in self.pairs if isinstance(p, CrossPair))

    def to_join(self, cohorts: dict[str, str]):
        """Return the resolved table name to join against.

        Prefers the temporary table created during this run if it exists,
        otherwise falls back to the permanent schema-qualified table name.

        :param cohorts: Mapping of task name -> created temp table name,
                        as maintained by the active ExecutionStrategy.
        :type cohorts: dict[str, str]

        :return: Resolved table name string (used to build SA TableClause).
        :rtype: str
        """
        if self.reference.full_name in cohorts:
            return cohorts[self.reference.full_name]
        return self.reference.full_name

    def to_onclause(self) -> Any:
        """Build a SQLAlchemy ON clause for this join as a text expression.

        :return: SA text clause for the JOIN ON condition.
        :rtype: ClauseElement
        """
        return text(" AND ".join(
            pair.to_onclause(self.reference_alias, self.reference.alias)
            for pair in self.pairs
        ))


class FilterConfig:
    """Filter configuration mapping a task to a cohort or reference table.

    :param columns: Target columns to filter.
    :type columns: list[str]

    :param reference: Target reference configuration.
    :type reference: ReferenceConfig, optional

    :param cohort: Source cohort identifier.
    :type cohort: str, optional

    :param join: List of intermediate join chains.
    :type join: list[JoinConfig], optional
    """

    def __init__(
        self,
        columns: list[str],
        reference: dict[str, Any],
        cohort: str | None = None,
        join: list[dict[str, Any]] | None = None
    ):
        self.columns = [alias_column(c, "src") for c in columns]
        self.reference = ReferenceConfig(**reference)
        self.cohort = cohort
        self.join = []

        valid_aliases = {self.reference.alias}
        for j in join or []:
            j_config = JoinConfig(**j, reference_alias=self.reference.alias)

            # Validate that joins do not reference tables outside its scope
            for col in j_config.columns:
                if col.partition(".")[0] not in valid_aliases:
                    raise ValueError(f"Invalid alias in join column '{col}'.")

            self.join.append(j_config)
            valid_aliases.add(j_config.reference.alias)

    @property
    def is_sql_cohort(self) -> bool:
        """Evaluates whether the filter targets an explicit SQL cohort file."""
        return bool(self.cohort and self.cohort.endswith(".sql"))

    @property
    def resolved_cohort(self) -> str | None:
        """Return the resolved cohort table name."""
        if self.cohort:
            return self.cohort
        if self.reference.schema and self.reference.table:
            return f"{self.reference.schema}.{self.reference.table}"
        return None

    @property
    def cohort_columns(self) -> list[str]:
        """Return the target columns to fetch from the cohort table."""
        # Extract seed columns from joins
        columns = {}
        for j in self.join:
            for pair in j.seed_pairs():
                columns[pair.local] = None

        # Only columns from the root cohort are read from its local data.
        for reference in self.reference_columns:
            alias, name = reference.split('.', 1)
            if alias == self.reference.alias:
                columns[name] = None
        return list(columns)

    @cached_property
    def reference_columns(self) -> list[str]:
        """Collect root and joined reference columns used in comparisons."""
        aliases = {self.reference.alias}
        for join in self.join:
            aliases.add(join.reference.alias)

        # Keep each cohort or join reference once, in expression order.
        columns = []
        for expression in self.reference.columns + self.columns:
            for reference in ColumnPair.extract_columns(expression):
                alias = reference.split('.', 1)[0]
                if alias in aliases and reference not in columns:
                    columns.append(reference)
        return columns


class ExtractMeta:
    """Extraction metadata."""

    def __init__(
        self,
        table: str,
        schema: str,
        filters: list[dict | list[dict]] = None,
        encoding: str = "utf-8",
        chunk_size: int = 10000,
        **kwargs
    ):
        # pylint: disable=too-many-arguments, too-many-positional-arguments
        """
        :param table: Name of the table.
        :type table: str

        :param schema: Name of the database schema.
        :type schema: str

        :param filters: Metadata for filters.
        :type filters: list[dict] | list[list[dict]]

        :param encoding: encoding of the data file.
        :type encoding: str

        :param chunk_size: Chunk size for cohort seed loading during
                           extraction.
                           Smaller values reduce payload size and memory
                           pressure for large tables.
        :type chunk_size: int
        """
        self.table = table
        self.schema = schema
        self.encoding = encoding
        self.chunk_size = chunk_size
        self.kwargs = kwargs
        self.filters = []

        # Normalize individual filters to groups and skip empty groups
        # before they reach the query builder.
        for group in filters or []:
            if not isinstance(group, list):
                group = [group]
            if not group:
                continue

            filter_group = []
            for config in group:
                filter_group.append(FilterConfig(**config))
            self.filters.append(filter_group)

    @property
    def full_name(self) -> str:
        """Returns the full table name (schema.table)."""
        return f"{self.schema}.{self.table}"

    @property
    def temp_name(self) -> str:
        """Returns the temporary table name used during extraction."""
        return f"tmp_{self.table}_{UUID}"

    @property
    def requires(self) -> set[str]:
        """Retrieve dependencies from filters.

        :return: Set of dependency task names.
        :rtype: set[str]
        """
        dependencies = set()
        for group in self.filters:
            for table_filter in group:
                # Cohort dependencies
                if table_filter.cohort:
                    dependencies.add(table_filter.cohort)
                # Table dependencies from primary reference
                elif (
                    table_filter.reference
                    and table_filter.reference.schema
                    and table_filter.reference.table
                ):
                    ref = table_filter.reference
                    dependencies.add(f"{ref.schema}.{ref.table}")

                # Join-chain dependencies
                for j in table_filter.join:
                    if j.reference.schema and j.reference.table:
                        dependencies.add(j.reference.full_name)

        return dependencies

    @classmethod
    def from_json(cls, path: Path) -> 'ExtractMeta':
        """Load extraction metadata from JSON.

        :param path: Path to the extraction metadata file.
        :type path: Path

        :return: Extraction metadata.
        :rtype: ExtractMeta
        """
        with path.open("r", encoding="utf-8") as file:
            meta = json.load(file)
        return cls(**meta)


class ExtractGraph(DependencyGraph):
    """Directed Acyclic Graph for extraction tasks.

    Dependencies that are not tasks (e.g. cohorts and tables that are not
    extracted) are kept, so they can be orchestrated by the executor.
    """

    def __init__(self, tasks: Iterable[ExtractMeta],
                 export: set[ExtractMeta] | None = None):
        """
        :param tasks: Extraction tasks.
        :type tasks: Iterable[ExtractMeta]

        :param export: Tasks whose data is exported. The other tasks are only
                       extracted, because they are needed. All by default.
        :type export: set[ExtractMeta]
        """
        super().__init__(tasks, keep_unresolved=True)
        self.export = set(tasks) if export is None else export

    @classmethod
    def select(cls, tasks: list[ExtractMeta],
               include: list[str] | None = None) -> 'ExtractGraph':
        """Graph of the tasks to export, and the tasks they need.

        :param tasks: Extraction tasks.
        :type tasks: list[ExtractMeta]

        :param include: Names of the tasks to export. All by default.
        :type include: list[str]

        :raises ValueError: If any of the names is not a task.
        """
        # The graph of all the tasks, which is the one to export by default,
        # and which tells what each task needs
        graph = cls(tasks)
        if not include:
            return graph

        # The names must be tasks of the graph, since they are the ones the
        # user wants to export
        export = graph.find(include)

        # Keep only the tasks to export and the ones they need, directly or
        # indirectly, since they have to be extracted first. The others are
        # left out of the graph
        return cls(graph.requirements(export), export)

    @property
    def tasks(self) -> dict[str, ExtractMeta]:
        """Extraction tasks by name."""
        return self.nodes
