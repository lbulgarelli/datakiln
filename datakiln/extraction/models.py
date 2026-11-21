"""
Configuration domain models and task graphs for DBMS extraction pipeline.
"""
import json
import re
from collections.abc import Iterable, Iterator
from functools import cached_property
from itertools import repeat
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
)
from uuid import uuid4

from sqlalchemy import (
    MetaData,
    String,
    Table,
    and_,
    exists,
    false,
    literal,
    literal_column,
    or_,
    select,
    text,
    union_all,
)
from sqlalchemy import column as sa_column
from sqlalchemy import values as sa_values

from datakiln.metadata.graph import DependencyGraph

if TYPE_CHECKING:
    from datakiln.extraction.strategies import ExecutionStrategy


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

    @cached_property
    def patterns(self) -> list[Any]:
        """Precompiled regex patterns for reference columns."""
        pattern_list = []
        for column in self.columns:
            pattern_str = rf"(?<![\w\.\:]){re.escape(column)}(?![\w\.])"
            pattern_list.append(re.compile(pattern_str))
        return pattern_list


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
    def extract_columns(expr: str, alias: str) -> list[str]:
        """Extract all column names associated with an alias.

        :param expr: SQL column expression
                     (e.g. ``'COALESCE(src.id, ref.pat_id)'``).
        :type expr: str

        :param alias: Table alias (e.g. ``'ref'``).
        :type alias: str

        :return: List of extracted bare column names.
        :rtype: list[str]
        """
        pattern = rf"(?<![\w\.]){re.escape(alias)}\.([a-zA-Z0-9_]+)(?![\w\.])"
        return [match.group(1) for match in re.finditer(pattern, expr)]

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

        # Extract any identifier that starts with "alias."
        alias = self.reference.alias
        for col in self.reference.columns:
            for col_name in ColumnPair.extract_columns(col, alias):
                columns[col_name] = None
        return list(columns)

    @cached_property
    def seed_positions(self) -> dict[int, int]:
        """Return the position in the cohort data of each reference column
        that is a column of the cohort, by the position of the reference
        column. The others are constants, such as a list of values."""
        # A reference column that names a column of the cohort takes the next
        # position of the cohort data
        positions = {}
        for ref_idx, ref_col in enumerate(self.reference.columns):
            if ColumnPair.extract_columns(ref_col, self.reference.alias):
                positions[ref_idx] = len(positions)
        return positions

    def to_exists(
        self,
        strategy: 'ExecutionStrategy'
    ) -> Any:
        """Generate a SQLAlchemy EXISTS subquery for this filter configuration.

        :param strategy: Execution strategy (used to resolve cohort aliases).
        :type strategy: ExecutionStrategy

        :return: SA exists() expression.
        :rtype: Exists
        """
        # Resolve the primary table
        resolved = strategy.cohorts.get(self.resolved_cohort, self.cohort)

        if resolved:
            if resolved.strip().startswith("("):
                # resolved is a subquery like "( SELECT ... ) cohort_name"
                # Strip the cohort name and replace it with the reference alias
                query_body = resolved.strip().rsplit(")", 1)[0] + ")"
                from_clause = text(f"{query_body} {self.reference.alias}")
            else:
                from_clause = get_table(resolved, self.reference.alias)
        elif self.reference.schema and self.reference.table:
            from_clause = Table(
                self.reference.table,
                MetaData(),
                schema=self.reference.schema
            ).alias(self.reference.alias)
        else:
            from_clause = None

        # Build WHERE conditions
        conditions = [
            text(f"{col} {op} {ref}")
            for col, op, ref in zip(
                self.columns,
                self.reference.operators,
                self.reference.columns
            )
        ]

        # If there is no from_clause, return the conditions as a conjunction
        if from_clause is None:
            return and_(*conditions)

        # Build join chain
        for j in self.join:
            join_name = j.to_join(strategy.cohorts)
            join_tbl = get_table(join_name, j.reference.alias)
            from_clause = from_clause.join(join_tbl, j.to_onclause())

        return exists(
            select(literal(1))
            .select_from(from_clause)
            .where(and_(*conditions))
        )

    def candidate_types(
        self,
        table_types: dict[str, Any],
        cohort_types: dict[str, Any]
    ) -> tuple[list[Any], list[Any]]:
        """The types the columns of the cohort data may have, to be tried in
        order: the types of the columns of the table, and the ones of the
        cohort. They only differ in the columns compared to an expression.

        :param table_types: Types of the columns of the table, by lower case
                            name.
        :type table_types: dict[str, TypeEngine]

        :param cohort_types: Types of the columns of the cohort, by lower case
                             name.
        :type cohort_types: dict[str, TypeEngine]

        :return: The types of each column, for each candidate.
        :rtype: tuple[list[TypeEngine], list[TypeEngine]]
        """
        by_table, by_cohort = [], []
        for ref_idx, (col, ref_col) in enumerate(zip(
            self.columns,
            self.reference.columns
        )):
            # A constant is not in the cohort data
            if ref_idx not in self.seed_positions:
                continue

            # The type of the first column of the table in the column
            names = ColumnPair.extract_columns(col.lower(), "src")
            table_type = String()
            for name in names:
                if name in table_types:
                    table_type = table_types[name]
                    break

            # A column of the table is compared as it is, so that the
            # database does not convert it, which would stop it from using
            # its indexes. The type of an expression depends on it, so it
            # may be the one of the cohort
            ref_name = ColumnPair.bare(ref_col).lower()
            cohort_type = cohort_types.get(ref_name, table_type)
            if names and col.lower() == f"src.{names[0]}":
                cohort_type = table_type

            by_table.append(table_type)
            by_cohort.append(cohort_type)
        return by_table, by_cohort

    def seed_expression(self, col: str) -> str:
        """Replace any reference column in a column of the filter with its
        corresponding column of the cohort data.

        :param col: Column of the filter, or an expression.
        :type col: str

        :return: The column, in terms of the columns of the cohort data.
        :rtype: str
        """
        # A reference column of the cohort is a column of the cohort data,
        # which is named "seed"
        for ref_idx, pattern in enumerate(self.reference.patterns):
            if ref_idx in self.seed_positions:
                col = pattern.sub(f"seed.c{self.seed_positions[ref_idx]}", col)
        return col

    def to_where(
        self,
        values: list[tuple],
        types: list[Any],
        chunk_size: int | None = None
    ) -> Any:
        """Generate a SQLAlchemy WHERE clause from pre-fetched cohort data.

        The cohort data is a VALUES clause, which each dialect sends to the
        database as one parameter.

        :param values: List of value tuples from the resolved cohort.
        :type values: list[tuple]

        :param types: Type of each column of the cohort data.
        :type types: list[TypeEngine]

        :param chunk_size: Number of seeds of each parameter. All the seeds
                           in one by default.
        :type chunk_size: int

        :return: SA ClauseElement.
        :rtype: ClauseElement
        """
        # Without any seed, nothing matches
        if not values:
            return false()

        # The cohort data is a VALUES clause, with a typed column for each
        # column of the cohort
        columns = []
        for col_idx, type_ in enumerate(types):
            columns.append(sa_column(f"c{col_idx}", type_))

        # A parameter for each group of seeds, which are the rows of one
        # table
        size = chunk_size or len(values)
        chunks = []
        for i in range(0, len(values), size):
            chunk = sa_values(*columns, name="seed").data(values[i:i + size])
            chunks.append(chunk)

        # The groups are one row source, which the database reads as one
        # semi-join
        seeds = chunks[0]
        if len(chunks) > 1:
            seeds = union_all(*[select(chunk) for chunk in chunks])
            seeds = seeds.subquery("seed")

        # Each column of the filter is compared to its column of the cohort
        # data, or to its constant
        conditions = []
        for ref_idx, (col, op, ref_col) in enumerate(zip(
            self.columns,
            self.reference.operators,
            self.reference.columns
        )):
            # A constant, such as a list of values, is compared as it is
            if ref_idx not in self.seed_positions:
                conditions.append(text(f"{col} {op} {ref_col}"))
                continue

            # The column of the filter, with its reference columns replaced,
            # is compared to its column of the cohort data
            pos = self.seed_positions[ref_idx]
            seed = seeds.c[f"c{pos}"]
            condition = literal_column(self.seed_expression(col)).op(op)(seed)

            # A row without a value for the column does not restrict it
            if any(row[pos] is None for row in values):
                condition = or_(seed.is_(None), condition)
            conditions.append(condition)

        # A row of the table matches if any of the rows of the cohort data
        # meets all the conditions
        return exists(
            select(literal(1)).select_from(seeds).where(and_(*conditions))
        )


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

        for f in (filters or []):
            if isinstance(f, list):
                self.filters.append([FilterConfig(**c) for c in f])
            else:
                self.filters.append([FilterConfig(**f)])

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
