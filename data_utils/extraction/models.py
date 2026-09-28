"""
Configuration domain models and task graphs for DBMS extraction pipeline.
"""
import re
import math
import json
from uuid import uuid4
from datetime import datetime
from pathlib import Path
from itertools import repeat
from functools import cached_property
from graphlib import TopologicalSorter
from typing import (
    List, Dict, Set, Optional, Any, Tuple, Iterator, Union, TYPE_CHECKING
)

from sqlalchemy import (
    select, exists, literal, and_, or_, false, text,
    Table, MetaData
)

if TYPE_CHECKING:
    from data_utils.extraction.strategies import ExecutionStrategy


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


def get_table(name: str, alias: Optional[str] = None) -> Any:
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


def _coerce(value: Any) -> Any:
    """Coerce value to datetime if it's an ISO formatted string.

    :param value: Input value to coerce.
    :type value: Any

    :return: Coerced datetime object or the original value.
    :rtype: Any
    """
    if isinstance(value, str) and '-' in value:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass
    return value


def _is_null(v: Any) -> bool:
    """Check if a value is None or NaN (Not a Number).

    :param v: Value to inspect.
    :type v: Any

    :return: True if the value is None or a float NaN, False otherwise.
    :rtype: bool
    """
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    return False


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
        columns: List[str],
        schema: Optional[str] = None,
        table: Optional[str] = None,
        operators: Optional[List[str]] = None,
        alias: Optional[str] = "ref"
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
    def extract_columns(expr: str, alias: str) -> List[str]:
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
        columns: List[str],
        reference: Dict[str, Any],
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
    def columns(self) -> List[str]:
        """Left-side column references (backward-compatible view of pairs)."""
        return [pair.left for pair in self.pairs]

    def seed_pairs(self) -> Iterator["SeedPair"]:
        """Pairs that link a local cohort column to a remote column."""
        return (p for p in self.pairs if isinstance(p, SeedPair))

    def const_filters(self) -> Iterator["ConstantFilter"]:
        """Pairs that apply a constant equality constraint."""
        return (p for p in self.pairs if isinstance(p, ConstantFilter))

    def cross_pairs(self) -> Iterator["CrossPair"]:
        """Pairs that join two remote aliased columns."""
        return (p for p in self.pairs if isinstance(p, CrossPair))

    def to_join(self, cohorts: Dict[str, str]):
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
        columns: List[str],
        reference: Dict[str, Any],
        cohort: Optional[str] = None,
        join: Optional[List[Dict[str, Any]]] = None
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
                if not any(col.startswith(f"{a}.") for a in valid_aliases):
                    raise ValueError(f"Invalid alias in join column '{col}'.")

            self.join.append(j_config)
            valid_aliases.add(j_config.reference.alias)

    @property
    def is_sql_cohort(self) -> bool:
        """Evaluates whether the filter targets an explicit SQL cohort file."""
        return bool(self.cohort and self.cohort.endswith(".sql"))

    @property
    def resolved_cohort(self) -> Optional[str]:
        """Return the resolved cohort table name."""
        if self.cohort:
            return self.cohort
        if self.reference.schema and self.reference.table:
            return f"{self.reference.schema}.{self.reference.table}"
        return None

    @property
    def cohort_columns(self) -> List[str]:
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

    def to_where(self, values: List[Tuple]) -> Any:
        # pylint: disable=too-many-locals
        """Generate a SQLAlchemy WHERE clause from pre-fetched cohort data.

        :param values: List of value tuples from the resolved cohort.
        :type values: list[tuple]

        :return: SA ClauseElement.
        :rtype: ClauseElement
        """

        if not values:
            return false()

        or_blocks = []
        for row_idx, values_set in enumerate(values):
            conditions = []
            for col_idx, (col, op, val) in enumerate(zip(
                self.columns,
                self.reference.operators,
                values_set
            )):
                if _is_null(val):
                    continue

                # Set the first parameter and bind
                expr = col
                param_name = f"p_{row_idx}_{col_idx}"
                used_params = {param_name: _coerce(val)}

                # Replace any reference column in `col`
                # with its corresponding bind parameter
                for ref_idx, ref_col in enumerate(self.reference.columns):
                    # Skip references that are not present in the expression
                    if ref_col not in expr:
                        continue

                    # Skip references that are NULL
                    ref_val = values_set[ref_idx]
                    if _is_null(ref_val):
                        continue

                    # Replace the reference column with a bind parameter
                    pattern = rf"(?<![\w\.\:]){re.escape(ref_col)}(?![\w\.])"
                    ref_param = f"p_{row_idx}_{ref_idx}"
                    expr, count = re.subn(pattern, f":{ref_param}", expr)
                    if count > 0:
                        used_params[ref_param] = _coerce(ref_val)

                # Append the condition
                conditions.append(
                    text(f"{expr} {op} :{param_name}").bindparams(
                        **used_params
                    )
                )
            if conditions:
                or_blocks.append(and_(*conditions))

        return or_(*or_blocks) if or_blocks else false()


class ExtractMeta:
    """Extraction metadata."""

    def __init__(
        self,
        table: str,
        schema: str,
        filters: List[Union[Dict, List[Dict]]] = None,
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
    def name(self) -> str:
        """Returns the full table name (schema.table)."""
        return f"{self.schema}.{self.table}"

    @property
    def temp_name(self) -> str:
        """Returns the temporary table name used during extraction."""
        return f"tmp_{self.table}_{UUID}"

    def dependencies(self) -> Set[str]:
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


class DependentsMap(dict):
    """Successor map from adjacency map."""

    def __init__(self, adj: Dict[str, Set[str]]):
        super().__init__()
        for node, predecessors in adj.items():
            for pred in predecessors:
                self.setdefault(pred, set()).add(node)

    def count(self) -> Dict[str, int]:
        """Returns the reference counts for each node."""
        return {node: len(deps) for node, deps in self.items()}

    def as_list(self) -> Dict[str, List[str]]:
        """Returns the dependents map with sets converted to lists."""
        return {node: list(deps) for node, deps in self.items()}


class ExtractGraph:
    """Directed Acyclic Graph for extraction tasks."""

    def __init__(self, tasks: List[ExtractMeta]):
        self.adj = {}
        self.tasks = {}
        for task in tasks:
            self.tasks[task.name] = task
            self.adj[task.name] = task.dependencies()

    @property
    def topological_sorter(self) -> TopologicalSorter:
        """Returns a prepared TopologicalSorter for the graph."""
        ts = TopologicalSorter(self.adj)
        ts.prepare()
        return ts

    @cached_property
    def dependents(self) -> DependentsMap:
        """Map each node to the set of tasks that depend on it."""
        return DependentsMap(self.adj)

    def sort(self) -> List[ExtractMeta]:
        """Perform topological sort to determine execution order."""
        ordered_tasks = []
        for name in TopologicalSorter(self.adj).static_order():
            if name in self.tasks:
                ordered_tasks.append(self.tasks[name])
        return ordered_tasks
