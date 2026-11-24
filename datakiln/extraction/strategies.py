"""
Execution strategies for the extraction pipeline.
"""
import hashlib
import logging
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from functools import cached_property
from itertools import chain
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import (
    Column,
    Index,
    MetaData,
    String,
    Table,
    and_,
    inspect,
    literal,
    literal_column,
    select,
    text,
    values,
)
from sqlalchemy import column as sa_column
from sqlalchemy.exc import DatabaseError, NoSuchTableError, SQLAlchemyError
from sqlalchemy.schema import DropTable
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.selectable import SelectBase

from datakiln.extraction.exporters import DataExporter, MetadataExtractor
from datakiln.extraction.filters import (
    CohortFilters,
    DatabaseFilter,
    FilterBuilder,
    SeedContext,
    SeedFilter,
)
from datakiln.extraction.models import (
    UUID,
    ExtractMeta,
    FilterConfig,
    get_table,
)
from datakiln.extraction.renderers import QueryDirector, SQLGenerator
from datakiln.metadata.database import DatabaseMeta, Session

logger = logging.getLogger(__name__)


class HashingReader:
    # pylint: disable=too-few-public-methods
    """File-like wrapper that calculates MD5 hash on the fly while reading."""

    def __init__(self, fp):
        self.fp = fp
        self.hasher = hashlib.md5()  # noqa: DUO130

    def read(self, size=-1):
        """Read data and update hasher."""
        chunk = self.fp.read(size)
        self.hasher.update(chunk)
        return chunk

    def seek(self, offset, whence=0):
        """Delegate seek to the underlying file pointer."""
        return self.fp.seek(offset, whence)

    def tell(self):
        """Delegate tell to the underlying file pointer."""
        return self.fp.tell()


# pylint: disable=too-many-instance-attributes
class ExecutionStrategy(ABC):
    """Abstract base class for extraction strategies."""

    def __init__(self, db_params: dict, directory: Path):
        self.db_params = db_params
        self.directory = directory
        self.cohorts = {}
        self.created_tables = set()
        self.lock = threading.Lock()
        self._local = threading.local()
        self.csv_hashes = {}
        self.graph = None
        self.abort = threading.Event()

    @cached_property
    def engine(self):
        """Returns the shared database engine."""
        with self.session(bind=False) as session:
            return session.get_bind()

    @contextmanager
    def session(self, bind: bool | None = True) -> Iterator[Session]:
        """Returns a managed database session.

        If a session is already active for this thread, it is yielded.
        Otherwise, a new pooled session is created and registered as active.

        :param bind: Whether to use the shared engine. If False, a new
                     engine is created using Session.create().
        """
        if not bind:
            with Session.create(**self.db_params) as session:
                yield session
            return

        # Return thread-local session if active
        if hasattr(self._local, "session"):
            yield self._local.session
            return

        # Create a new managed session for this thread
        with Session(bind=self.engine, autoflush=True) as session:
            self._local.session = session
            try:
                yield session
            finally:
                if hasattr(self._local, "session"):
                    del self._local.session

    def build_queries(self, task: ExtractMeta) -> SelectBase:
        """Build the extraction SA Select statement.

        :param task: Extraction task metadata.
        :type task: ExtractMeta

        :return: SA Select statement.
        :rtype: sqlalchemy.sql.selectable.SelectBase
        """
        return QueryDirector.build_queries(task, strategy=self)

    @property
    def is_read_only(self) -> bool:
        """Returns whether the strategy is read-only."""
        return False

    @abstractmethod
    def process_filter(
        self,
        filter_config: FilterConfig,
        task: ExtractMeta,
        queries: CohortFilters,
        seeds: SeedContext
    ) -> ClauseElement:
        """Construct the filter join and return its match condition.

        :param filter_config: Filter to apply to the source rows.
        :type filter_config: datakiln.extraction.models.FilterConfig

        :param task: Extraction target.
        :type task: datakiln.extraction.models.ExtractMeta

        :param queries: Statement builder that attaches filter joins.
        :type queries: datakiln.extraction.filters.CohortFilters

        :param seeds: Seed settings and shared CTE cache for this query.
        :type seeds: datakiln.extraction.filters.SeedContext

        :return: Condition indicating whether this filter matched.
        :rtype: sqlalchemy.sql.elements.ClauseElement
        """
        # Constant filters have no reference; database filters resolve a
        # table or SQL cohort through this strategy's cohort mapping.
        if filter_config.resolved_cohort is None:
            return FilterBuilder(filter_config).build(queries)
        return DatabaseFilter(
            filter_config, self.cohorts, seeds.dialect
        ).build(queries)

    @abstractmethod
    def execute(self, task: ExtractMeta, export_to: Path | None = None):
        """Execute the extraction task."""

    @abstractmethod
    def get_cohort(self, cohort: str) -> str:
        """Retrieve or create a cohort representation."""

    @abstractmethod
    def drop_cohort(self, cohort: str):
        """Drop a specific cohort table."""

    @property
    def tmp_dir(self) -> Path:
        """Folder of the files that are not exported."""
        return self.directory / "data" / ".tmp"

    def drop(self, name: str):
        """Drop the temporary data of the task with this temporary name,
        which is its file."""
        table_name = name[4:-(len(UUID) + 1)]
        (self.tmp_dir / f"{table_name}.csv.gz").unlink(missing_ok=True)
        self.created_tables.discard(name)

    def cleanup(self):
        """Drop all remaining temporary data and cohorts, and the folder of
        the files that are not exported."""
        for name in list(self.created_tables):
            self.drop(name)
        self.drop_cohorts()

        try:
            self.tmp_dir.rmdir()
        except OSError:
            pass

    def drop_cohorts(self):
        """Drop all remaining cohort tables."""
        for cohort in list(self.cohorts.keys()):
            self.drop_cohort(cohort)


class TempTableStrategy(ExecutionStrategy):
    """Strategy that uses temporary tables (requires write permissions)."""

    def has_index(
        self,
        session: Session,
        schema: str | None,
        table: str,
        columns: list[str]
    ) -> bool:
        """Evaluate the existence of an index with the specified columns."""
        insp = inspect(session.bind)
        for idx in insp.get_indexes(table, schema=schema):
            if idx["column_names"] == columns:
                return True
        return False

    def ensure_index(
        self,
        session: Session,
        table_parts: list[str],
        columns: list[str]
    ):
        """Create a covering index on the reference table if non-existent."""
        if not columns:
            return

        columns_hash = hashlib.md5("_".join(columns).encode()).hexdigest()[:8]  # noqa: DUO130, E501
        schema = table_parts[0] if len(table_parts) == 2 else None
        tbl_name = table_parts[-1]

        with self.lock:
            if not self.has_index(session, schema, tbl_name, columns):
                try:
                    name = f"{tbl_name[:49]}_idx_{columns_hash}"
                    sa_table = Table(
                        tbl_name, MetaData(), schema=schema,
                        autoload_with=session.bind
                    )
                    sa_cols = [sa_table.c[c] for c in columns]
                    idx = Index(name, *sa_cols)
                    idx.create(bind=session.bind)
                except SQLAlchemyError as err:
                    logger.warning(
                        "Table '%s': Failed to create index (%s).",
                        ".".join(table_parts), err
                    )

    def process_filter(
        self,
        filter_config: FilterConfig,
        task: ExtractMeta,
        queries: CohortFilters,
        seeds: SeedContext
    ) -> ClauseElement:
        if filter_config.resolved_cohort is not None:
            actual_table = self.cohorts.get(
                filter_config.resolved_cohort,
                filter_config.resolved_cohort
            ).split(".")
            with self.session() as session:
                self.ensure_index(
                    session, actual_table, filter_config.cohort_columns)
        return super().process_filter(filter_config, task, queries, seeds)

    def execute(self, task: ExtractMeta, export_to: Path | None = None):
        # pylint: disable=too-many-locals
        logger.info("Table '%s': Extracting.", task.full_name)

        with self.session() as session:
            # All filter groups share one duplicate-preserving statement.
            query = self.build_queries(task)
            temp_table = task.temp_name

            # Reflect source table to get column names and types
            source_table = Table(
                task.table,
                MetaData(),
                schema=task.schema,
                autoload_with=session.bind
            )

            # Initialize destination table
            meta = MetaData()
            dest_table = Table(temp_table, meta)

            # Add columns to destination table
            col_names = []
            for col in source_table.columns:
                dest_table.append_column(Column(col.name, col.type))
                col_names.append(col.name)

            # Create destination table
            self.created_tables.add(temp_table)
            self.cohorts[task.full_name] = temp_table
            dest_table.create(session.bind)

            # Fallback to .tmp dir if export_to is not specified
            target_dir = export_to or self.tmp_dir
            target_dir.mkdir(parents=True, exist_ok=True)
            export_path = target_dir / f"{task.table}.csv.gz"

            # Write to CSV and inserting simultaneously, deleting the file if
            # it is not complete
            try:
                self.csv_hashes[task.full_name] = DataExporter.stream_to_csv(
                    session,
                    query,
                    export_path,
                    table_name=task.table,
                    insert_to=dest_table,
                    chunk_size=task.chunk_size,
                    abort=self.abort
                )
            except BaseException:
                export_path.unlink(missing_ok=True)
                raise
            if self.abort.is_set():
                export_path.unlink(missing_ok=True)

    def drop(self, name: str):
        """Drop a temporary table, and its file."""
        super().drop(name)
        with self.session() as session:
            try:
                sa_table = Table(name, MetaData())
                session.execute(DropTable(sa_table, if_exists=True))
                session.commit()
            except SQLAlchemyError as err:
                logger.warning("Table '%s': Could not drop (%s).", name, err)

    # pylint: disable=too-many-locals
    def get_cohort(self, cohort: str) -> str:
        """Retrieve or create a cohort table."""
        if cohort in self.cohorts:
            return self.cohorts[cohort]

        logger.info("Cohort '%s': Generating data.", cohort)

        name = cohort.replace(".sql", "").replace(".", "_")
        name = f'tmp_{name}_{UUID}'

        if not cohort.endswith(".sql"):
            db_params = DatabaseMeta.load_env(self.directory)
            source_engine = Session.create(**db_params).get_bind()
            schema, table = cohort.split(".")

            # Initialize source table
            source_meta = MetaData()
            source_table = Table(
                table,
                source_meta,
                schema=schema,
                autoload_with=source_engine
            )

            dest_columns = []
            dest_engine = self.engine
            for column in source_table.columns:
                # Get column metadata
                dialect = dest_engine.dialect.name
                col_meta = MetadataExtractor.generate_column_meta(
                    column, dialect=dialect, compile_type=False
                )
                dest_columns.append(Column(
                    col_meta["name"], col_meta["dtype"][dialect]
                ))

            # Create destination table
            dest_meta = MetaData()
            dest_table = Table(name, dest_meta, *dest_columns)
            self.cohorts[cohort] = name
            dest_table.create(dest_engine)

            # Move data from source to destination
            cohort_data = pd.read_sql_table(table, source_engine, schema)
            cohort_data.to_sql(
                name, dest_engine, index=False, if_exists='append'
            )

            return name

        # Load cohort query
        cohort_path = self.directory / 'extract' / cohort
        with open(cohort_path, encoding="utf-8") as file:
            query = text(file.read().strip())

        # Write cohort data to temp table
        self.cohorts[cohort] = name
        self.created_tables.add(name)
        with self.session() as session:
            cohort_data = pd.read_sql_query(query, session.bind)
            cohort_data.to_sql(
                name, session.bind, index=False, if_exists='fail'
            )

        return name

    def drop_cohort(self, cohort: str):
        """Drop a specific cohort table."""
        if cohort in self.cohorts:
            logger.info("Cohort '%s': Dropping data.", cohort)
            try:
                with self.session() as session:
                    sa_table = Table(self.cohorts[cohort], MetaData())
                    session.execute(DropTable(sa_table, if_exists=True))
                    session.commit()
            except SQLAlchemyError as err:
                logger.warning(
                    "Cohort '%s': Could not drop (%s).", cohort, err)
            finally:
                del self.cohorts[cohort]


class JoinChain:
    """Join chain for filtering on read-only mode."""

    def __init__(self, filter_config: 'FilterConfig', engine):
        self._engine = engine
        self._cohort_cols = filter_config.cohort_columns
        self._cohort_alias = filter_config.reference.alias
        self._target_cols = []
        for reference in filter_config.reference_columns:
            self._target_cols.append(literal_column(reference))
        self._cohort_types = {}
        self._seed_joins = []
        self._where_clauses = []
        self._from_clause = None

    @classmethod
    def build(
        cls,
        filter_config: 'FilterConfig',
        engine,
        task: 'ExtractMeta'
    ) -> "JoinChain":
        """Build and return a fully resolved chain for *filter_config*.

        :param filter_config: Filter configuration describing the join chain.
        :type filter_config: FilterConfig

        :param engine: SQLAlchemy engine used to reflect remote tables.

        :param task: Extraction task metadata.
        :type task: ExtractMeta

        :return: Populated :class:`JoinChain` ready to execute seed chunks.
        :rtype: JoinChain
        """
        # Initialize join chain
        chain_ = cls(filter_config, engine)

        # Create aliased source table for the chain
        src = Table(
            task.table,
            MetaData(),
            schema=task.schema,
            autoload_with=engine
        ).alias("src")
        chain_._from_clause = src

        # Resolve column types and seed joins from the filter config
        for name in filter_config.cohort_columns:
            col = src.c.get(name, src.c.get(name.upper()))
            if col is None:
                continue
            chain_._cohort_types[name] = col.type
            chain_._seed_joins.append((col, name))

        # Extend join chain with subsequent joins
        for j_conf in filter_config.join:
            chain_._extend(j_conf)

        return chain_

    def _extend(self, j_conf):
        """Reflect one join config and extend the SA chain."""
        ref = j_conf.reference
        sa_table = Table(
            ref.table, MetaData(), schema=ref.schema,
            autoload_with=self._engine,
        ).alias(ref.alias)

        # Retrieve column types and seed joins from the filter config
        for pair in j_conf.seed_pairs():
            col = sa_table.c[pair.remote]
            self._cohort_types[pair.local] = col.type
            self._seed_joins.append((col, pair.local))

        # Retrieve constant filters from the filter config
        for p in j_conf.const_filters():
            where = sa_table.c[p.remote] == literal(p.value)
            self._where_clauses.append(where)

        # Build the FROM clause for the join chain
        if self._from_clause is None:
            self._from_clause = sa_table
        else:
            on_clauses = []
            for p in j_conf.cross_pairs():
                on_clauses.append(text(f"{p.left} = {p.right}"))
            onclause = and_(*on_clauses) if on_clauses else text("1=1")
            self._from_clause = self._from_clause.join(sa_table, onclause)

    def execute(self, chunk: list, session) -> list:
        """Execute a seed chunk against the built chain.

        :param chunk: Batch of seed tuples to look up.
        :type chunk: list

        :param session: Active SQLAlchemy session.
        :type session: Session

        :return: All matching rows from the remote tables.
        :rtype: list
        """
        # Create list of column objects for VALUES clause
        val_cols = []
        for name in self._cohort_cols:
            column = sa_column(name, self._cohort_types.get(name, String()))
            val_cols.append(column)
        v = values(*val_cols, name=self._cohort_alias).data(chunk)

        # Build join ON clauses from seed pairs
        seed_on = []
        for col, name in self._seed_joins:
            seed_on.append(col == v.c[name])

        # Build the full statement
        stmt = select(*self._target_cols).select_from(
            self._from_clause.join(v, and_(*seed_on))
        ).distinct()
        if self._where_clauses:
            stmt = stmt.where(and_(*self._where_clauses))
        return session.execute(
            stmt,
            execution_options={
                "skip_formatting": True
            }
        ).all()


class StreamStrategy(ExecutionStrategy):
    """Strategy that streams data directly from source (read-only)."""

    @property
    def is_read_only(self) -> bool:
        return True

    def process_filter(
        self,
        filter_config: FilterConfig,
        task: ExtractMeta,
        queries: CohortFilters,
        seeds: SeedContext
    ) -> ClauseElement:
        if filter_config.resolved_cohort and not filter_config.is_sql_cohort:
            # The seeds are the values of the cohort that the rows must match
            start = time.time()
            cohort_data = self.get_cohort_set(filter_config, task=task)
            logger.info(
                "Table '%s': Loaded %d seeds in %.1f s.",
                task.full_name, len(cohort_data), time.time() - start
            )

            # The database needs the type of each column of the seeds
            start = time.time()
            types = self._seed_types(filter_config, task, cohort_data)

            # The filter has the seeds in groups of chunk_size
            condition = SeedFilter(
                filter_config,
                cohort_data,
                types,
                context=seeds
            ).build(queries)
            logger.info(
                "Table '%s': Built the filter in %.1f s.",
                task.full_name, time.time() - start
            )
            return condition
        return super().process_filter(filter_config, task, queries, seeds)

    def _column_types(self, schema: str | None, table: str) -> dict[str, Any]:
        """Types of the columns of a table of the source, by lower case name,
        or none if the source does not have the table."""
        # The source tells the columns of the table, as the database has them
        try:
            columns = inspect(self.engine).get_columns(table, schema=schema)
        except NoSuchTableError:
            return {}

        # The types are found by the name of the column, in lower case
        types = {}
        for col in columns:
            types[col["name"].lower()] = col["type"]
        return types

    def _seed_types(
        self,
        filter_config: FilterConfig,
        task: ExtractMeta,
        cohort_data: list[tuple]
    ) -> dict[str, Any]:
        """Types of the columns of the cohort data, which the dialects need
        to send it as one parameter.

        A column compared to a column of the table has its type, so that the
        database does not convert the column of the table, which would stop
        it from using its indexes. The type of a column compared to an
        expression depends on the expression, so the database tells it:
        the first of the types of the column of the table in the expression, or
        of the column of the cohort.
        """
        # The types of the columns of the table and of the cohort
        table_types = self._column_types(task.schema, task.table)
        cohort = get_table(filter_config.resolved_cohort)
        cohort_types = {}
        for name, kind in self._column_types(
            cohort.schema,
            cohort.name
        ).items():
            name = f'{filter_config.reference.alias}.{name}'.lower()
            cohort_types[name] = kind

        # Joined reference fields retain their own types, including when
        # different tables contain columns with the same name.
        for join in filter_config.join:
            for name, kind in self._column_types(
                join.reference.schema,
                join.reference.table
            ).items():
                name = f'{join.reference.alias}.{name}'.lower()
                cohort_types[name] = kind

        # The types the columns of the cohort data may have
        by_table, by_cohort = SeedFilter.candidate_types(
            filter_config, table_types, cohort_types
        )

        # Without seed rows, there is no sample to use in a type probe.
        if not cohort_data:
            return by_table

        # Probe expressions only when the two candidate SQL types differ.
        for name, kind in by_table.items():
            dialect = self.engine.dialect
            source_sql_type = kind.compile(dialect=dialect)
            reference_sql_type = by_cohort[name].compile(dialect=dialect)
            if source_sql_type != reference_sql_type:
                if self._accepts(
                    filter_config, task, cohort_data[:1], by_table
                ):
                    return by_table
                return by_cohort
        return by_table

    def _accepts(
        self,
        filter_config: FilterConfig,
        task: ExtractMeta,
        cohort_data: list[tuple],
        types: dict[str, Any]
    ) -> bool:
        """Whether the database accepts the filter with these types, which it
        tells from a statement that does not return any row."""
        # The filter with one seed, which returns no row
        source = SQLGenerator.base_select(
            task.schema, task.table, self.engine
        )
        queries = CohortFilters(source)
        seeds = SeedContext(self.engine.dialect, task.chunk_size)
        seed_filter = SeedFilter(filter_config, cohort_data, types, seeds)
        condition = seed_filter.build(queries)
        stmt = queries.build([[condition]]).limit(0)

        # The database refuses the statement if the types do not fit
        try:
            with self.engine.connect() as conn:
                conn.execute(stmt)
        except DatabaseError as err:
            logger.debug(
                "Table '%s': Types %s not accepted (%s).",
                task.full_name, types, err.orig
            )
            return False
        return True

    def execute(self, task: ExtractMeta, export_to: Path | None = None):
        """Execute extraction by streaming and filtering in memory."""
        target_dir = export_to or self.tmp_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        logger.info("Table '%s': Extracting.", task.full_name)

        # The file is known before it is written, to delete it if it is not
        # complete
        if export_to is None:
            self.created_tables.add(task.temp_name)

        with self.session() as session:
            # Export to a CSV file
            query = self.build_queries(task)
            export_path = target_dir / f"{task.table}.csv.gz"
            try:
                self.csv_hashes[task.full_name] = DataExporter.stream_to_csv(
                    session,
                    query,
                    export_path,
                    table_name=task.table,
                    chunk_size=task.chunk_size,
                    abort=self.abort
                )
            except BaseException:
                export_path.unlink(missing_ok=True)
                raise
            if self.abort.is_set():
                export_path.unlink(missing_ok=True)

    def drop_cohort(self, cohort: str):
        """Clear cohort from memory (no DDL in stream mode)."""
        if cohort in self.cohorts:
            logger.info("Cohort '%s': Dropping data.", cohort)
            del self.cohorts[cohort]

    def get_cohort(self, cohort: str) -> Any:
        """Register the cohort but do not load data yet."""
        if cohort in self.cohorts:
            return self.cohorts[cohort]

        if cohort.endswith(".sql"):
            cohort_path = self.directory / 'extract' / cohort
            name = cohort.replace(".sql", "")
            with open(cohort_path, encoding="utf-8") as file:
                query = file.read().strip()
                subquery = f"(\n  {query}\n) {name}"
                self.cohorts[cohort] = subquery
                return subquery

        self.cohorts[cohort] = cohort
        return cohort

    def get_cohort_set(
        self,
        filter_config: FilterConfig,
        task: 'ExtractMeta'
    ) -> list[tuple[str, ...]]:
        """Load cohort data into memory.

        :param filter_config: Filter configuration.
        :type filter_config: FilterConfig

        :param task: Extraction task metadata.
        :type task: ExtractMeta

        :return: List of cohort records.
        :rtype: list[tuple[str, ...]]
        """
        seed_chunks = self._fetch_local_seeds(
            filter_config,
            chunk_size=task.chunk_size
        )

        # If no joins, no need to query the database
        if not filter_config.join:
            return list(dict.fromkeys(chain.from_iterable(seed_chunks)))

        chain_obj = JoinChain.build(filter_config, self.engine, task=task)
        final_tuples = set()
        for chunk in seed_chunks:
            with self.session() as session:
                for row in chain_obj.execute(chunk, session):
                    str_vals = []
                    for val in row:
                        str_vals.append(None if val is None else str(val))
                    final_tuples.add(tuple(str_vals))
        return list(final_tuples)

    def _fetch_local_seeds(
        self,
        filter_config: FilterConfig,
        chunk_size: int = 10000
    ) -> Iterator[list[tuple[str, ...]]]:
        # pylint: disable=too-many-locals
        """Fetch the initial set of IDs from the local cohort table or CSV."""
        fetch_cols = filter_config.cohort_columns
        cohort = filter_config.resolved_cohort

        # Check if the cohort is an extraction task in the graph
        if self.graph and cohort in self.graph.tasks:
            # Determine the file path
            table = self.graph.tasks[cohort].table
            temp_name = f"tmp_{table}_{UUID}"
            if temp_name in self.created_tables:
                csv_path = self.directory / "data" / ".tmp" / f"{table}.csv.gz"
            else:
                csv_path = self.directory / "data" / f"{table}.csv.gz"

            if not csv_path.exists():
                raise FileNotFoundError(f"Missing file for '{cohort}'")

            # Read and hash the compressed file on-the-fly
            with open(csv_path, 'rb') as f:
                reader = HashingReader(f)
                chunks = pd.read_csv(
                    reader,
                    compression='gzip',
                    usecols=fetch_cols,
                    dtype=str,
                    chunksize=chunk_size
                )

                # Load seeds in chunks
                for chunk in chunks:
                    chunk = chunk[fetch_cols]

                    # A row without any value does not restrict, and a
                    # missing value is None, as the database gives it
                    unique = (
                        chunk.drop_duplicates()
                        .dropna(how="all")
                        .astype(object)
                        .where(pd.notna, None)
                    )
                    yield list(unique.itertuples(index=False, name=None))

            # Validate hash to prevent corrupted reads
            if reader.hasher.hexdigest() != self.csv_hashes[cohort]:
                raise ValueError(f"Hash mismatch for file '{csv_path.name}'")

        else:
            # Fallback to local cohort database query
            db_params = DatabaseMeta.load_env(self.directory)
            with Session.create(**db_params) as session:
                schema, table_name = cohort.split(".")
                sa_table = Table(
                    table_name,
                    MetaData(),
                    schema=schema,
                    autoload_with=session.bind
                )
                cols = [sa_table.c[c] for c in fetch_cols]
                stmt = select(*cols).distinct()
                res = session.execute(stmt)
                for partition in res.partitions(chunk_size):
                    yield [tuple(str(v) for v in row) for row in partition]
